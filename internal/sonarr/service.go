package sonarr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Service struct {
	baseURL string
	apiKey  string
	http    *http.Client
	logger  *slog.Logger
}

var ErrNotManaged = errors.New("sonarr series or season not managed")

type seriesResource struct {
	ID     int    `json:"id"`
	TvdbID int    `json:"tvdbId"`
	TmdbID int    `json:"tmdbId"`
	ImdbID string `json:"imdbId"`
	Title  string `json:"title"`
}

func NewService(baseURL, apiKey string) *Service {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: false}
	return &Service{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		apiKey:  strings.TrimSpace(apiKey),
		http:    &http.Client{Timeout: 20 * time.Second, Transport: transport},
		logger:  slog.Default(),
	}
}

func (s *Service) SetLogger(logger *slog.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

func (s *Service) Enabled() bool {
	return s != nil && s.baseURL != "" && s.apiKey != ""
}

func (s *Service) RemoveSeasonByProviderIDs(ctx context.Context, providerIDs map[string]string, seasonNumber int) error {
	if !s.Enabled() {
		return nil
	}
	if seasonNumber < 0 { // 0 is Specials
		return fmt.Errorf("invalid season number: %d", seasonNumber)
	}
	s.logger.Info("sonarr season delete start", "lex", "SONARR-DELETE", "season_number", seasonNumber, "tvdb", strings.TrimSpace(providerIDs["tvdb"]), "tmdb", strings.TrimSpace(providerIDs["tmdb"]), "imdb", strings.TrimSpace(providerIDs["imdb"]))
	series, found, err := s.findSeries(ctx, providerIDs)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w for provider ids: %v", ErrNotManaged, providerIDs)
	}
	s.logger.Info("sonarr season delete resolved series", "lex", "SONARR-DELETE", "series_id", series.ID, "series_title", strings.TrimSpace(series.Title), "season_number", seasonNumber)
	episodeIDs, err := s.listSeasonEpisodeIDs(ctx, series.ID, seasonNumber)
	if err != nil {
		return err
	}
	if len(episodeIDs) == 0 {
		return fmt.Errorf("%w: season not found or empty for series %d season %d", ErrNotManaged, series.ID, seasonNumber)
	}
	s.logger.Info("sonarr season delete loaded episodes", "lex", "SONARR-DELETE", "series_id", series.ID, "season_number", seasonNumber, "episode_count", len(episodeIDs), "endpoint", "/api/v3/episode")
	episodeFileIDs, err := s.listSeasonEpisodeFileIDs(ctx, series.ID, seasonNumber)
	if err != nil {
		return err
	}
	s.logger.Info("sonarr season delete loaded episode files", "lex", "SONARR-DELETE", "series_id", series.ID, "season_number", seasonNumber, "episode_file_count", len(episodeFileIDs), "endpoint", "/api/v3/episode")
	if len(episodeFileIDs) > 0 {
		s.logger.Info("sonarr season delete deleting episode files", "lex", "SONARR-DELETE", "series_id", series.ID, "season_number", seasonNumber, "episode_file_count", len(episodeFileIDs), "endpoint", "/api/v3/episodefile/bulk")
		if err := s.deleteEpisodeFiles(ctx, episodeFileIDs); err != nil {
			return err
		}
	}
	s.logger.Info("sonarr season delete unmonitoring season", "lex", "SONARR-DELETE", "series_id", series.ID, "season_number", seasonNumber, "endpoint", "/api/v3/series/{id}")
	gone, err := s.unmonitorSeason(ctx, series.ID, seasonNumber)
	if err != nil {
		return err
	}
	if gone {
		s.logger.Info("sonarr season delete already gone", "lex", "SONARR-DELETE", "series_id", series.ID, "season_number", seasonNumber)
		return nil
	}
	s.logger.Info("sonarr season delete complete", "lex", "SONARR-DELETE", "series_id", series.ID, "season_number", seasonNumber, "episode_count", len(episodeIDs), "episode_file_count", len(episodeFileIDs))
	return nil
}

// unmonitorSeason flips the season-level monitored flag rather than the
// episodes'. Sonarr cascades season→episode monitoring only when the season
// flag changes (SeriesService.UpdateSeries), so leaving the season monitored
// with unmonitored episodes makes a later Jellyseerr re-request (which sets
// the season monitored=true) a no-op that never re-monitors or searches.
// Returns gone=true when Sonarr 404s (series removed between list and update).
func (s *Service) unmonitorSeason(ctx context.Context, seriesID, seasonNumber int) (gone bool, err error) {
	endpoint := fmt.Sprintf("%s/api/v3/series/%d", s.baseURL, seriesID)
	series, gone, err := s.getSeriesRaw(ctx, endpoint)
	if err != nil || gone {
		return gone, err
	}
	seasons, _ := series["seasons"].([]any)
	matched := false
	for _, raw := range seasons {
		season, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if n, ok := season["seasonNumber"].(json.Number); ok && n.String() == strconv.Itoa(seasonNumber) {
			season["monitored"] = false
			matched = true
		}
	}
	if !matched {
		return false, fmt.Errorf("%w: season %d missing from sonarr series %d", ErrNotManaged, seasonNumber, seriesID)
	}
	body, err := json.Marshal(series)
	if err != nil {
		return false, fmt.Errorf("encode sonarr series update payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("build sonarr series update request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("perform sonarr series update request: %w", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return false, nil
	case resp.StatusCode == http.StatusNotFound:
		return true, nil
	}
	return false, fmt.Errorf("sonarr series update returned status %d", resp.StatusCode)
}

// getSeriesRaw keeps the full resource as untyped JSON: PUT /series replaces
// the whole series, so every field we don't model must round-trip unchanged.
func (s *Service) getSeriesRaw(ctx context.Context, endpoint string) (series map[string]any, gone bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, fmt.Errorf("build sonarr series get request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("perform sonarr series get request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, true, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("sonarr series get returned status %d", resp.StatusCode)
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 8<<20))
	dec.UseNumber()
	if err := dec.Decode(&series); err != nil {
		return nil, false, fmt.Errorf("decode sonarr series get response: %w", err)
	}
	return series, false, nil
}

func (s *Service) findSeries(ctx context.Context, providerIDs map[string]string) (seriesResource, bool, error) {
	all, err := s.listSeries(ctx)
	if err != nil {
		return seriesResource{}, false, err
	}
	tvdb := strings.TrimSpace(providerIDs["tvdb"])
	tmdb := strings.TrimSpace(providerIDs["tmdb"])
	imdb := strings.TrimSpace(providerIDs["imdb"])
	for _, v := range all {
		if tvdb != "" {
			if n, convErr := strconv.Atoi(tvdb); convErr == nil && v.TvdbID == n {
				return v, true, nil
			}
		}
		if tmdb != "" {
			if n, convErr := strconv.Atoi(tmdb); convErr == nil && v.TmdbID == n {
				return v, true, nil
			}
		}
		if imdb != "" && strings.EqualFold(strings.TrimSpace(v.ImdbID), imdb) {
			return v, true, nil
		}
	}
	return seriesResource{}, false, nil
}

func (s *Service) listSeries(ctx context.Context) ([]seriesResource, error) {
	endpoint := s.baseURL + "/api/v3/series"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build sonarr series request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform sonarr series request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sonarr series returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read sonarr series response: %w", err)
	}
	var out []seriesResource
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode sonarr series response: %w", err)
	}
	return out, nil
}

type episodeResource struct {
	ID            int `json:"id"`
	EpisodeFileID int `json:"episodeFileId"`
}

func (s *Service) listSeasonEpisodeIDs(ctx context.Context, seriesID int, seasonNumber int) ([]int, error) {
	endpoint := fmt.Sprintf("%s/api/v3/episode?seriesId=%d&seasonNumber=%d", s.baseURL, seriesID, seasonNumber)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build sonarr episodes request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform sonarr episodes request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sonarr episodes returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read sonarr episodes response: %w", err)
	}
	var out []episodeResource
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode sonarr episodes response: %w", err)
	}
	ids := make([]int, 0, len(out))
	for _, ep := range out {
		if ep.ID > 0 {
			ids = append(ids, ep.ID)
		}
	}
	return ids, nil
}

func (s *Service) listSeasonEpisodeFileIDs(ctx context.Context, seriesID int, seasonNumber int) ([]int, error) {
	endpoint := fmt.Sprintf("%s/api/v3/episode?seriesId=%d&seasonNumber=%d", s.baseURL, seriesID, seasonNumber)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build sonarr episodes(file ids) request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform sonarr episodes(file ids) request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sonarr episodes(file ids) returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read sonarr episodes(file ids) response: %w", err)
	}
	var out []episodeResource
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode sonarr episodes(file ids) response: %w", err)
	}
	set := map[int]struct{}{}
	for _, ep := range out {
		if ep.EpisodeFileID > 0 {
			set[ep.EpisodeFileID] = struct{}{}
		}
	}
	ids := make([]int, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Service) deleteEpisodeFiles(ctx context.Context, fileIDs []int) error {
	body, err := json.Marshal(map[string]any{"episodeFileIds": fileIDs})
	if err != nil {
		return fmt.Errorf("encode sonarr episodefile delete payload: %w", err)
	}
	endpoint := fmt.Sprintf("%s/api/v3/episodefile/bulk", s.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build sonarr episodefile bulk delete request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform sonarr episodefile bulk delete request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	// 404 — files already gone. Idempotent success.
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("sonarr episodefile bulk delete returned status %d", resp.StatusCode)
}
