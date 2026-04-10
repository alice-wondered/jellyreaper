package radarr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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

var ErrNotManaged = errors.New("radarr movie not managed")

type movieResource struct {
	ID     int    `json:"id"`
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

func (s *Service) Enabled() bool {
	return s != nil && s.baseURL != "" && s.apiKey != ""
}

// RemoveByProviderIDs deletes a movie's files from disk via Radarr's
// moviefile bulk-delete endpoint, then removes the movie entry from Radarr.
// This two-phase approach mirrors the Sonarr season-delete strategy and
// avoids the unreliable deleteFiles query-param on the movie DELETE endpoint.
func (s *Service) RemoveByProviderIDs(ctx context.Context, providerIDs map[string]string) error {
	if !s.Enabled() {
		return nil
	}
	movie, found, err := s.findMovie(ctx, providerIDs)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w for provider ids: %v", ErrNotManaged, BuildProviderIDs(providerIDs))
	}
	s.logger.Info("radarr movie delete start", "lex", "RADARR-DELETE", "movie_id", movie.ID, "title", strings.TrimSpace(movie.Title), "tmdb", providerIDs["tmdb"], "imdb", providerIDs["imdb"])

	// Phase 1: explicitly delete the movie's file(s) from disk.
	fileIDs, err := s.listMovieFileIDs(ctx, movie.ID)
	if err != nil {
		return err
	}
	s.logger.Info("radarr movie delete loaded files", "lex", "RADARR-DELETE", "movie_id", movie.ID, "file_count", len(fileIDs))
	if len(fileIDs) > 0 {
		if err := s.deleteMovieFiles(ctx, fileIDs); err != nil {
			return err
		}
	}

	// Phase 2: remove the movie entry from Radarr. deleteFiles=false since
	// we already handled file removal above.
	endpoint := fmt.Sprintf("%s/api/v3/movie/%d?deleteFiles=false&addImportExclusion=false", s.baseURL, movie.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build radarr movie delete request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform radarr movie delete request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.logger.Info("radarr movie delete complete", "lex", "RADARR-DELETE", "movie_id", movie.ID, "file_count", len(fileIDs))
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		s.logger.Info("radarr movie delete already gone", "lex", "RADARR-DELETE", "movie_id", movie.ID)
		return nil
	}
	return fmt.Errorf("radarr movie delete returned status %d", resp.StatusCode)
}

type movieFileResource struct {
	ID int `json:"id"`
}

func (s *Service) listMovieFileIDs(ctx context.Context, movieID int) ([]int, error) {
	endpoint := fmt.Sprintf("%s/api/v3/moviefile?movieId=%d", s.baseURL, movieID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build radarr moviefile list request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform radarr moviefile list request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("radarr moviefile list returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read radarr moviefile list response: %w", err)
	}
	var files []movieFileResource
	if err := json.Unmarshal(body, &files); err != nil {
		return nil, fmt.Errorf("decode radarr moviefile list response: %w", err)
	}
	ids := make([]int, 0, len(files))
	for _, f := range files {
		if f.ID > 0 {
			ids = append(ids, f.ID)
		}
	}
	return ids, nil
}

func (s *Service) deleteMovieFiles(ctx context.Context, fileIDs []int) error {
	body, err := json.Marshal(map[string]any{"movieFileIds": fileIDs})
	if err != nil {
		return fmt.Errorf("encode radarr moviefile delete payload: %w", err)
	}
	endpoint := fmt.Sprintf("%s/api/v3/moviefile/bulk", s.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build radarr moviefile bulk delete request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform radarr moviefile bulk delete request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("radarr moviefile bulk delete returned status %d", resp.StatusCode)
}

func (s *Service) findMovie(ctx context.Context, providerIDs map[string]string) (movieResource, bool, error) {
	all, err := s.listMovies(ctx)
	if err != nil {
		return movieResource{}, false, err
	}
	tmdb := strings.TrimSpace(providerIDs["tmdb"])
	imdb := strings.TrimSpace(providerIDs["imdb"])
	for _, m := range all {
		if tmdb != "" {
			if n, convErr := strconv.Atoi(tmdb); convErr == nil && m.TmdbID == n {
				return m, true, nil
			}
		}
		if imdb != "" && strings.EqualFold(strings.TrimSpace(m.ImdbID), imdb) {
			return m, true, nil
		}
	}
	return movieResource{}, false, nil
}

func (s *Service) listMovies(ctx context.Context) ([]movieResource, error) {
	endpoint := s.baseURL + "/api/v3/movie"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build radarr movies request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform radarr movies request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("radarr movies returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read radarr movies response: %w", err)
	}
	var out []movieResource
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode radarr movies response: %w", err)
	}
	return out, nil
}

func BuildProviderIDs(raw map[string]string) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		key := strings.ToLower(strings.TrimSpace(k))
		val := strings.TrimSpace(v)
		if key == "" || val == "" {
			continue
		}
		out[key] = val
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func EncodeQuery(values map[string]string) string {
	v := url.Values{}
	for k, val := range values {
		v.Set(k, val)
	}
	return v.Encode()
}
