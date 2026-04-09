package sonarr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Service struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

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
	}
}

func (s *Service) Enabled() bool {
	return s != nil && s.baseURL != "" && s.apiKey != ""
}

func (s *Service) RemoveByProviderIDs(ctx context.Context, providerIDs map[string]string) error {
	if !s.Enabled() {
		return nil
	}
	series, found, err := s.findSeries(ctx, providerIDs)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("sonarr series not found for provider ids: %v", providerIDs)
	}
	endpoint := fmt.Sprintf("%s/api/v3/series/%d?deleteFiles=false&addImportListExclusion=false", s.baseURL, series.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build sonarr delete request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform sonarr delete request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("sonarr delete returned status %d", resp.StatusCode)
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
