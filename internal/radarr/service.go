package radarr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
}

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
	}
}

func (s *Service) Enabled() bool {
	return s != nil && s.baseURL != "" && s.apiKey != ""
}

func (s *Service) RemoveByProviderIDs(ctx context.Context, providerIDs map[string]string) error {
	if !s.Enabled() {
		return nil
	}
	movie, found, err := s.findMovie(ctx, providerIDs)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("radarr movie not found for provider ids: %v", BuildProviderIDs(providerIDs))
	}
	endpoint := fmt.Sprintf("%s/api/v3/movie/%d?deleteFiles=true&addImportExclusion=false", s.baseURL, movie.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build radarr delete request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform radarr delete request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("radarr delete returned status %d", resp.StatusCode)
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
