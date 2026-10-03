package jellyseerr

import (
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

// MediaKind doubles as Jellyseerr's route segment (/api/v1/movie, /api/v1/tv).
// TMDB movie and TV ids are separate namespaces, so the kind must match the
// deleted media or the lookup clears an unrelated title.
type MediaKind string

const (
	MediaKindMovie MediaKind = "movie"
	MediaKindTV    MediaKind = "tv"
)

var ErrNoTMDB = errors.New("jellyseerr lookup needs a tmdb provider id")

type Service struct {
	baseURL string
	apiKey  string
	http    *http.Client
	logger  *slog.Logger
}

func NewService(baseURL, apiKey string) *Service {
	return &Service{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		apiKey:  strings.TrimSpace(apiKey),
		http:    &http.Client{Timeout: 20 * time.Second},
		logger:  slog.Default(),
	}
}

// ClearMedia removes Jellyseerr's media record (requests cascade) — the API
// form of the UI's "Clear Data". Without it the record stays AVAILABLE until
// the daily availability-sync job, and a re-request in that window is
// swallowed as "already available" and, for movies, leaves an APPROVED
// request that blocks every later request as a duplicate. For TV this clears
// the whole series; Jellyseerr's next full library scan restores the
// seasons still on disk, since there is no per-season reset endpoint.
func (s *Service) ClearMedia(ctx context.Context, kind MediaKind, providerIDs map[string]string) error {
	tmdb, err := strconv.Atoi(strings.TrimSpace(providerIDs["tmdb"]))
	if err != nil || tmdb <= 0 {
		return ErrNoTMDB
	}
	mediaID, err := s.lookupMediaID(ctx, kind, tmdb)
	if err != nil || mediaID == 0 {
		return err
	}
	resp, err := s.do(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/media/%d", mediaID))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("jellyseerr media delete returned status %d", resp.StatusCode)
	}
	s.logger.Info("jellyseerr media cleared", "lex", "JELLYSEERR", "kind", kind, "tmdb", tmdb, "media_id", mediaID)
	return nil
}

// lookupMediaID returns 0 when Jellyseerr has no record — nothing to clear.
func (s *Service) lookupMediaID(ctx context.Context, kind MediaKind, tmdb int) (int, error) {
	resp, err := s.do(ctx, http.MethodGet, fmt.Sprintf("/api/v1/%s/%d", kind, tmdb))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("jellyseerr %s lookup returned status %d", kind, resp.StatusCode)
	}
	var out struct {
		MediaInfo *struct {
			ID int `json:"id"`
		} `json:"mediaInfo"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode jellyseerr %s lookup: %w", kind, err)
	}
	if out.MediaInfo == nil {
		return 0, nil
	}
	return out.MediaInfo.ID, nil
}

func (s *Service) do(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build jellyseerr %s %s: %w", method, path, err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform jellyseerr %s %s: %w", method, path, err)
	}
	return resp, nil
}
