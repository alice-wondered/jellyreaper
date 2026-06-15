package jellyfin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/txguard"
)

// itemExistenceChecker is a narrow interface satisfied by *Client that lets
// the reconciliation service be tested without a real HTTP connection.
type ItemExistenceChecker interface {
	CheckItemsExist(ctx context.Context, itemIDs []string) (map[string]bool, error)
}

var dashedHexIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ErrIOInTransaction is returned when an outbound Jellyfin HTTP call is
// attempted while the caller is inside a bbolt write transaction. Holding the
// single global write lock across a network round-trip stalls every other
// writer until the remote call times out; this guard makes that mistake fail
// loudly and immediately rather than silently degrading the service.
var ErrIOInTransaction = errors.New("network I/O attempted inside bbolt write transaction")

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func NewClient(baseURL, apiKey string, httpClient *http.Client) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				Proxy:             http.ProxyFromEnvironment,
				ForceAttemptHTTP2: false,
			},
		}
	}
	return &Client{baseURL: baseURL, apiKey: apiKey, http: httpClient}
}

func (c *Client) DeleteItem(ctx context.Context, itemID string) error {
	if txguard.InTx(ctx) {
		return fmt.Errorf("jellyfin delete item %q: %w", itemID, ErrIOInTransaction)
	}
	if c.baseURL == "" {
		return fmt.Errorf("jellyfin base url is required")
	}
	if c.apiKey == "" {
		return fmt.Errorf("jellyfin api key is required")
	}
	if itemID == "" {
		return fmt.Errorf("item id is required")
	}

	endpoint := c.baseURL + "/Items/" + url.PathEscape(itemID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build jellyfin delete request: %w", err)
	}
	req.Header.Set("X-Emby-Token", c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("perform jellyfin delete request: %w", err)
	}
	defer resp.Body.Close()

	// 204/200 — successfully deleted.
	// 404 — already gone. Treat as success so the destructive delete handler
	// is idempotent across retries and across "Jellyfin removed it out from
	// under us" scenarios.
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
		return nil
	}

	return fmt.Errorf("jellyfin delete failed with status %d", resp.StatusCode)
}

func (c *Client) FetchProviderIDs(ctx context.Context, itemID string) (map[string]string, error) {
	if txguard.InTx(ctx) {
		return nil, fmt.Errorf("jellyfin fetch provider ids %q: %w", itemID, ErrIOInTransaction)
	}
	if c.baseURL == "" {
		return nil, fmt.Errorf("jellyfin base url is required")
	}
	if c.apiKey == "" {
		return nil, fmt.Errorf("jellyfin api key is required")
	}
	if itemID == "" {
		return nil, fmt.Errorf("item id is required")
	}

	candidate := providerIDCandidate(itemID)
	endpoint := c.baseURL + "/Items?Ids=" + url.QueryEscape(candidate) + "&Fields=ProviderIds&Limit=1"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build jellyfin provider ids request: %w", err)
	}
	req.Header.Set("X-Emby-Token", c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform jellyfin provider ids request: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read jellyfin provider ids response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("jellyfin provider ids request failed with status %d", resp.StatusCode)
	}

	var payload struct {
		Items []struct {
			ProviderIds map[string]string `json:"ProviderIds"`
		} `json:"Items"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode jellyfin provider ids response: %w", err)
	}
	if len(payload.Items) == 0 {
		return nil, nil
	}
	return domain.NormalizeProviderIDs(payload.Items[0].ProviderIds), nil
}

// CheckItemsExist queries Jellyfin for a batch of item IDs and returns the
// subset that are still present in the library. The returned map is keyed by
// the normalised item ID (lower-case, no dashes). Unknown / deleted items are
// simply absent from the map. Callers should treat an absent entry as "gone".
//
// At most maxBatch IDs are sent per HTTP request; the method fans out
// automatically when len(itemIDs) > maxBatch. Pass a nil or empty slice to
// get an empty map with no network I/O.
func (c *Client) CheckItemsExist(ctx context.Context, itemIDs []string) (map[string]bool, error) {
	if txguard.InTx(ctx) {
		return nil, fmt.Errorf("jellyfin check items exist: %w", ErrIOInTransaction)
	}
	if c.baseURL == "" {
		return nil, fmt.Errorf("jellyfin base url is required")
	}
	if c.apiKey == "" {
		return nil, fmt.Errorf("jellyfin api key is required")
	}
	if len(itemIDs) == 0 {
		return map[string]bool{}, nil
	}

	const maxBatch = 100
	present := make(map[string]bool, len(itemIDs))

	// fan-out in chunks of maxBatch
	for start := 0; start < len(itemIDs); start += maxBatch {
		end := start + maxBatch
		if end > len(itemIDs) {
			end = len(itemIDs)
		}
		chunk := itemIDs[start:end]
		candidates := make([]string, 0, len(chunk))
		for _, id := range chunk {
			if c := providerIDCandidate(id); c != "" {
				candidates = append(candidates, c)
			}
		}
		if len(candidates) == 0 {
			continue
		}

		endpoint := c.baseURL + "/Items?Ids=" + url.QueryEscape(strings.Join(candidates, ",")) + "&Fields=Id&Limit=" + fmt.Sprintf("%d", maxBatch)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build jellyfin items exist request: %w", err)
		}
		req.Header.Set("X-Emby-Token", c.apiKey)

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("perform jellyfin items exist request: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read jellyfin items exist response: %w", readErr)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("jellyfin items exist request failed with status %d", resp.StatusCode)
		}

		var payload struct {
			Items []struct {
				ID string `json:"Id"`
			} `json:"Items"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, fmt.Errorf("decode jellyfin items exist response: %w", err)
		}
		for _, item := range payload.Items {
			if norm := domain.NormalizeID(item.ID); norm != "" {
				present[norm] = true
			}
		}
	}

	return present, nil
}

func providerIDCandidate(itemID string) string {
	normalized := domain.NormalizeID(itemID)
	if normalized == "" {
		return ""
	}
	if !dashedHexIDPattern.MatchString(normalized) {
		return normalized
	}
	return strings.ReplaceAll(normalized, "-", "")
}
