package jellyfin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"jellyreaper/internal/domain"
)

var dashedHexIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

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

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("jellyfin delete failed with status %d", resp.StatusCode)
}

func (c *Client) FetchProviderIDs(ctx context.Context, itemID string) (map[string]string, error) {
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
	endpoint := c.baseURL + "/Items/" + url.PathEscape(candidate) + "?Fields=ProviderIds"
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
		ProviderIds map[string]string `json:"ProviderIds"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode jellyfin provider ids response: %w", err)
	}
	return domain.NormalizeProviderIDs(payload.ProviderIds), nil
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
