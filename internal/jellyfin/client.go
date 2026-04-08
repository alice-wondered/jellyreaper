package jellyfin

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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
