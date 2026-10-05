package jellyfin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/txguard"
)

type identityDTO struct {
	ID                string `json:"Id"`
	Type              string `json:"Type"`
	SeriesID          string `json:"SeriesId"`
	SeasonID          string `json:"SeasonId"`
	SeasonName        string `json:"SeasonName"`
	ParentIndexNumber *int   `json:"ParentIndexNumber"`
}

// ResolvedIdentity is Jellyfin's answer: the identity plus the season's
// display name, which travels with it but is not part of it.
type ResolvedIdentity struct {
	Identity   domain.MediaIdentity
	SeasonName string
}

func (d identityDTO) resolved() ResolvedIdentity {
	id := domain.MediaIdentity{ItemType: d.Type, SeriesID: domain.NormalizeID(d.SeriesID), SeasonID: domain.NormalizeID(d.SeasonID)}
	if d.ParentIndexNumber != nil {
		id.SeasonNumber, id.Placed = *d.ParentIndexNumber, true
	}
	return ResolvedIdentity{Identity: id, SeasonName: d.SeasonName}
}

// FetchIdentity asks Jellyfin what an item is and where it sits. found=false
// means Jellyfin no longer has the item.
func (c *Client) FetchIdentity(ctx context.Context, itemID string) (ResolvedIdentity, bool, error) {
	page, err := c.identityPage(ctx, url.Values{"Ids": {providerIDCandidate(itemID)}, "Limit": {"1"}})
	if err != nil || len(page) == 0 {
		return ResolvedIdentity{}, false, err
	}
	return page[0].resolved(), true, nil
}

// ListEpisodeIdentities returns every episode's identity keyed by normalized
// item id, paging pageSize at a time.
func (c *Client) ListEpisodeIdentities(ctx context.Context, pageSize int) (map[string]ResolvedIdentity, error) {
	out := map[string]ResolvedIdentity{}
	for start := 0; ; start += pageSize {
		page, err := c.identityPage(ctx, url.Values{
			"IncludeItemTypes": {"Episode"},
			"Recursive":        {"true"},
			"StartIndex":       {strconv.Itoa(start)},
			"Limit":            {strconv.Itoa(pageSize)},
		})
		if err != nil {
			return nil, err
		}
		for _, d := range page {
			out[domain.NormalizeID(d.ID)] = d.resolved()
		}
		if len(page) < pageSize {
			return out, nil
		}
	}
}

func (c *Client) identityPage(ctx context.Context, q url.Values) ([]identityDTO, error) {
	if txguard.InTx(ctx) {
		return nil, fmt.Errorf("jellyfin identity lookup: %w", ErrIOInTransaction)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/Items?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("build jellyfin identity request: %w", err)
	}
	req.Header.Set("X-Emby-Token", c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform jellyfin identity request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("jellyfin identity request failed with status %d", resp.StatusCode)
	}
	var body struct {
		Items []identityDTO `json:"Items"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode jellyfin identity response: %w", err)
	}
	return body.Items, nil
}
