package jellyfin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	gen "jellyreaper/internal/jellyfin/gen"
)

type BackfillClient interface {
	GetLogEntriesWithResponse(ctx context.Context, params *gen.GetLogEntriesParams, reqEditors ...gen.RequestEditorFn) (*gen.GetLogEntriesResponse, error)
	GetItemsWithResponse(ctx context.Context, params *gen.GetItemsParams, reqEditors ...gen.RequestEditorFn) (*gen.GetItemsResponse, error)
}

type BackfillService struct {
	client  BackfillClient
	baseURL string
}

type PlaybackEvent struct {
	ItemID string
	UserID string
	Type   string
	Name   string
	Date   time.Time
}

type ItemSnapshot struct {
	ItemID             string
	ItemType           string
	SeasonID           string
	SeasonName         string
	SeriesID           string
	SeriesName         string
	Name               string
	ImageURL           string
	LastPlayedAt       time.Time
	PlayCount          int32
	DateCreated        time.Time
	DateLastMediaAdded time.Time
}

func NewBackfillService(baseURL, apiKey string, httpClient *http.Client) (*BackfillService, error) {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 20 * time.Second,
			Transport: &http.Transport{
				Proxy:             http.ProxyFromEnvironment,
				ForceAttemptHTTP2: false,
			},
		}
	}

	opts := []gen.ClientOption{
		gen.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
			req.Header.Set("X-Emby-Token", apiKey)
			return nil
		}),
	}
	if httpClient != nil {
		opts = append(opts, gen.WithHTTPClient(httpClient))
	}
	client, err := gen.NewClientWithResponses(baseURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("create generated jellyfin client: %w", err)
	}
	return &BackfillService{client: client, baseURL: strings.TrimRight(baseURL, "/")}, nil
}

func NewBackfillServiceWithClient(client BackfillClient) *BackfillService {
	return &BackfillService{client: client}
}

func (s *BackfillService) FetchPlaybackEventsSince(ctx context.Context, since time.Time, limit int32) ([]PlaybackEvent, error) {
	pageSize := limit
	if pageSize <= 0 {
		pageSize = 500
	}

	out := make([]PlaybackEvent, 0, pageSize)
	startIndex := int32(0)

	for {
		params := &gen.GetLogEntriesParams{StartIndex: &startIndex, Limit: &pageSize}
		if !since.IsZero() {
			params.MinDate = &since
		}

		resp, err := s.client.GetLogEntriesWithResponse(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("fetch jellyfin activity log: %w", err)
		}
		if resp.StatusCode() != http.StatusOK {
			return nil, fmt.Errorf("activity log returned status %d", resp.StatusCode())
		}

		body := resp.JSON200
		if body == nil {
			body = resp.ApplicationjsonProfileCamelCase200
		}
		if body == nil {
			body = resp.ApplicationjsonProfilePascalCase200
		}
		if body == nil && len(resp.Body) > 0 {
			var parsed gen.ActivityLogEntryQueryResult
			if err := json.Unmarshal(resp.Body, &parsed); err == nil {
				body = &parsed
			} else if looksLikeHTML(resp.Body) {
				return nil, htmlResponseError("activity log", resp.HTTPResponse, resp.Body)
			}
		}
		if body == nil || body.Items == nil || len(*body.Items) == 0 {
			break
		}

		for _, entry := range *body.Items {
			t := safeString(entry.Type)
			if t == "" {
				continue
			}
			out = append(out, PlaybackEvent{
				ItemID: safeString(entry.ItemId),
				UserID: uuidString(entry.UserId),
				Type:   t,
				Name:   safeString(entry.Name),
				Date:   safeTime(entry.Date),
			})
		}

		if int32(len(*body.Items)) < pageSize {
			break
		}
		startIndex += int32(len(*body.Items))
	}

	return out, nil
}

func (s *BackfillService) FetchChangedItemsSince(ctx context.Context, since time.Time, limit int32) ([]ItemSnapshot, error) {
	recursive := true
	enableUserData := true
	pageSize := limit
	if pageSize <= 0 {
		pageSize = 500
	}

	out := make([]ItemSnapshot, 0, pageSize)
	startIndex := int32(0)

	for {
		params := &gen.GetItemsParams{
			Recursive:      &recursive,
			EnableUserData: &enableUserData,
			Limit:          &pageSize,
			StartIndex:     &startIndex,
		}
		if !since.IsZero() {
			params.MinDateLastSaved = &since
		}

		resp, err := s.client.GetItemsWithResponse(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("fetch jellyfin changed items: %w", err)
		}
		if resp.StatusCode() != http.StatusOK {
			return nil, fmt.Errorf("items query returned status %d", resp.StatusCode())
		}

		body := resp.JSON200
		if body == nil {
			body = resp.ApplicationjsonProfileCamelCase200
		}
		if body == nil {
			body = resp.ApplicationjsonProfilePascalCase200
		}
		if body == nil && len(resp.Body) > 0 {
			var parsed gen.BaseItemDtoQueryResult
			if err := json.Unmarshal(resp.Body, &parsed); err == nil {
				body = &parsed
			} else if looksLikeHTML(resp.Body) {
				return nil, htmlResponseError("items endpoint", resp.HTTPResponse, resp.Body)
			}
		}
		if body == nil || body.Items == nil || len(*body.Items) == 0 {
			break
		}

		for _, item := range *body.Items {
			itemID := uuidString(item.Id)
			itemType := stringValueFromKind(item.Type)
			imageURL := buildPrimaryImageURL(s.baseURL, itemID, item.ImageTags)
			out = append(out, ItemSnapshot{
				ItemID:             itemID,
				ItemType:           itemType,
				SeasonID:           uuidString(item.SeasonId),
				SeasonName:         safeString(item.SeasonName),
				SeriesID:           uuidString(item.SeriesId),
				SeriesName:         safeString(item.SeriesName),
				Name:               safeString(item.Name),
				ImageURL:           imageURL,
				LastPlayedAt:       safeNestedLastPlayed(item.UserData),
				PlayCount:          safeNestedPlayCount(item.UserData),
				DateCreated:        safeTime(item.DateCreated),
				DateLastMediaAdded: safeTime(item.DateLastMediaAdded),
			})
		}

		if int32(len(*body.Items)) < pageSize {
			break
		}
		startIndex += int32(len(*body.Items))
	}

	return out, nil
}

func buildPrimaryImageURL(baseURL string, itemID string, imageTags *map[string]string) string {
	if strings.TrimSpace(baseURL) == "" || strings.TrimSpace(itemID) == "" || imageTags == nil {
		return ""
	}
	tag := ""
	if t, ok := (*imageTags)["Primary"]; ok {
		tag = t
	}
	base := strings.TrimRight(baseURL, "/")
	if tag == "" {
		return base + "/Items/" + itemID + "/Images/Primary"
	}
	return base + "/Items/" + itemID + "/Images/Primary?tag=" + tag
}

func stringValueFromKind(kind *gen.BaseItemKind) string {
	if kind == nil {
		return ""
	}
	return string(*kind)
}

func looksLikeHTML(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	return bytes.HasPrefix(trimmed, []byte("<"))
}

func htmlResponseError(endpoint string, httpResp *http.Response, body []byte) error {
	status := 0
	location := ""
	contentType := ""
	if httpResp != nil {
		status = httpResp.StatusCode
		location = httpResp.Header.Get("Location")
		contentType = httpResp.Header.Get("Content-Type")
	}

	snippet := strings.TrimSpace(string(bytes.TrimSpace(body)))
	if len(snippet) > 200 {
		snippet = snippet[:200]
	}

	parts := []string{
		fmt.Sprintf("%s returned HTML instead of JSON", endpoint),
		"check JELLYFIN_URL base path (often it must include /jellyfin)",
	}
	if status != 0 {
		parts = append(parts, fmt.Sprintf("status=%d", status))
	}
	if contentType != "" {
		parts = append(parts, fmt.Sprintf("content_type=%q", contentType))
	}
	if location != "" {
		parts = append(parts, fmt.Sprintf("location=%q", location))
	}
	if snippet != "" {
		parts = append(parts, fmt.Sprintf("body_snippet=%q", snippet))
	}

	return fmt.Errorf("%s", strings.Join(parts, " | "))
}

func safeString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func safeTime(v *time.Time) time.Time {
	if v == nil {
		return time.Time{}
	}
	return v.UTC()
}

func uuidString(v *uuid.UUID) string {
	if v == nil {
		return ""
	}
	return v.String()
}

func safeNestedLastPlayed(data *gen.UserItemDataDto) time.Time {
	if data == nil || data.LastPlayedDate == nil {
		return time.Time{}
	}
	return data.LastPlayedDate.UTC()
}

func safeNestedPlayCount(data *gen.UserItemDataDto) int32 {
	if data == nil || data.PlayCount == nil {
		return 0
	}
	return *data.PlayCount
}
