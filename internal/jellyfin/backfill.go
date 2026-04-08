package jellyfin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	client       BackfillClient
	baseURL      string
	apiKey       string
	httpClient   *http.Client
	progressHook func(FetchProgress)
}

type FetchProgress struct {
	Stream           string
	Page             int
	Fetched          int
	PageItems        int
	TotalRecordCount int
	Since            time.Time
}

type PlaybackPage struct {
	Events           []PlaybackEvent
	NextStartIndex   int32
	HasMore          bool
	TotalRecordCount int
}

type ItemPage struct {
	Items            []ItemSnapshot
	NextStartIndex   int32
	HasMore          bool
	TotalRecordCount int
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
	return &BackfillService{client: client, baseURL: strings.TrimRight(baseURL, "/"), apiKey: strings.TrimSpace(apiKey), httpClient: httpClient}, nil
}

func NewBackfillServiceWithClient(client BackfillClient) *BackfillService {
	return &BackfillService{client: client}
}

func (s *BackfillService) SetProgressHook(hook func(FetchProgress)) {
	s.progressHook = hook
}

func (s *BackfillService) FetchPlaybackEventsSince(ctx context.Context, since time.Time, limit int32) ([]PlaybackEvent, error) {
	pageSize := limit
	if pageSize <= 0 {
		pageSize = 500
	}

	out := make([]PlaybackEvent, 0, pageSize)
	startIndex := int32(0)
	page := 0

	for {
		pageData, err := s.FetchPlaybackEventsPage(ctx, since, startIndex, pageSize)
		if err != nil {
			return nil, err
		}
		if len(pageData.Events) == 0 {
			break
		}
		page++

		out = append(out, pageData.Events...)
		s.emitProgress(FetchProgress{
			Stream:           "playback",
			Page:             page,
			Fetched:          len(out),
			PageItems:        len(pageData.Events),
			TotalRecordCount: pageData.TotalRecordCount,
			Since:            since,
		})

		if !pageData.HasMore {
			break
		}
		startIndex = pageData.NextStartIndex
	}

	return out, nil
}

func (s *BackfillService) FetchPlaybackEventsPage(ctx context.Context, since time.Time, startIndex int32, limit int32) (PlaybackPage, error) {
	pageSize := limit
	if pageSize <= 0 {
		pageSize = 500
	}

	params := &gen.GetLogEntriesParams{StartIndex: &startIndex, Limit: &pageSize}
	if !since.IsZero() {
		params.MinDate = &since
	}

	resp, err := s.client.GetLogEntriesWithResponse(ctx, params)
	if err != nil {
		return PlaybackPage{}, fmt.Errorf("fetch jellyfin activity log: %w", err)
	}
	if resp.StatusCode() != http.StatusOK {
		return PlaybackPage{}, fmt.Errorf("activity log returned status %d", resp.StatusCode())
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
			return PlaybackPage{}, htmlResponseError("activity log", resp.HTTPResponse, resp.Body)
		}
	}
	if body == nil || body.Items == nil || len(*body.Items) == 0 {
		return PlaybackPage{Events: []PlaybackEvent{}, NextStartIndex: startIndex, HasMore: false, TotalRecordCount: 0}, nil
	}

	events := make([]PlaybackEvent, 0, len(*body.Items))
	for _, entry := range *body.Items {
		t := safeString(entry.Type)
		if t == "" {
			continue
		}
		events = append(events, PlaybackEvent{
			ItemID: safeString(entry.ItemId),
			UserID: uuidString(entry.UserId),
			Type:   t,
			Name:   safeString(entry.Name),
			Date:   safeTime(entry.Date),
		})
	}

	return PlaybackPage{
		Events:           events,
		NextStartIndex:   startIndex + int32(len(*body.Items)),
		HasMore:          int32(len(*body.Items)) >= pageSize,
		TotalRecordCount: safeTotalCount(body.TotalRecordCount),
	}, nil
}

func (s *BackfillService) FetchChangedItemsSince(ctx context.Context, since time.Time, limit int32) ([]ItemSnapshot, error) {
	pageSize := limit
	if pageSize <= 0 {
		pageSize = 500
	}

	out := make([]ItemSnapshot, 0, pageSize)
	startIndex := int32(0)
	page := 0

	for {
		pageData, err := s.FetchChangedItemsPage(ctx, since, startIndex, pageSize)
		if err != nil {
			return nil, err
		}
		if len(pageData.Items) == 0 {
			break
		}
		page++

		out = append(out, pageData.Items...)
		s.emitProgress(FetchProgress{
			Stream:           "items",
			Page:             page,
			Fetched:          len(out),
			PageItems:        len(pageData.Items),
			TotalRecordCount: pageData.TotalRecordCount,
			Since:            since,
		})

		if !pageData.HasMore {
			break
		}
		startIndex = pageData.NextStartIndex
	}

	return out, nil
}

func (s *BackfillService) FetchChangedItemsPage(ctx context.Context, since time.Time, startIndex int32, limit int32) (ItemPage, error) {
	recursive := true
	enableUserData := true
	sortBy := []gen.ItemSortBy{gen.ItemSortByDateCreated}
	sortOrder := []gen.SortOrder{gen.SortOrder("Ascending")}
	pageSize := limit
	if pageSize <= 0 {
		pageSize = 500
	}

	params := &gen.GetItemsParams{
		Recursive:      &recursive,
		EnableUserData: &enableUserData,
		SortBy:         &sortBy,
		SortOrder:      &sortOrder,
		Limit:          &pageSize,
		StartIndex:     &startIndex,
	}
	if !since.IsZero() {
		params.MinDateLastSaved = &since
	}

	resp, err := s.client.GetItemsWithResponse(ctx, params)
	if err != nil {
		return ItemPage{}, fmt.Errorf("fetch jellyfin changed items: %w", err)
	}
	if resp.StatusCode() != http.StatusOK {
		return ItemPage{}, fmt.Errorf("items query returned status %d", resp.StatusCode())
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
			return ItemPage{}, htmlResponseError("items endpoint", resp.HTTPResponse, resp.Body)
		}
	}
	if body == nil || body.Items == nil || len(*body.Items) == 0 {
		return ItemPage{Items: []ItemSnapshot{}, NextStartIndex: startIndex, HasMore: false, TotalRecordCount: 0}, nil
	}

	out := make([]ItemSnapshot, 0, len(*body.Items))
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

	if enriched, err := s.enrichItemsWithAllUsersPlayback(ctx, out); err == nil {
		out = enriched
	}

	return ItemPage{
		Items:            out,
		NextStartIndex:   startIndex + int32(len(*body.Items)),
		HasMore:          int32(len(*body.Items)) >= pageSize,
		TotalRecordCount: safeTotalCount(body.TotalRecordCount),
	}, nil
}

type userSummary struct {
	ID string `json:"Id"`
}

type usersItemsResponse struct {
	Items []struct {
		ID       string `json:"Id"`
		UserData *struct {
			LastPlayedDate *time.Time `json:"LastPlayedDate"`
			PlayCount      *int32     `json:"PlayCount"`
		} `json:"UserData"`
	} `json:"Items"`
}

func (s *BackfillService) enrichItemsWithAllUsersPlayback(ctx context.Context, items []ItemSnapshot) ([]ItemSnapshot, error) {
	if len(items) == 0 || s.httpClient == nil || strings.TrimSpace(s.baseURL) == "" || strings.TrimSpace(s.apiKey) == "" {
		return items, nil
	}

	users, err := s.fetchUsers(ctx)
	if err != nil || len(users) == 0 {
		return items, err
	}

	ids := make([]string, 0, len(items))
	indexByID := make(map[string]int, len(items))
	for i := range items {
		id := strings.TrimSpace(items[i].ItemID)
		if id == "" {
			continue
		}
		ids = append(ids, id)
		indexByID[id] = i
	}
	if len(ids) == 0 {
		return items, nil
	}

	lastPlayed := map[string]time.Time{}
	playCount := map[string]int32{}
	for _, user := range users {
		userItems, err := s.fetchUserItemsPlayback(ctx, user.ID, ids)
		if err != nil {
			continue
		}
		for _, it := range userItems.Items {
			id := strings.TrimSpace(it.ID)
			if id == "" || it.UserData == nil {
				continue
			}
			if it.UserData.LastPlayedDate != nil {
				ts := it.UserData.LastPlayedDate.UTC()
				if ts.After(lastPlayed[id]) {
					lastPlayed[id] = ts
				}
			}
			if it.UserData.PlayCount != nil && *it.UserData.PlayCount > playCount[id] {
				playCount[id] = *it.UserData.PlayCount
			}
		}
	}

	for id, idx := range indexByID {
		if ts, ok := lastPlayed[id]; ok && ts.After(items[idx].LastPlayedAt) {
			items[idx].LastPlayedAt = ts
		}
		if pc, ok := playCount[id]; ok && pc > items[idx].PlayCount {
			items[idx].PlayCount = pc
		}
	}
	return items, nil
}

func (s *BackfillService) fetchUsers(ctx context.Context) ([]userSummary, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/Users", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Emby-Token", s.apiKey)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetch users returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var users []userSummary
	if err := json.Unmarshal(body, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func (s *BackfillService) fetchUserItemsPlayback(ctx context.Context, userID string, itemIDs []string) (usersItemsResponse, error) {
	values := url.Values{}
	values.Set("Ids", strings.Join(itemIDs, ","))
	values.Set("Recursive", "true")
	values.Set("EnableUserData", "true")
	values.Set("Fields", "UserData")
	endpoint := s.baseURL + "/Users/" + url.PathEscape(strings.TrimSpace(userID)) + "/Items?" + values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return usersItemsResponse{}, err
	}
	req.Header.Set("X-Emby-Token", s.apiKey)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return usersItemsResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return usersItemsResponse{}, fmt.Errorf("fetch user items returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return usersItemsResponse{}, err
	}
	var out usersItemsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return usersItemsResponse{}, err
	}
	return out, nil
}

func (s *BackfillService) emitProgress(progress FetchProgress) {
	if s.progressHook != nil {
		s.progressHook(progress)
	}
}

func safeTotalCount(value *int32) int {
	if value == nil {
		return 0
	}
	if *value < 0 {
		return 0
	}
	return int(*value)
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
