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
	"sync"
	"time"

	"github.com/google/uuid"

	gen "jellyreaper/internal/jellyfin/gen"
)

type BackfillClient interface {
	GetLogEntriesWithResponse(ctx context.Context, params *gen.GetLogEntriesParams, reqEditors ...gen.RequestEditorFn) (*gen.GetLogEntriesResponse, error)
	GetItemsWithResponse(ctx context.Context, params *gen.GetItemsParams, reqEditors ...gen.RequestEditorFn) (*gen.GetItemsResponse, error)
}

type BackfillService struct {
	client        BackfillClient
	baseURL       string
	apiKey        string
	httpClient    *http.Client
	progressHook  func(FetchProgress)
	warningHook   func(string, error)
	usersMu       sync.Mutex
	cachedUsers   []userSummary
	usersCachedAt time.Time
}

const usersCacheTTL = 30 * time.Minute
const userItemsIDsChunkSize = 80
const enrichmentRetryMaxAttempts = 4
const enrichmentRetryBaseDelay = 200 * time.Millisecond
const enrichmentRetryMaxDelay = 2 * time.Second

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

func (s *BackfillService) SetWarningHook(hook func(string, error)) {
	s.warningHook = hook
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
	out := make([]ItemSnapshot, 0)
	nextStart := startIndex
	hasMore := false
	totalCount := 0
	if body != nil && body.Items != nil {
		totalCount = safeTotalCount(body.TotalRecordCount)
		nextStart = startIndex + int32(len(*body.Items))
		hasMore = int32(len(*body.Items)) >= pageSize
		out = make([]ItemSnapshot, 0, len(*body.Items))
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
	}

	if !since.IsZero() && startIndex == 0 {
		if recent, err := s.fetchRecentlyPlayedAcrossUsersSince(ctx, since, pageSize); err != nil {
			s.emitWarning("recent-user-plays", err)
			if len(recent) > 0 {
				out = mergeRecentSnapshots(out, recent)
			}
			return ItemPage{}, fmt.Errorf("recent user-play merge failed: %w", err)
		} else if len(recent) > 0 {
			out = mergeRecentSnapshots(out, recent)
		}
	}

	if enriched, err := s.enrichItemsWithAllUsersPlayback(ctx, out); err != nil {
		out = enriched
		s.emitWarning("user-playback-enrichment", err)
		return ItemPage{}, fmt.Errorf("user playback enrichment failed: %w", err)
	} else {
		out = enriched
	}

	return ItemPage{
		Items:            out,
		NextStartIndex:   nextStart,
		HasMore:          hasMore,
		TotalRecordCount: totalCount,
	}, nil
}

type userSummary struct {
	ID string `json:"Id"`
}

type usersItemsResponse struct {
	Items []struct {
		ID                 string     `json:"Id"`
		Type               string     `json:"Type"`
		Name               string     `json:"Name"`
		SeasonID           string     `json:"SeasonId"`
		SeasonName         string     `json:"SeasonName"`
		SeriesID           string     `json:"SeriesId"`
		SeriesName         string     `json:"SeriesName"`
		DateCreated        *time.Time `json:"DateCreated"`
		DateLastMediaAdded *time.Time `json:"DateLastMediaAdded"`
		UserData           *struct {
			LastPlayedDate *time.Time `json:"LastPlayedDate"`
			PlayCount      *int32     `json:"PlayCount"`
		} `json:"UserData"`
	} `json:"Items"`
}

func (s *BackfillService) enrichItemsWithAllUsersPlayback(ctx context.Context, items []ItemSnapshot) ([]ItemSnapshot, error) {
	if len(items) == 0 || s.httpClient == nil || strings.TrimSpace(s.baseURL) == "" || strings.TrimSpace(s.apiKey) == "" {
		return items, nil
	}

	users, err := s.fetchUsersCached(ctx)
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
	chunkFailures := 0
	var firstErr error
	for _, user := range users {
		for _, chunk := range chunkItemIDs(ids, userItemsIDsChunkSize) {
			userItems, err := s.fetchUserItemsPlayback(ctx, user.ID, chunk)
			if err != nil {
				chunkFailures++
				if firstErr == nil {
					firstErr = err
				}
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
	}

	for id, idx := range indexByID {
		if ts, ok := lastPlayed[id]; ok && ts.After(items[idx].LastPlayedAt) {
			items[idx].LastPlayedAt = ts
		}
		if pc, ok := playCount[id]; ok && pc > items[idx].PlayCount {
			items[idx].PlayCount = pc
		}
	}
	if chunkFailures > 0 {
		return items, fmt.Errorf("user playback enrichment had %d failed user-item chunks: %w", chunkFailures, firstErr)
	}
	return items, nil
}

func (s *BackfillService) fetchUsersCached(ctx context.Context) ([]userSummary, error) {
	s.usersMu.Lock()
	if len(s.cachedUsers) > 0 && time.Since(s.usersCachedAt) < usersCacheTTL {
		out := append([]userSummary(nil), s.cachedUsers...)
		s.usersMu.Unlock()
		return out, nil
	}
	s.usersMu.Unlock()

	users, err := s.fetchUsers(ctx)
	if err != nil {
		return nil, err
	}

	s.usersMu.Lock()
	s.cachedUsers = append(s.cachedUsers[:0], users...)
	s.usersCachedAt = time.Now().UTC()
	out := append([]userSummary(nil), s.cachedUsers...)
	s.usersMu.Unlock()
	return out, nil
}

func (s *BackfillService) fetchUsers(ctx context.Context) ([]userSummary, error) {
	var users []userSummary
	if err := s.getJSONWithRetry(ctx, s.baseURL+"/Users", "fetch users", 4<<20, &users); err != nil {
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
	var out usersItemsResponse
	if err := s.getJSONWithRetry(ctx, endpoint, "fetch user items", 8<<20, &out); err != nil {
		return usersItemsResponse{}, err
	}
	return out, nil
}

func (s *BackfillService) fetchRecentlyPlayedAcrossUsersSince(ctx context.Context, since time.Time, limit int32) ([]ItemSnapshot, error) {
	if since.IsZero() {
		return nil, nil
	}
	users, err := s.fetchUsersCached(ctx)
	if err != nil || len(users) == 0 {
		return nil, err
	}
	if limit <= 0 {
		limit = 500
	}

	merged := map[string]ItemSnapshot{}
	failures := 0
	var firstErr error
	for _, user := range users {
		start := int32(0)
		for {
			page, hasMore, nextStart, err := s.fetchUserRecentlyPlayedPage(ctx, user.ID, since, start, limit)
			if err != nil {
				failures++
				if firstErr == nil {
					firstErr = err
				}
				break
			}
			if len(page.Items) == 0 {
				break
			}

			for _, raw := range page.Items {
				if raw.UserData == nil || raw.UserData.LastPlayedDate == nil {
					continue
				}
				played := raw.UserData.LastPlayedDate.UTC()
				if played.Before(since) {
					continue
				}
				id := strings.TrimSpace(raw.ID)
				if id == "" {
					continue
				}
				item := merged[id]
				item.ItemID = id
				if strings.TrimSpace(item.ItemType) == "" {
					item.ItemType = strings.TrimSpace(raw.Type)
				}
				if strings.TrimSpace(item.Name) == "" {
					item.Name = strings.TrimSpace(raw.Name)
				}
				if strings.TrimSpace(item.SeasonID) == "" {
					item.SeasonID = strings.TrimSpace(raw.SeasonID)
				}
				if strings.TrimSpace(item.SeasonName) == "" {
					item.SeasonName = strings.TrimSpace(raw.SeasonName)
				}
				if strings.TrimSpace(item.SeriesID) == "" {
					item.SeriesID = strings.TrimSpace(raw.SeriesID)
				}
				if strings.TrimSpace(item.SeriesName) == "" {
					item.SeriesName = strings.TrimSpace(raw.SeriesName)
				}
				if played.After(item.LastPlayedAt) {
					item.LastPlayedAt = played
				}
				if raw.UserData.PlayCount != nil && *raw.UserData.PlayCount > item.PlayCount {
					item.PlayCount = *raw.UserData.PlayCount
				}
				if raw.DateCreated != nil {
					item.DateCreated = raw.DateCreated.UTC()
				}
				if raw.DateLastMediaAdded != nil {
					item.DateLastMediaAdded = raw.DateLastMediaAdded.UTC()
				}
				merged[id] = item
			}

			if !hasMore {
				break
			}
			start = nextStart
		}
	}

	out := make([]ItemSnapshot, 0, len(merged))
	for _, v := range merged {
		out = append(out, v)
	}
	if failures > 0 {
		return out, fmt.Errorf("recent user-play fetch had %d failed user streams: %w", failures, firstErr)
	}
	return out, nil
}

func (s *BackfillService) fetchUserRecentlyPlayedPage(ctx context.Context, userID string, since time.Time, startIndex int32, limit int32) (usersItemsResponse, bool, int32, error) {
	values := url.Values{}
	values.Set("Recursive", "true")
	values.Set("EnableUserData", "true")
	values.Set("Fields", "UserData")
	values.Set("SortBy", "DatePlayed")
	values.Set("SortOrder", "Descending")
	values.Set("IsPlayed", "true")
	values.Set("StartIndex", fmt.Sprintf("%d", startIndex))
	values.Set("Limit", fmt.Sprintf("%d", limit))
	values.Set("MinDateLastSavedForUser", since.UTC().Format(time.RFC3339Nano))

	endpoint := s.baseURL + "/Users/" + url.PathEscape(strings.TrimSpace(userID)) + "/Items?" + values.Encode()
	var out usersItemsResponse
	if err := s.getJSONWithRetry(ctx, endpoint, "fetch user played items", 8<<20, &out); err != nil {
		return usersItemsResponse{}, false, startIndex, err
	}
	next := startIndex + int32(len(out.Items))
	return out, int32(len(out.Items)) >= limit, next, nil
}

func (s *BackfillService) getJSONWithRetry(ctx context.Context, endpoint string, op string, maxRead int64, out any) error {
	var lastErr error
	for attempt := 1; attempt <= enrichmentRetryMaxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Emby-Token", s.apiKey)
		resp, err := s.httpClient.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxRead))
			_ = resp.Body.Close()
			if readErr != nil {
				err = readErr
			} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				err = fmt.Errorf("%s returned status %d", op, resp.StatusCode)
				if !isRetryableStatus(resp.StatusCode) {
					return err
				}
			} else if err := json.Unmarshal(body, out); err != nil {
				return err
			} else {
				return nil
			}
		}

		lastErr = err
		if attempt == enrichmentRetryMaxAttempts {
			break
		}
		delay := retryBackoffDelayForEnrichment(attempt-1, enrichmentRetryBaseDelay, enrichmentRetryMaxDelay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", op, enrichmentRetryMaxAttempts, lastErr)
}

func isRetryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func retryBackoffDelayForEnrichment(attempt int, base time.Duration, max time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	if attempt <= 0 {
		return base
	}

	delay := base
	for i := 0; i < attempt; i++ {
		if delay >= max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func mergeRecentSnapshots(base []ItemSnapshot, recent []ItemSnapshot) []ItemSnapshot {
	index := make(map[string]int, len(base))
	for i := range base {
		id := strings.TrimSpace(base[i].ItemID)
		if id != "" {
			index[id] = i
		}
	}
	for _, rec := range recent {
		id := strings.TrimSpace(rec.ItemID)
		if id == "" {
			continue
		}
		if idx, ok := index[id]; ok {
			if rec.LastPlayedAt.After(base[idx].LastPlayedAt) {
				base[idx].LastPlayedAt = rec.LastPlayedAt
			}
			if rec.PlayCount > base[idx].PlayCount {
				base[idx].PlayCount = rec.PlayCount
			}
			continue
		}
		index[id] = len(base)
		base = append(base, rec)
	}
	return base
}

func chunkItemIDs(ids []string, chunkSize int) [][]string {
	if chunkSize <= 0 {
		chunkSize = len(ids)
	}
	out := make([][]string, 0, (len(ids)+chunkSize-1)/chunkSize)
	for start := 0; start < len(ids); start += chunkSize {
		end := start + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		piece := append([]string(nil), ids[start:end]...)
		out = append(out, piece)
	}
	return out
}

func (s *BackfillService) emitProgress(progress FetchProgress) {
	if s.progressHook != nil {
		s.progressHook(progress)
	}
}

func (s *BackfillService) emitWarning(stage string, err error) {
	if err == nil || s.warningHook == nil {
		return
	}
	s.warningHook(stage, err)
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
