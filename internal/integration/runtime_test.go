package integration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/app"
	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	api "jellyreaper/internal/http"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/jobs/handlers"
	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
	"jellyreaper/internal/scheduler"
	"jellyreaper/internal/worker"
)

type evalCounterHandler struct{ count *atomic.Int64 }

func (h evalCounterHandler) Kind() domain.JobKind { return domain.JobKindEvaluatePolicy }
func (h evalCounterHandler) Handle(context.Context, domain.JobRecord) error {
	h.count.Add(1)
	return nil
}

type removalRecorder struct {
	calls atomic.Int64
	last  map[string]string
}

func (r *removalRecorder) RemoveByProviderIDs(_ context.Context, providerIDs map[string]string) error {
	r.calls.Add(1)
	cpy := make(map[string]string, len(providerIDs))
	for k, v := range providerIDs {
		cpy[k] = v
	}
	r.last = cpy
	return nil
}

func openStore(t *testing.T) *bboltrepo.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "integration.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })
	return store
}

func TestIntegrationWebhookToSchedulerDispatch(t *testing.T) {
	store := openStore(t)
	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}

	wakeCh := make(chan time.Time, 32)
	appSvc := app.NewService(store, nil, func(at time.Time) {
		select {
		case wakeCh <- at:
		default:
		}
	})

	var counter atomic.Int64
	reg, err := jobs.NewRegistry(evalCounterHandler{count: &counter})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	dispatcher := worker.NewDispatcher(store, reg, nil)
	loop := scheduler.NewLoop(store, dispatcher.Dispatch, nil, scheduler.Config{
		LeaseOwner: "it",
		LeaseTTL:   2 * time.Second,
		IdlePoll:   10 * time.Millisecond,
		LeaseLimit: 8,
		Signal:     wakeCh,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = loop.Run(ctx) }()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:       "item-timeout",
			Name:         "Timeout Movie",
			Title:        "Timeout Movie",
			ItemType:     "Movie",
			LastPlayedAt: time.Now().UTC().Add(-120 * 24 * time.Hour),
			UpdatedAt:    time.Now().UTC(),
		})
	}); err != nil {
		t.Fatalf("seed stale media: %v", err)
	}

	mux, err := api.NewMux(api.Config{
		Addr:                     ":0",
		Jellyfin:                 appSvc.HandleJellyfinWebhook,
		Discord:                  discordSvc,
		HandleDiscordInteraction: appSvc.HandleDiscordComponentInteraction,
	})
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"ItemId":"item-e2e","ItemType":"Movie","Name":"E2E Movie","EventId":"evt-e2e-1","NotificationType":"ItemAdded"}`)
	resp, err := http.Post(server.URL+"/webhooks/jellyfin", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for counter.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if counter.Load() < 1 {
		t.Fatal("expected evaluate_policy handler to be dispatched")
	}
}

func TestIntegrationDiscordArchiveNoDeleteJob(t *testing.T) {
	store := openStore(t)
	pub, priv, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}

	appSvc := app.NewService(store, nil, nil)
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:         "flow:target:item:item-arc",
			ItemID:         "target:item:item-arc",
			SubjectType:    "item",
			DisplayName:    "Archive Target",
			State:          domain.FlowStatePendingReview,
			Version:        0,
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			CreatedAt:      time.Now().UTC(),
			UpdatedAt:      time.Now().UTC(),
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}
	mux, err := api.NewMux(api.Config{
		Addr:                     ":0",
		Jellyfin:                 appSvc.HandleJellyfinWebhook,
		Discord:                  discordSvc,
		HandleDiscordInteraction: appSvc.HandleDiscordComponentInteraction,
	})
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	payload := []byte(`{"id":"175928847299117063","token":"tok","type":3,"data":{"custom_id":"jr:v1:archive:target:item:item-arc:0"}}`)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/discord/interactions", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	signDiscord(req, priv, payload)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post discord interaction: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	var flow domain.Flow
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		var found bool
		var err error
		flow, found, err = tx.GetFlow(context.Background(), "target:item:item-arc")
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("expected archived flow")
		}
		return nil
	}); err != nil {
		t.Fatalf("read flow: %v", err)
	}
	if flow.State != domain.FlowStateArchived {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), time.Now().Add(24*time.Hour), 100, "it", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	for _, job := range jobs {
		if job.ItemID == "item-arc" && job.Kind == domain.JobKindExecuteDelete {
			t.Fatalf("unexpected delete job for archived item: %#v", job)
		}
	}
}

func TestIntegrationWebhookDoesNotDeleteImmediatelyWithShortTimeoutConfig(t *testing.T) {
	store := openStore(t)
	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}

	var sentPrompts atomic.Int64
	discordSvc.SetSendPromptHookForTest(func(context.Context, string, string, int64, string, string, string) (string, error) {
		sentPrompts.Add(1)
		return "msg-1", nil
	})

	deleteServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/Items/item-timeout" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer deleteServer.Close()

	wakeCh := make(chan time.Time, 32)
	appSvc := app.NewService(store, nil, func(at time.Time) {
		select {
		case wakeCh <- at:
		default:
		}
	})

	reg, err := jobs.NewRegistry(
		handlers.NewEvaluatePolicyHandler(store, nil),
		handlers.NewSendHITLPromptHandler(store, nil, discordSvc, "ch-1", 25*time.Millisecond),
		handlers.NewHITLTimeoutHandler(store, discordSvc, nil),
		handlers.NewExecuteDeleteHandler(store, jellyfin.NewClient(deleteServer.URL, "api", deleteServer.Client())),
	)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}

	dispatcher := worker.NewDispatcher(store, reg, nil)
	loop := scheduler.NewLoop(store, dispatcher.Dispatch, nil, scheduler.Config{
		LeaseOwner: "it",
		LeaseTTL:   100 * time.Millisecond,
		IdlePoll:   5 * time.Millisecond,
		LeaseLimit: 16,
		Signal:     wakeCh,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = loop.Run(ctx) }()

	mux, err := api.NewMux(api.Config{
		Addr:                     ":0",
		Jellyfin:                 appSvc.HandleJellyfinWebhook,
		Discord:                  discordSvc,
		HandleDiscordInteraction: appSvc.HandleDiscordComponentInteraction,
	})
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"ItemId":"item-timeout","ItemType":"Movie","Name":"Timeout Movie","EventId":"evt-timeout-1","NotificationType":"ItemAdded"}`)
	resp, err := http.Post(server.URL+"/webhooks/jellyfin", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	_ = resp.Body.Close()

	time.Sleep(150 * time.Millisecond)
	flow, found, err := getFlow(store, "target:movie:item-timeout")
	if err != nil || !found {
		t.Fatalf("expected timeout flow: found=%v err=%v", found, err)
	}
	if flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleteInProgress {
		t.Fatalf("unexpected immediate deletion state: %s", flow.State)
	}
}

func TestIntegrationBackfillIndexesStateFromGeneratedTypes(t *testing.T) {
	store := openStore(t)
	now := time.Now().UTC().Add(-time.Hour)

	backfillServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/System/ActivityLog/Entries":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Type": "PlaybackStart", "ItemId": "item-bf", "Name": "BF Movie", "Date": now.Format(time.RFC3339)},
				},
			})
		case "/Items":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "Name": "BF Movie", "DateCreated": now.Format(time.RFC3339), "DateLastMediaAdded": now.Format(time.RFC3339)},
				},
			})
		case "/Users":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"Id": "u1"}})
		case "/Users/u1/Items":
			_ = json.NewEncoder(w).Encode(map[string]any{"Items": []map[string]any{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backfillServer.Close()

	b, err := jellyfin.NewBackfillService(backfillServer.URL, "api", backfillServer.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	plays, err := b.FetchPlaybackEventsSince(context.Background(), now.Add(-time.Minute), 50)
	if err != nil {
		t.Fatalf("fetch plays: %v", err)
	}
	items, err := b.FetchChangedItemsSince(context.Background(), now.Add(-time.Minute), 50)
	if err != nil {
		t.Fatalf("fetch changed items: %v", err)
	}

	if len(plays) != 1 || plays[0].ItemID != "item-bf" {
		t.Fatalf("unexpected plays: %#v", plays)
	}
	if len(items) != 1 || items[0].Name != "BF Movie" {
		t.Fatalf("unexpected changed items: %#v", items)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: items[0].ItemID, Title: items[0].Name, LastPlayedAt: plays[0].Date, UpdatedAt: time.Now().UTC()}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{FlowID: "flow:" + items[0].ItemID, ItemID: items[0].ItemID, State: domain.FlowStateActive, Version: 0, CreatedAt: time.Now().UTC()}, 0)
	}); err != nil {
		t.Fatalf("persist backfill snapshots: %v", err)
	}

	if _, found, err := getFlow(store, items[0].ItemID); err != nil || !found {
		t.Fatalf("expected flow from backfill indexing, found=%v err=%v", found, err)
	}
}

func TestIntegrationBackfillUsesUserPlaybackToDeferReviewScheduling(t *testing.T) {
	store := openStore(t)
	now := time.Now().UTC()
	recentPlay := now.Add(-4 * 24 * time.Hour)
	oldEpisodePlay := now.Add(-120 * 24 * time.Hour)

	movieID := "11111111-2222-3333-4444-555555555555"
	episodeID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	var usersCalls atomic.Int64
	var userItemsCalls atomic.Int64
	backfillServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Items":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Id": movieID, "Type": "Movie", "Name": "Sample Movie", "DateCreated": now.Add(-300 * 24 * time.Hour).Format(time.RFC3339), "DateLastMediaAdded": now.Add(-300 * 24 * time.Hour).Format(time.RFC3339)},
					{"Id": episodeID, "Type": "Episode", "Name": "Sample Episode", "SeriesId": "99999999-8888-7777-6666-555555555555", "SeriesName": "Sample Series", "SeasonId": "12121212-3434-5656-7878-909090909090", "SeasonName": "Season 2", "DateCreated": now.Add(-200 * 24 * time.Hour).Format(time.RFC3339), "DateLastMediaAdded": now.Add(-200 * 24 * time.Hour).Format(time.RFC3339)},
				},
			})
		case "/Users":
			usersCalls.Add(1)
			_ = json.NewEncoder(w).Encode([]map[string]any{{"Id": "u1"}, {"Id": "u2"}})
		case "/Users/u1/Items":
			userItemsCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Id": movieID, "Type": "Movie", "Name": "Sample Movie", "UserData": map[string]any{"PlayCount": 0}},
					{"Id": episodeID, "Type": "Episode", "Name": "Sample Episode", "UserData": map[string]any{"PlayCount": 0}},
				},
			})
		case "/Users/u2/Items":
			userItemsCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Id": movieID, "Type": "Movie", "Name": "Sample Movie", "UserData": map[string]any{"LastPlayedDate": recentPlay.Format(time.RFC3339Nano), "PlayCount": 4}},
					{"Id": episodeID, "Type": "Episode", "Name": "Sample Episode", "UserData": map[string]any{"LastPlayedDate": oldEpisodePlay.Format(time.RFC3339Nano), "PlayCount": 1}},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backfillServer.Close()

	b, err := jellyfin.NewBackfillService(backfillServer.URL, "api", backfillServer.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	items, err := b.FetchChangedItemsSince(context.Background(), now.Add(-365*24*time.Hour), 100)
	if err != nil {
		t.Fatalf("fetch changed items: %v", err)
	}
	if usersCalls.Load() == 0 || userItemsCalls.Load() == 0 {
		t.Fatalf("expected user playback enrichment calls, users=%d user_items=%d", usersCalls.Load(), userItemsCalls.Load())
	}

	appSvc := app.NewService(store, nil, nil)
	appSvc.SetPolicyDefaults(60, 15*24*time.Hour)
	if err := appSvc.IngestBackfillItems(context.Background(), items); err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	err = store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		media, found, err := tx.GetMedia(context.Background(), movieID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("expected movie media record")
		}
		if media.LastPlayedAt.IsZero() {
			t.Fatalf("expected movie last played to be populated from user data")
		}
		if media.LastPlayedAt.Before(recentPlay.Add(-time.Second)) || media.LastPlayedAt.After(recentPlay.Add(time.Second)) {
			t.Fatalf("unexpected movie last played: got=%s want~=%s", media.LastPlayedAt, recentPlay)
		}

		jobID := "job:eval:scheduled:target:movie:" + movieID
		job, found, err := tx.GetJob(context.Background(), jobID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("expected scheduled evaluate job for movie flow")
		}
		if !job.RunAt.After(now) {
			t.Fatalf("expected evaluation to be deferred to future, got run_at=%s now=%s", job.RunAt, now)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify stored state: %v", err)
	}

	leased, err := store.LeaseDueJobs(context.Background(), now.Add(5*time.Minute), 200, "it", time.Minute)
	if err != nil {
		t.Fatalf("lease due jobs: %v", err)
	}
	for _, job := range leased {
		if job.Kind == domain.JobKindSendHITLPrompt && job.ItemID == "target:movie:"+movieID {
			t.Fatalf("did not expect immediate HITL prompt job for recently played movie")
		}
	}
}

func TestIntegrationCanonicalizesIDsAcrossBackfillAndWebhookSources(t *testing.T) {
	store := openStore(t)
	now := time.Now().UTC()
	backfillPlay := now.Add(-4 * 24 * time.Hour)
	webhookPlay := now.Add(-24 * time.Hour)

	dashedID := "1bb7dcaf-2c6e-04a7-5d91-c4f0ee6b3cfd"
	nonDashedID := "1bb7dcaf2c6e04a75d91c4f0ee6b3cfd"

	backfillServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Items":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Id": dashedID, "Type": "Movie", "Name": "Sample Movie", "DateCreated": now.Add(-300 * 24 * time.Hour).Format(time.RFC3339), "DateLastMediaAdded": now.Add(-300 * 24 * time.Hour).Format(time.RFC3339)},
				},
			})
		case "/Users":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"Id": "u1"}})
		case "/Users/u1/Items":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Id": dashedID, "Type": "Movie", "Name": "Sample Movie", "UserData": map[string]any{"LastPlayedDate": backfillPlay.Format(time.RFC3339Nano), "PlayCount": 2}},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backfillServer.Close()

	b, err := jellyfin.NewBackfillService(backfillServer.URL, "api", backfillServer.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	items, err := b.FetchChangedItemsSince(context.Background(), now.Add(-365*24*time.Hour), 100)
	if err != nil {
		t.Fatalf("fetch changed items: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	appSvc := app.NewService(store, nil, nil)
	appSvc.SetPolicyDefaults(60, 15*24*time.Hour)
	if err := appSvc.IngestBackfillItems(context.Background(), items); err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	mux, err := api.NewMux(api.Config{
		Addr:                     ":0",
		Jellyfin:                 appSvc.HandleJellyfinWebhook,
		Discord:                  discordSvc,
		HandleDiscordInteraction: appSvc.HandleDiscordComponentInteraction,
	})
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	payload := []byte(`{"ItemId":"` + nonDashedID + `","ItemType":"Movie","Name":"Sample Movie","EventId":"evt-id-format","NotificationType":"PlaybackStart","LastPlayedAt":"` + webhookPlay.Format(time.RFC3339Nano) + `"}`)
	resp, err := http.Post(server.URL+"/webhooks/jellyfin", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	err = store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		media, found, err := tx.GetMedia(context.Background(), dashedID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("expected canonical dashed media record")
		}
		if media.LastPlayedAt.Before(webhookPlay.Add(-time.Second)) || media.LastPlayedAt.After(webhookPlay.Add(time.Second)) {
			t.Fatalf("expected webhook playback timestamp to win, got=%s want~=%s", media.LastPlayedAt, webhookPlay)
		}

		flowID := "target:movie:" + dashedID
		flow, found, err := tx.GetFlow(context.Background(), flowID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("expected canonical movie flow %s", flowID)
		}
		if flow.ItemID != flowID {
			t.Fatalf("unexpected flow item id: %s", flow.ItemID)
		}

		if _, found, err := tx.GetFlow(context.Background(), "target:movie:"+nonDashedID); err != nil {
			return err
		} else if found {
			t.Fatalf("did not expect non-canonical movie flow key")
		}

		canonicalJobID := "job:eval:scheduled:target:movie:" + dashedID
		job, found, err := tx.GetJob(context.Background(), canonicalJobID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("expected canonical scheduled evaluate job")
		}
		if job.ItemID != "target:movie:"+dashedID {
			t.Fatalf("expected canonical job item id, got %s", job.ItemID)
		}
		if _, found, err := tx.GetJob(context.Background(), "job:eval:scheduled:target:movie:"+nonDashedID); err != nil {
			return err
		} else if found {
			t.Fatalf("did not expect non-canonical scheduled evaluate job")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify canonicalized persisted state: %v", err)
	}
}

func TestIntegrationWebhookDeleteDoesNotTriggerARRRemovalPath(t *testing.T) {
	store := openStore(t)
	now := time.Now().UTC()
	movieID := "1bb7dcaf-2c6e-04a7-5d91-c4f0ee6b3cfd"

	backfillServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Items":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Items": []map[string]any{
					{"Id": movieID, "Type": "Movie", "Name": "Sample Movie", "ProviderIds": map[string]any{"Tmdb": "603", "Imdb": "tt0133093"}, "DateCreated": now.Add(-300 * 24 * time.Hour).Format(time.RFC3339), "DateLastMediaAdded": now.Add(-300 * 24 * time.Hour).Format(time.RFC3339)},
				},
			})
		case "/Users":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"Id": "u1"}})
		case "/Users/u1/Items":
			_ = json.NewEncoder(w).Encode(map[string]any{"Items": []map[string]any{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backfillServer.Close()

	b, err := jellyfin.NewBackfillService(backfillServer.URL, "api", backfillServer.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}
	items, err := b.FetchChangedItemsSince(context.Background(), now.Add(-365*24*time.Hour), 100)
	if err != nil {
		t.Fatalf("fetch changed items: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	appSvc := app.NewService(store, nil, nil)
	appSvc.SetPolicyDefaults(60, 15*24*time.Hour)
	if err := appSvc.IngestBackfillItems(context.Background(), items); err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	mux, err := api.NewMux(api.Config{
		Addr:                     ":0",
		Jellyfin:                 appSvc.HandleJellyfinWebhook,
		Discord:                  discordSvc,
		HandleDiscordInteraction: appSvc.HandleDiscordComponentInteraction,
	})
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	payload := []byte(`{"ItemId":"` + movieID + `","ItemType":"Movie","Name":"Sample Movie","EventId":"evt-delete-arr","NotificationType":"ItemDeleted"}`)
	resp, err := http.Post(server.URL+"/webhooks/jellyfin", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	err = store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if _, found, err := tx.GetMedia(context.Background(), movieID); err != nil {
			return err
		} else if found {
			t.Fatalf("expected deleted movie media to be removed from store")
		}
		if _, found, err := tx.GetFlow(context.Background(), "target:movie:"+movieID); err != nil {
			return err
		} else if found {
			t.Fatalf("expected deleted movie flow to be removed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify removed state: %v", err)
	}
}

func getFlow(store *bboltrepo.Store, itemID string) (domain.Flow, bool, error) {
	var flow domain.Flow
	var found bool
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		var err error
		flow, found, err = tx.GetFlow(context.Background(), itemID)
		return err
	})
	return flow, found, err
}

func signDiscord(req *http.Request, priv ed25519.PrivateKey, body []byte) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	msg := append([]byte(timestamp), body...)
	sig := ed25519.Sign(priv, msg)
	req.Header.Set("X-Signature-Timestamp", timestamp)
	req.Header.Set("X-Signature-Ed25519", hex.EncodeToString(sig))
}
