package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
)

func newTestStore(t *testing.T) *bboltrepo.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })
	return store
}

func interaction(action, itemID string, version int64, id string) discord.IncomingInteraction {
	return discord.IncomingInteraction{
		Raw:           &discordgo.Interaction{},
		Type:          discordgo.InteractionMessageComponent,
		InteractionID: id,
		Token:         "tok",
		CustomID:      "jr:v1:" + action + ":" + itemID + ":" + strconv.FormatInt(version, 10),
	}
}

func snowflakeIDFor(t time.Time) string {
	const discordEpochMs = int64(1420070400000)
	ms := t.UTC().UnixMilli()
	if ms < discordEpochMs {
		ms = discordEpochMs
	}
	id := (ms - discordEpochMs) << 22
	return strconv.FormatInt(id, 10)
}

func TestHITLArchiveLeavesNoDeletionJob(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	targetID := "target:item:item-archive"
	resp, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("archive", targetID, 0, snowflakeIDFor(now)))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}
	if resp == nil || resp.Data == nil {
		t.Fatal("expected response data")
	}
	if resp.Type != discordgo.InteractionResponseUpdateMessage {
		t.Fatalf("expected update message response, got %d", resp.Type)
	}
	if len(resp.Data.Components) != 0 {
		t.Fatalf("expected cleared message components, got %d", len(resp.Data.Components))
	}
	if resp.Data.Content == "" {
		t.Fatal("expected decision summary content")
	}

	flow := mustGetFlow(t, store, targetID)
	if flow.State != domain.FlowStateArchived {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(365*24*time.Hour), 100, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	for _, job := range jobs {
		if job.ItemID == targetID {
			t.Fatalf("expected no queued jobs for archive action, found: %#v", job)
		}
	}
}

func TestHITLDeleteQueuesImmediateDeleteJob(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 12, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	targetID := "target:item:item-delete"
	_, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("delete", targetID, 0, snowflakeIDFor(now)))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}

	flow := mustGetFlow(t, store, targetID)
	if flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now, 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one job, got %d", len(jobs))
	}
	if jobs[0].Kind != domain.JobKindExecuteDelete {
		t.Fatalf("unexpected job kind: %s", jobs[0].Kind)
	}
}

func TestApplyAIDecisionDeleteQueuesImmediateDeleteJob(t *testing.T) {
	store := newTestStore(t)
	var wokeAt time.Time
	wakeCount := 0
	svc := NewService(store, nil, func(at time.Time) {
		wakeCount++
		wokeAt = at
	})
	now := time.Date(2026, 4, 7, 13, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:item:item-ai-delete"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "item",
			DisplayName: "AI Delete Target",
			State:       domain.FlowStateActive,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	if err := svc.ApplyAIDecision(context.Background(), itemID, "delete"); err != nil {
		t.Fatalf("apply ai decision: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now, 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	found := false
	for _, job := range jobs {
		if job.ItemID == itemID && job.Kind == domain.JobKindExecuteDelete {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected execute_delete job from ai decision")
	}
	if wakeCount != 1 {
		t.Fatalf("expected one scheduler wake, got %d", wakeCount)
	}
	if !wokeAt.Equal(now) {
		t.Fatalf("expected wake at now, got %s want %s", wokeAt, now)
	}
}

func TestApplyAIDecisionFinalizesOpenHITLPrompt(t *testing.T) {
	store := newTestStore(t)
	discordSvc, err := discord.NewService("", nil)
	if err != nil {
		t.Fatalf("new discord service: %v", err)
	}

	svc := NewService(store, nil, nil)
	svc.SetDiscordService(discordSvc)
	now := time.Date(2026, 4, 7, 13, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:item:item-ai-archive"
	err = store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "item",
			DisplayName: "AI Archive Target",
			State:       domain.FlowStatePendingReview,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			Discord:   domain.DiscordContext{ChannelID: "chan-1", MessageID: "msg-1"},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	finalized := false
	discordSvc.SetEditPromptHookForTest(func(ctx context.Context, channelID, messageID, content string) error {
		if channelID != "chan-1" || messageID != "msg-1" {
			t.Fatalf("unexpected finalize target: %s/%s", channelID, messageID)
		}
		if !strings.Contains(content, "Decision: ARCHIVE for AI Archive Target (AI).") {
			t.Fatalf("unexpected finalize content: %s", content)
		}
		finalized = true
		return nil
	})

	if err := svc.ApplyAIDecision(context.Background(), itemID, "archive"); err != nil {
		t.Fatalf("apply ai decision: %v", err)
	}
	if !finalized {
		t.Fatal("expected open HITL prompt to be finalized")
	}
	flow := mustGetFlow(t, store, itemID)
	if flow.State != domain.FlowStateArchived {
		t.Fatalf("expected archived state, got %s", flow.State)
	}
}

func TestApplyAIDecisionUnarchiveEnqueuesEvaluateNow(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 14, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:item:item-ai-unarchive"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "item",
			DisplayName: "AI Unarchive Target",
			State:       domain.FlowStateArchived,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	if err := svc.ApplyAIDecision(context.Background(), itemID, "unarchive"); err != nil {
		t.Fatalf("apply ai decision: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected active state, got %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now, 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	foundEval := false
	for _, job := range jobs {
		if job.ItemID == itemID && job.Kind == domain.JobKindEvaluatePolicy {
			foundEval = true
			break
		}
	}
	if !foundEval {
		t.Fatal("expected evaluate_policy job to be queued after unarchive")
	}
}

func TestApplyAIDelayDaysSetsPolicyAndSchedulesEval(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 15, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:season:season-ai-delay"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "season",
			DisplayName: "AI Delay Season",
			State:       domain.FlowStatePendingReview,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	if err := svc.ApplyAIDelayDays(context.Background(), itemID, 12); err != nil {
		t.Fatalf("apply ai delay days: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.PolicySnapshot.ExpireAfterDays != 12 {
		t.Fatalf("expected policy expire days 12, got %d", flow.PolicySnapshot.ExpireAfterDays)
	}
	if flow.NextActionAt.Before(now.Add(11 * 24 * time.Hour)) {
		t.Fatalf("expected next action ~12 days out, got %s", flow.NextActionAt)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(13*24*time.Hour), 20, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	foundEval := false
	for _, job := range jobs {
		if job.ItemID == itemID && job.Kind == domain.JobKindEvaluatePolicy {
			foundEval = true
			break
		}
	}
	if !foundEval {
		t.Fatal("expected scheduled evaluate_policy job after ai delay")
	}
}

func TestApplyAIDelayDaysDefersLazilyAndFinalizesHITLPrompt(t *testing.T) {
	store := newTestStore(t)
	discordSvc, err := discord.NewService("", nil)
	if err != nil {
		t.Fatalf("new discord service: %v", err)
	}

	svc := NewService(store, nil, nil)
	svc.SetDiscordService(discordSvc)
	now := time.Date(2026, 4, 7, 15, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:season:season-ai-delay-prompt"
	err = store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:         "flow:" + itemID,
			ItemID:         itemID,
			SubjectType:    "season",
			DisplayName:    "Season Delay Prompt",
			State:          domain.FlowStatePendingReview,
			Version:        0,
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			Discord:        domain.DiscordContext{ChannelID: "ch-delay", MessageID: "msg-delay"},
			CreatedAt:      now,
			UpdatedAt:      now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	finalized := false
	discordSvc.SetEditPromptHookForTest(func(ctx context.Context, channelID, messageID, content string) error {
		if channelID != "ch-delay" || messageID != "msg-delay" {
			t.Fatalf("unexpected finalize target: %s/%s", channelID, messageID)
		}
		if !strings.Contains(content, "Decision: DELAY 7 days for Season Delay Prompt (AI).") {
			t.Fatalf("unexpected finalize content: %s", content)
		}
		finalized = true
		return nil
	})

	if err := svc.ApplyAIDelayDays(context.Background(), itemID, 7); err != nil {
		t.Fatalf("apply ai delay days: %v", err)
	}
	if !finalized {
		t.Fatal("expected HITL message to be finalized on ai delay")
	}

	jobsEarly, err := store.LeaseDueJobs(context.Background(), now.Add(6*24*time.Hour), 20, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease early jobs: %v", err)
	}
	for _, job := range jobsEarly {
		if job.ItemID == itemID && job.Kind == domain.JobKindEvaluatePolicy {
			t.Fatalf("expected no evaluate job before delay window, got %#v", job)
		}
	}

	jobsLate, err := store.LeaseDueJobs(context.Background(), now.Add(8*24*time.Hour), 20, "test2", time.Minute)
	if err != nil {
		t.Fatalf("lease late jobs: %v", err)
	}
	foundEval := false
	for _, job := range jobsLate {
		if job.ItemID == itemID && job.Kind == domain.JobKindEvaluatePolicy {
			foundEval = true
			break
		}
	}
	if !foundEval {
		t.Fatal("expected evaluate job to become due after delay window")
	}
}

func TestSetGlobalPolicyDefaultsUpdateMetaOnly(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 16, 0, 0, 0, time.UTC)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{FlowID: "flow:target:season:s-g1", ItemID: "target:season:s-g1", SubjectType: "season", DisplayName: "Season G1", State: domain.FlowStateActive, Version: 0, PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"}, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{FlowID: "flow:target:movie:m-g2", ItemID: "target:movie:m-g2", SubjectType: "movie", DisplayName: "Movie G2", State: domain.FlowStatePendingReview, Version: 0, PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"}, CreatedAt: now, UpdatedAt: now}, 0)
	})
	if err != nil {
		t.Fatalf("seed flows: %v", err)
	}

	if err := svc.SetGlobalReviewDays(context.Background(), 60); err != nil {
		t.Fatalf("set global review days: %v", err)
	}
	if err := svc.SetGlobalDeferDays(context.Background(), 15); err != nil {
		t.Fatalf("set global defer days: %v", err)
	}

	err = store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		raw, ok, err := tx.GetMeta(context.Background(), metaReviewDays)
		if err != nil {
			return err
		}
		if !ok || raw != "60" {
			return fmt.Errorf("unexpected review meta value: ok=%v raw=%q", ok, raw)
		}
		raw, ok, err = tx.GetMeta(context.Background(), metaDeferDays)
		if err != nil {
			return err
		}
		if !ok || raw != "15" {
			return fmt.Errorf("unexpected defer meta value: ok=%v raw=%q", ok, raw)
		}

		flowA, found, err := tx.GetFlow(context.Background(), "target:season:s-g1")
		if err != nil || !found {
			return fmt.Errorf("flow A missing after global settings: found=%v err=%w", found, err)
		}
		if flowA.Version != 0 || flowA.PolicySnapshot.ExpireAfterDays != 30 {
			return fmt.Errorf("flow A should remain unchanged, got version=%d review=%d", flowA.Version, flowA.PolicySnapshot.ExpireAfterDays)
		}
		flowB, found, err := tx.GetFlow(context.Background(), "target:movie:m-g2")
		if err != nil || !found {
			return fmt.Errorf("flow B missing after global settings: found=%v err=%w", found, err)
		}
		if flowB.Version != 0 || flowB.PolicySnapshot.ExpireAfterDays != 30 {
			return fmt.Errorf("flow B should remain unchanged, got version=%d review=%d", flowB.Version, flowB.PolicySnapshot.ExpireAfterDays)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify global defaults update: %v", err)
	}
}

func TestGlobalDeferDaysAppliesLazilyOnNextDelayAction(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 17, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := svc.SetGlobalDeferDays(context.Background(), 15); err != nil {
		t.Fatalf("set global defer days: %v", err)
	}

	targetID := "target:season:season-lazy-defer"
	_, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("delay", targetID, 0, snowflakeIDFor(now)))
	if err != nil {
		t.Fatalf("handle delay interaction: %v", err)
	}

	flow := mustGetFlow(t, store, targetID)
	if want := now.Add(15 * 24 * time.Hour); !flow.NextActionAt.Equal(want) {
		t.Fatalf("expected delay to use global defer days, got=%s want=%s", flow.NextActionAt, want)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(16*24*time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	found := false
	for _, job := range jobs {
		if job.ItemID == targetID && job.Kind == domain.JobKindEvaluatePolicy {
			if !job.RunAt.Equal(now.Add(15 * 24 * time.Hour)) {
				t.Fatalf("expected evaluate job runAt to match global defer, got %s", job.RunAt)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("expected evaluate job after delay interaction")
	}
}

func TestHITLDelaySchedulesFutureEvaluation(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 13, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	targetID := "target:item:item-delay"
	_, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("delay", targetID, 0, snowflakeIDFor(now)))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}

	flow := mustGetFlow(t, store, targetID)
	if flow.State != domain.FlowStatePendingReview {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}
	if want := now.Add(24 * time.Hour); !flow.NextActionAt.Equal(want) {
		t.Fatalf("unexpected next action: got=%s want=%s", flow.NextActionAt, want)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(48*time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("unexpected jobs: %#v", jobs)
	}
	if !jobs[0].RunAt.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("unexpected evaluate runAt: got=%s want=%s", jobs[0].RunAt, now.Add(24*time.Hour))
	}
}

func TestWebhookIndexesFlowAndJobAndDedupe(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 14, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	event := jellyfin.WebhookEvent{
		Payload:   jellyfin.WebhookPayload{ItemID: "item-webhook", ItemType: "Movie", Name: "Webhook Movie", NotificationType: "ItemAdded", EventID: "evt-1"},
		Raw:       map[string]any{"ItemId": "item-webhook", "NotificationType": "ItemAdded", "EventId": "evt-1"},
		ItemID:    "item-webhook",
		EventID:   "evt-1",
		EventType: "ItemAdded",
		DedupeKey: "jellyfin:evt-1",
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("first webhook handle: %v", err)
	}
	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("duplicate webhook handle should be no-op: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:item-webhook")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		processed, err := tx.IsProcessed(context.Background(), "jellyfin:evt-1")
		if err != nil {
			return err
		}
		if !processed {
			t.Fatalf("expected dedupe key to be marked processed")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify dedupe key: %v", err)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now, 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("expected single evaluate job after dedupe, got %#v", jobs)
	}
}

func TestWebhookEpisodeCatalogEventAggregatesToSeasonTarget(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 15, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	event := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "ep-1",
			ItemType:         "Episode",
			Name:             "Pilot",
			SeasonID:         "season-1",
			SeasonName:       "Season 1",
			SeriesID:         "series-1",
			SeriesName:       "My Show",
			NotificationType: "ItemAdded",
			EventID:          "evt-episode-1",
		},
		Raw:       map[string]any{"ItemId": "ep-1", "ItemType": "Episode", "SeasonId": "season-1", "SeriesId": "series-1", "EventId": "evt-episode-1"},
		ItemID:    "ep-1",
		EventID:   "evt-episode-1",
		EventType: "ItemAdded",
		DedupeKey: "jellyfin:evt-episode-1",
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	seasonFlow := mustGetFlow(t, store, "target:season:season-1")
	if seasonFlow.DisplayName != "Season 1 of My Show" {
		t.Fatalf("unexpected season flow display name: %s", seasonFlow.DisplayName)
	}
	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(120*24*time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one evaluate job for season target, got %d (%#v)", len(jobs), jobs)
	}
}

func TestParseCustomIDWithColonsInTargetID(t *testing.T) {
	parsed, err := parseCustomID("jr:v1:archive:target:series:series:part:1:42")
	if err != nil {
		t.Fatalf("parse custom id: %v", err)
	}
	if parsed.Action != "archive" {
		t.Fatalf("unexpected action: %s", parsed.Action)
	}
	if parsed.ItemID != "target:series:series:part:1" {
		t.Fatalf("unexpected parsed item id: %s", parsed.ItemID)
	}
	if parsed.Version != 42 {
		t.Fatalf("unexpected version: %d", parsed.Version)
	}
}

func TestDeriveTargetsUsesIDsNotTitles(t *testing.T) {
	event := jellyfin.WebhookEvent{Payload: jellyfin.WebhookPayload{
		ItemType:   "Episode",
		Name:       "Some:Anime:Episode",
		SeasonID:   "season-01",
		SeasonName: "Season: 1",
		SeriesID:   "series-abc",
		SeriesName: "Anime: Saga",
	}}

	targets := deriveTargets(event)
	if len(targets) != 1 {
		t.Fatalf("expected one target, got %d", len(targets))
	}
	if targets[0].Canonical != "target:season:season-01" {
		t.Fatalf("unexpected season canonical key: %s", targets[0].Canonical)
	}
}

func TestDeriveTargetsSkipsSeriesItems(t *testing.T) {
	event := jellyfin.WebhookEvent{Payload: jellyfin.WebhookPayload{
		ItemType:   "Series",
		ItemID:     "series-abc",
		SeriesName: "My Show",
		Name:       "My Show",
	}}

	targets := deriveTargets(event)
	if len(targets) != 0 {
		t.Fatalf("expected no targets for series payloads, got %#v", targets)
	}
}

func TestIngestBackfillItemsSchedulesDeferredEvaluateFromLastPlay(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 8, 9, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	lastPlayed := now.Add(-2 * time.Hour)
	err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:       "ep-backfill-1",
		ItemType:     "Episode",
		SeasonID:     "season-1",
		SeasonName:   "Season 1",
		SeriesID:     "series-1",
		SeriesName:   "Show A",
		Name:         "Pilot",
		LastPlayedAt: lastPlayed,
		PlayCount:    3,
	}})
	if err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	flow := mustGetFlow(t, store, "target:season:season-1")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}
	wantRunAt := lastPlayed.Add(30 * 24 * time.Hour)
	if !flow.NextActionAt.Equal(wantRunAt) {
		t.Fatalf("unexpected next action: got=%s want=%s", flow.NextActionAt, wantRunAt)
	}

	jobsEarly, err := store.LeaseDueJobs(context.Background(), now.Add(time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs early: %v", err)
	}
	if len(jobsEarly) != 0 {
		t.Fatalf("expected no due jobs before scheduled time, got %#v", jobsEarly)
	}

	jobsDue, err := store.LeaseDueJobs(context.Background(), wantRunAt.Add(time.Minute), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs due: %v", err)
	}
	if len(jobsDue) != 1 || jobsDue[0].Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("unexpected due jobs: %#v", jobsDue)
	}
}

func TestIngestBackfillItemsPreservesHigherPlaybackMetrics(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:         "movie-1",
			Name:           "Movie One",
			Title:          "Movie One",
			ItemType:       "Movie",
			LastPlayedAt:   now.Add(-time.Hour),
			PlayCountTotal: 9,
			UpdatedAt:      now.Add(-time.Hour),
		})
	}); err != nil {
		t.Fatalf("seed media: %v", err)
	}

	err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:     "movie-1",
		ItemType:   "Movie",
		Name:       "Movie One",
		PlayCount:  0,
		ImageURL:   "",
		SeriesName: "",
	}})
	if err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	media := mustGetMedia(t, store, "movie-1")
	if media.PlayCountTotal != 9 {
		t.Fatalf("expected preserved play count 9, got %d", media.PlayCountTotal)
	}
	if media.LastPlayedAt.IsZero() {
		t.Fatal("expected preserved last played timestamp")
	}
}

func TestLiveWebhookUpdatesSameBackfilledItem(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 11, 9, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:   "movie-live-1",
		ItemType: "Movie",
		Name:     "Old Title",
	}}); err != nil {
		t.Fatalf("seed backfill item: %v", err)
	}

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "movie-live-1",
			ItemType:         "Movie",
			Name:             "Updated Title",
			NotificationType: "ItemUpdated",
			EventID:          "evt-live-1",
		},
		Raw:       map[string]any{"EventId": "evt-live-1", "ItemId": "movie-live-1"},
		ItemID:    "movie-live-1",
		EventID:   "evt-live-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-live-1",
	})
	if err != nil {
		t.Fatalf("handle live webhook: %v", err)
	}

	media := mustGetMedia(t, store, "movie-live-1")
	if media.Name != "Updated Title" {
		t.Fatalf("expected updated media title, got %q", media.Name)
	}
	flow := mustGetFlow(t, store, "target:movie:movie-live-1")
	if flow.DisplayName != "Updated Title" {
		t.Fatalf("expected updated flow display name, got %q", flow.DisplayName)
	}
}

func TestBackfillItemDedupeKeyUsesRevisionNotOrdinalWhenAvailable(t *testing.T) {
	item := jellyfin.ItemSnapshot{ItemID: "movie-1", DateLastMediaAdded: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	keyA := backfillItemDedupeKey(item, 1)
	keyB := backfillItemDedupeKey(item, 999)
	if keyA != keyB {
		t.Fatalf("expected stable dedupe key across ordinals when revision exists: %s != %s", keyA, keyB)
	}
}

func TestProcessWebhookBatchesFlushesAtBatchSize(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SetBackfillWriteBatching(2, time.Second, 10)

	ch := make(chan backfillWriteOp, 2)
	evt1 := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{ItemID: "movie-batch-1", ItemType: "Movie", Name: "Batch One", NotificationType: "ItemUpdated"},
		Raw:     map[string]any{"source": "test"},
		ItemID:  "movie-batch-1", EventType: "ItemUpdated", DedupeKey: "test:batch:1",
	}
	evt2 := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{ItemID: "movie-batch-2", ItemType: "Movie", Name: "Batch Two", NotificationType: "ItemUpdated"},
		Raw:     map[string]any{"source": "test"},
		ItemID:  "movie-batch-2", EventType: "ItemUpdated", DedupeKey: "test:batch:2",
	}
	ch <- backfillWriteOp{event: &evt1}
	ch <- backfillWriteOp{event: &evt2}
	close(ch)

	if err := svc.processWebhookBatches(context.Background(), ch, "test_batch"); err != nil {
		t.Fatalf("process webhook batches: %v", err)
	}

	m1 := mustGetMedia(t, store, "movie-batch-1")
	m2 := mustGetMedia(t, store, "movie-batch-2")
	if m1.Name != "Batch One" || m2.Name != "Batch Two" {
		t.Fatalf("unexpected media names: m1=%q m2=%q", m1.Name, m2.Name)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		processed1, err := tx.IsProcessed(context.Background(), "test:batch:1")
		if err != nil {
			return err
		}
		processed2, err := tx.IsProcessed(context.Background(), "test:batch:2")
		if err != nil {
			return err
		}
		if !processed1 || !processed2 {
			t.Fatal("expected both batched events marked processed")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify processed keys: %v", err)
	}
}

func TestProcessWebhookBatchesFlushesPartialBatchOnClose(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 11, 10, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SetBackfillWriteBatching(100, time.Second, 10)

	ch := make(chan backfillWriteOp, 1)
	evt := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{ItemID: "movie-batch-close-1", ItemType: "Movie", Name: "Batch Close", NotificationType: "ItemUpdated"},
		Raw:     map[string]any{"source": "test"},
		ItemID:  "movie-batch-close-1", EventType: "ItemUpdated", DedupeKey: "test:batch:close:1",
	}
	ch <- backfillWriteOp{event: &evt}
	close(ch)

	if err := svc.processWebhookBatches(context.Background(), ch, "test_batch_close"); err != nil {
		t.Fatalf("process webhook batches: %v", err)
	}

	m := mustGetMedia(t, store, "movie-batch-close-1")
	if m.Name != "Batch Close" {
		t.Fatalf("unexpected media name: %q", m.Name)
	}
}

func TestIngestBackfillItemsWithCursorPersistsCursorMeta(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 11, 11, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := svc.IngestBackfillItemsWithCursor(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:   "movie-cursor-1",
		ItemType: "Movie",
		Name:     "Cursor Movie",
	}}, "backfill.cursor.v1", `{"phase":"items","items_start_index":1}`); err != nil {
		t.Fatalf("ingest with cursor: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		raw, ok, err := tx.GetMeta(context.Background(), "backfill.cursor.v1")
		if err != nil {
			return err
		}
		if !ok || raw == "" {
			t.Fatal("expected cursor meta written by backfill writer")
		}
		if raw != `{"phase":"items","items_start_index":1}` {
			t.Fatalf("unexpected cursor payload: %s", raw)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify cursor meta: %v", err)
	}
}

func TestIngestBackfillPlaybackUsesOriginalEventTimestamp(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	eventAt := now.Add(-72 * time.Hour)
	err := svc.IngestBackfillPlayback(context.Background(), []jellyfin.PlaybackEvent{{
		ItemID: "movie-playback-ts",
		Type:   "PlaybackStart",
		Name:   "Movie Playback TS",
		Date:   eventAt,
	}})
	if err != nil {
		t.Fatalf("ingest backfill playback: %v", err)
	}

	media := mustGetMedia(t, store, "movie-playback-ts")
	if !media.LastPlayedAt.Equal(eventAt) {
		t.Fatalf("expected last played at original event time, got=%s want=%s", media.LastPlayedAt, eventAt)
	}
	if media.PlayCountTotal != 1 {
		t.Fatalf("expected play count to increment to 1, got %d", media.PlayCountTotal)
	}
}

func TestIngestBackfillPlaybackDoesNotCreateReviewFlowWithoutItemType(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	err := svc.IngestBackfillPlayback(context.Background(), []jellyfin.PlaybackEvent{{
		ItemID: "movie-no-type",
		Type:   "PlaybackStart",
		Name:   "alice is playing Movie X",
		Date:   now.Add(-time.Hour),
	}})
	if err != nil {
		t.Fatalf("ingest backfill playback: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetFlow(context.Background(), "target:item:movie-no-type")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected no review flow from playback-only backfill event without item type")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow absence: %v", err)
	}
}

func TestHandlePlaybackWebhookDoesNotCreateFlowOrOverwriteNames(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:         "movie-play-1",
			Name:           "Canonical Movie",
			Title:          "Canonical Movie",
			ItemType:       "Movie",
			PlayCountTotal: 2,
			UpdatedAt:      now.Add(-time.Hour),
		}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:movie:movie-play-1",
			ItemID:      "target:movie:movie-play-1",
			SubjectType: "movie",
			DisplayName: "Canonical Movie",
			State:       domain.FlowStateActive,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now.Add(-time.Hour),
			UpdatedAt: now.Add(-time.Hour),
		}, 0)
	}); err != nil {
		t.Fatalf("seed existing media/flow: %v", err)
	}

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "movie-play-1",
			ItemType:         "Movie",
			Name:             "alice is playing Canonical Movie",
			NotificationType: "PlaybackStart",
		},
		Raw:        map[string]any{"EventId": "evt-play-1"},
		ItemID:     "movie-play-1",
		EventType:  "PlaybackStart",
		DedupeKey:  "jellyfin:evt-play-1",
		OccurredAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("handle playback webhook: %v", err)
	}

	media := mustGetMedia(t, store, "movie-play-1")
	if media.Name != "Canonical Movie" {
		t.Fatalf("expected canonical media name, got %q", media.Name)
	}
	if media.PlayCountTotal != 3 {
		t.Fatalf("expected playback count increment, got %d", media.PlayCountTotal)
	}

	flow := mustGetFlow(t, store, "target:movie:movie-play-1")
	if flow.DisplayName != "Canonical Movie" {
		t.Fatalf("expected canonical flow name, got %q", flow.DisplayName)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetFlow(context.Background(), "target:item:movie-play-new")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected no new flow created from playback event")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify no new flow: %v", err)
	}
}

func TestHandleSeasonRemovalMarksChildrenAndSeasonFlowDeleted(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 10, 13, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-rm-1", SeasonID: "season-rm-1", SeasonName: "Season X", UpdatedAt: now}); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-rm-2", SeasonID: "season-rm-1", SeasonName: "Season X", UpdatedAt: now}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:season:season-rm-1",
			ItemID:      "target:season:season-rm-1",
			SubjectType: "season",
			DisplayName: "Season X of Show Y",
			State:       domain.FlowStateActive,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed season state: %v", err)
	}

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "season-rm-1",
			ItemType:         "Season",
			SeasonID:         "season-rm-1",
			SeasonName:       "Season X",
			NotificationType: "ItemDeleted",
			EventID:          "evt-season-rm",
		},
		Raw:       map[string]any{"EventId": "evt-season-rm"},
		ItemID:    "season-rm-1",
		EventType: "ItemDeleted",
		DedupeKey: "jellyfin:evt-season-rm",
	})
	if err != nil {
		t.Fatalf("handle season removal: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetMedia(context.Background(), "ep-rm-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected season child ep-rm-1 deleted from media index")
		}

		_, found, err = tx.GetMedia(context.Background(), "ep-rm-2")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected season child ep-rm-2 deleted from media index")
		}

		_, found, err = tx.GetFlow(context.Background(), "target:season:season-rm-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected season flow deleted")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify deletion state: %v", err)
	}
}

func TestCatalogStaleEventDoesNotOverwriteNewerMetadata(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 8, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	newer := jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "movie-stale-1", ItemType: "Movie", Name: "Newest Title", NotificationType: "ItemUpdated", EventID: "evt-new"},
		Raw:        map[string]any{"EventId": "evt-new"},
		ItemID:     "movie-stale-1",
		EventID:    "evt-new",
		EventType:  "ItemUpdated",
		DedupeKey:  "jellyfin:evt-new",
		OccurredAt: now,
	}
	if err := svc.HandleJellyfinWebhook(context.Background(), newer); err != nil {
		t.Fatalf("apply newer catalog event: %v", err)
	}
	before := mustGetFlow(t, store, "target:movie:movie-stale-1")

	older := jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "movie-stale-1", ItemType: "Movie", Name: "Older Title", NotificationType: "ItemUpdated", EventID: "evt-old"},
		Raw:        map[string]any{"EventId": "evt-old"},
		ItemID:     "movie-stale-1",
		EventID:    "evt-old",
		EventType:  "ItemUpdated",
		DedupeKey:  "jellyfin:evt-old",
		OccurredAt: now.Add(-2 * time.Hour),
	}
	if err := svc.HandleJellyfinWebhook(context.Background(), older); err != nil {
		t.Fatalf("apply older catalog event: %v", err)
	}

	media := mustGetMedia(t, store, "movie-stale-1")
	if media.Name != "Newest Title" {
		t.Fatalf("expected newer media title preserved, got %q", media.Name)
	}
	flow := mustGetFlow(t, store, "target:movie:movie-stale-1")
	if flow.DisplayName != "Newest Title" {
		t.Fatalf("expected newer flow display preserved, got %q", flow.DisplayName)
	}
	if flow.Version != before.Version {
		t.Fatalf("expected stale event to avoid flow version churn: before=%d after=%d", before.Version, flow.Version)
	}
}

func TestPlaybackStaleEventDoesNotIncrementPlayCount(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 9, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:              "movie-play-stale",
			Name:                "Play Movie",
			Title:               "Play Movie",
			ItemType:            "Movie",
			LastPlayedAt:        now,
			LastPlaybackEventAt: now,
			PlayCountTotal:      7,
			UpdatedAt:           now,
		})
	}); err != nil {
		t.Fatalf("seed media: %v", err)
	}

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "movie-play-stale", ItemType: "Movie", NotificationType: "PlaybackStart", EventID: "evt-play-old"},
		Raw:        map[string]any{"EventId": "evt-play-old"},
		ItemID:     "movie-play-stale",
		EventID:    "evt-play-old",
		EventType:  "PlaybackStart",
		DedupeKey:  "jellyfin:evt-play-old",
		OccurredAt: now.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("apply stale playback event: %v", err)
	}

	media := mustGetMedia(t, store, "movie-play-stale")
	if media.PlayCountTotal != 7 {
		t.Fatalf("expected play count unchanged for stale playback, got %d", media.PlayCountTotal)
	}
	if !media.LastPlayedAt.Equal(now) {
		t.Fatalf("expected last played unchanged, got %s", media.LastPlayedAt)
	}
}

func TestCatalogEventUsesPayloadTimestampNotServiceClock(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	svc.now = func() time.Time { return time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC) }

	payloadTS := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:             "movie-ts-1",
			ItemType:           "Movie",
			Name:               "Timestamped",
			NotificationType:   "ItemUpdated",
			DateLastMediaAdded: payloadTS,
			EventID:            "evt-ts-1",
		},
		Raw:       map[string]any{"EventId": "evt-ts-1"},
		ItemID:    "movie-ts-1",
		EventID:   "evt-ts-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-ts-1",
	})
	if err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	media := mustGetMedia(t, store, "movie-ts-1")
	if !media.LastCatalogEventAt.Equal(payloadTS) {
		t.Fatalf("expected catalog event timestamp from payload, got=%s want=%s", media.LastCatalogEventAt, payloadTS)
	}
	if !media.CreatedAt.Equal(payloadTS) {
		t.Fatalf("expected media created timestamp from payload, got=%s want=%s", media.CreatedAt, payloadTS)
	}
}

func TestDiscordInteractionUsesSnowflakeTimestamp(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	svc.now = func() time.Time { return time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC) }

	id := "175928847299117063"
	ts, err := discordgo.SnowflakeTimestamp(id)
	if err != nil {
		t.Fatalf("snowflake timestamp: %v", err)
	}

	_, err = svc.HandleDiscordComponentInteraction(context.Background(), interaction("keep", "target:item:item-snowflake", 0, id))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}

	flow := mustGetFlow(t, store, "target:item:item-snowflake")
	if !flow.UpdatedAt.Equal(ts.UTC()) {
		t.Fatalf("expected flow updated from discord snowflake timestamp, got=%s want=%s", flow.UpdatedAt, ts.UTC())
	}
}

func mustGetFlow(t *testing.T, store *bboltrepo.Store, itemID string) domain.Flow {
	t.Helper()
	var flow domain.Flow
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		var found bool
		var err error
		flow, found, err = tx.GetFlow(context.Background(), itemID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("flow %s not found", itemID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("get flow %s: %v", itemID, err)
	}
	return flow
}

func mustGetMedia(t *testing.T, store *bboltrepo.Store, itemID string) domain.MediaItem {
	t.Helper()
	var media domain.MediaItem
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		item, found, err := tx.GetMedia(context.Background(), itemID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("media %s not found", itemID)
		}
		media = item
		return nil
	})
	if err != nil {
		t.Fatalf("get media %s: %v", itemID, err)
	}
	return media
}
