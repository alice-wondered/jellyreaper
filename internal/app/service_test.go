package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/repo"
)

func TestHITLArchiveLeavesNoDeletionJob(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	targetID := "target:item:item-archive"
	seedFlowForInteraction(t, store, targetID, now)
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
	if !strings.Contains(resp.Data.Content, "Resolved: ARCHIVED") {
		t.Fatalf("expected archived decision wording, got %q", resp.Data.Content)
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
	svc.SyncFlowManagerClock()

	targetID := "target:item:item-delete"
	seedFlowForInteraction(t, store, targetID, now)
	resp, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("delete", targetID, 0, snowflakeIDFor(now)))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}
	if resp == nil || resp.Data == nil || !strings.Contains(resp.Data.Content, "Resolved: DELETE REQUESTED") {
		t.Fatalf("expected delete requested wording, got %#v", resp)
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

func TestStaleInteractionPointsToLatestPrompt(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 12, 45, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	targetID := "target:item:item-stale-link"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + targetID,
			ItemID:      targetID,
			SubjectType: "item",
			DisplayName: "Stale Link Item",
			State:       domain.FlowStatePendingReview,
			HITLOutcome: "",
			Discord:     domain.DiscordContext{ChannelID: "ch-new", MessageID: "msg-new"},
			Version:     2,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	interaction := discord.IncomingInteraction{
		Raw:           &discordgo.Interaction{Message: &discordgo.Message{ID: "msg-old"}},
		Type:          discordgo.InteractionMessageComponent,
		InteractionID: snowflakeIDFor(now),
		GuildID:       "guild-1",
		ChannelID:     "ch-new",
		CustomID:      "jr:v1:archive:" + targetID + ":1",
	}

	resp, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction)
	if err != nil {
		t.Fatalf("handle stale interaction: %v", err)
	}
	if resp == nil || resp.Data == nil {
		t.Fatal("expected response data")
	}
	if len(resp.Data.Components) != 0 {
		t.Fatalf("expected stale update to clear components, got %d", len(resp.Data.Components))
	}
	wantLink := "https://discord.com/channels/guild-1/ch-new/msg-new"
	if !strings.Contains(resp.Data.Content, wantLink) {
		t.Fatalf("expected stale response to include latest prompt link %q, got %q", wantLink, resp.Data.Content)
	}
	if strings.Contains(resp.Data.Content, "Resolved: DELAYED") {
		t.Fatalf("stale prompt without decision should not claim DELAYED, got %q", resp.Data.Content)
	}
	if !strings.Contains(resp.Data.Content, "No decision") {
		t.Fatalf("expected stale response to indicate no recorded decision, got %q", resp.Data.Content)
	}
}

func TestStaleInteractionWithoutNewPromptClosesCurrentMessage(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 12, 50, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	targetID := "target:item:item-stale-no-new"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + targetID,
			ItemID:      targetID,
			SubjectType: "item",
			DisplayName: "No New Prompt Item",
			State:       domain.FlowStateActive,
			HITLOutcome: "delay",
			Discord: domain.DiscordContext{
				ChannelID:         "ch-1",
				MessageID:         "",
				PreviousChannelID: "ch-1",
				PreviousMessageID: "msg-old",
			},
			Version:   2,
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	interaction := discord.IncomingInteraction{
		Raw:           &discordgo.Interaction{Message: &discordgo.Message{ID: "msg-old"}},
		Type:          discordgo.InteractionMessageComponent,
		InteractionID: snowflakeIDFor(now),
		GuildID:       "guild-1",
		ChannelID:     "ch-1",
		CustomID:      "jr:v1:delay:" + targetID + ":1",
	}

	resp, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction)
	if err != nil {
		t.Fatalf("handle stale interaction: %v", err)
	}
	if resp == nil || resp.Data == nil {
		t.Fatal("expected response data")
	}
	if len(resp.Data.Components) != 0 {
		t.Fatalf("expected stale update to clear components, got %d", len(resp.Data.Components))
	}
	if !strings.Contains(resp.Data.Content, "No newer prompt exists yet") {
		t.Fatalf("expected stale response to explain no newer prompt, got %q", resp.Data.Content)
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
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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
		if !strings.Contains(content, "Resolved: ARCHIVED for AI Archive Target (AI).") {
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
	svc.SyncFlowManagerClock()

	itemID := "target:item:item-ai-unarchive"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "item",
			DisplayName: "AI Unarchive Target",
			Discord:     domain.DiscordContext{ChannelID: "ch-unarchive", MessageID: "msg-unarchive"},
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
	if flow.Discord.MessageID != "" {
		t.Fatalf("expected unarchive to clear stale discord message id, got %q", flow.Discord.MessageID)
	}
	if flow.Discord.PreviousMessageID != "msg-unarchive" {
		t.Fatalf("expected unarchive to retain previous message id, got %q", flow.Discord.PreviousMessageID)
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
	svc.SyncFlowManagerClock()

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
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected active state after ai delay, got %s", flow.State)
	}
	if flow.Discord.MessageID != "" {
		t.Fatalf("expected ai delay to clear stale discord message id, got %q", flow.Discord.MessageID)
	}
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
	svc.SyncFlowManagerClock()

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
		if !strings.Contains(content, "Resolved: DELAYED 7 days for Season Delay Prompt (AI).") {
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
	flow := mustGetFlow(t, store, itemID)
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected active state after ai delay, got %s", flow.State)
	}
	if flow.Discord.MessageID != "" {
		t.Fatalf("expected ai delay to clear discord message id, got %q", flow.Discord.MessageID)
	}
	if flow.Discord.PreviousMessageID != "msg-delay" {
		t.Fatalf("expected ai delay to retain previous message id, got %q", flow.Discord.PreviousMessageID)
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
	if err := svc.SetGlobalHITLTimeoutHours(context.Background(), 72); err != nil {
		t.Fatalf("set global hitl timeout hours: %v", err)
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
		raw, ok, err = tx.GetMeta(context.Background(), metaHITLTimeoutHours)
		if err != nil {
			return err
		}
		if !ok || raw != "72" {
			return fmt.Errorf("unexpected hitl timeout meta value: ok=%v raw=%q", ok, raw)
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
	svc.SyncFlowManagerClock()

	if err := svc.SetGlobalDeferDays(context.Background(), 15); err != nil {
		t.Fatalf("set global defer days: %v", err)
	}

	targetID := "target:season:season-lazy-defer"
	seedFlowForInteraction(t, store, targetID, now)
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
	svc.SyncFlowManagerClock()

	targetID := "target:item:item-delay"
	seedFlowForInteraction(t, store, targetID, now)
	_, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("delay", targetID, 0, snowflakeIDFor(now)))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}

	flow := mustGetFlow(t, store, targetID)
	if flow.State != domain.FlowStateActive {
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
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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

func TestWebhookEpisodeCatalogEventFetchesSeriesProviderIDs(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 15, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	itemFetches := 0
	seriesFetches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/Items":
			ids := r.URL.Query().Get("Ids")
			w.Header().Set("Content-Type", "application/json")
			switch ids {
			case "ep-provider-1":
				itemFetches++
				_, _ = w.Write([]byte(`{"Items":[{"ProviderIds":{"Imdb":"tt6503782","Tmdb":"5957143"}}]}`))
			case "series-provider-1":
				seriesFetches++
				_, _ = w.Write([]byte(`{"Items":[{"ProviderIds":{"Tvdb":"73244","Imdb":"tt0386676","Tmdb":"2316"}}]}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc.SetJellyfinClient(jellyfin.NewClient(server.URL, "api-key", server.Client()))

	event := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "ep-provider-1",
			ItemType:         "Episode",
			Name:             "Pilot",
			SeasonID:         "season-provider-1",
			SeasonName:       "Season 1",
			SeriesID:         "series-provider-1",
			SeriesName:       "My Show",
			NotificationType: "ItemUpdated",
			EventID:          "evt-provider-1",
		},
		Raw:       map[string]any{"EventId": "evt-provider-1"},
		ItemID:    "ep-provider-1",
		EventID:   "evt-provider-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-provider-1",
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	media := mustGetMedia(t, store, "ep-provider-1")
	if media.ProviderIDs["tvdb"] != "73244" {
		t.Fatalf("expected episode media tvdb id from series provider ids, got %q", media.ProviderIDs["tvdb"])
	}
	if media.ProviderIDs["imdb"] != "tt0386676" {
		t.Fatalf("expected episode media imdb id from series provider ids, got %q", media.ProviderIDs["imdb"])
	}
	if media.ProviderIDs["tmdb"] != "2316" {
		t.Fatalf("expected episode media tmdb id from series provider ids, got %q", media.ProviderIDs["tmdb"])
	}
	seasonFlow := mustGetFlow(t, store, "target:season:season-provider-1")
	if seasonFlow.ProviderIDs["tvdb"] != "73244" {
		t.Fatalf("expected season flow tvdb id from series provider ids, got %q", seasonFlow.ProviderIDs["tvdb"])
	}
	if seasonFlow.ProviderIDs["imdb"] != "tt0386676" {
		t.Fatalf("expected season flow imdb id from series provider ids, got %q", seasonFlow.ProviderIDs["imdb"])
	}
	if seasonFlow.ProviderIDs["tmdb"] != "2316" {
		t.Fatalf("expected season flow tmdb id from series provider ids, got %q", seasonFlow.ProviderIDs["tmdb"])
	}
	if itemFetches != 1 {
		t.Fatalf("expected one episode provider id fetch, got %d", itemFetches)
	}
	if seriesFetches != 1 {
		t.Fatalf("expected one series provider id fetch, got %d", seriesFetches)
	}
}

func TestWebhookMovieCatalogEventStoresProjectionProviderIDs(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 15, 45, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	event := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "movie-provider-1",
			ItemType:         "Movie",
			Name:             "Movie Provider",
			ProviderIDs:      map[string]string{"tmdb": "603", "imdb": "tt0133093"},
			NotificationType: "ItemUpdated",
			EventID:          "evt-movie-provider-1",
		},
		Raw:       map[string]any{"EventId": "evt-movie-provider-1"},
		ItemID:    "movie-provider-1",
		EventID:   "evt-movie-provider-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-movie-provider-1",
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:movie-provider-1")
	if flow.ProviderIDs["tmdb"] != "603" {
		t.Fatalf("expected movie flow tmdb provider id, got %q", flow.ProviderIDs["tmdb"])
	}
	if flow.ProviderIDs["imdb"] != "tt0133093" {
		t.Fatalf("expected movie flow imdb provider id, got %q", flow.ProviderIDs["imdb"])
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
	svc.SyncFlowManagerClock()

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

func TestIngestBackfillReplayIsIdempotentForSameRevision(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	item := jellyfin.ItemSnapshot{
		ItemID:             "movie-backfill-replay-1",
		ItemType:           "Movie",
		Name:               "Replay Safe Movie",
		DateLastMediaAdded: now.Add(-2 * time.Hour),
		LastPlayedAt:       now.Add(-3 * time.Hour),
		PlayCount:          2,
	}

	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{item}); err != nil {
		t.Fatalf("first backfill ingest: %v", err)
	}
	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{item}); err != nil {
		t.Fatalf("replayed backfill ingest: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:movie-backfill-replay-1")
	if flow.DisplayName != "Replay Safe Movie" {
		t.Fatalf("expected stable flow display after replay, got %q", flow.DisplayName)
	}
	media := mustGetMedia(t, store, "movie-backfill-replay-1")
	if media.PlayCountTotal != 2 {
		t.Fatalf("expected stable play count after replay, got %d", media.PlayCountTotal)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		key := backfillItemDedupeKey(item, 0)
		processed, err := tx.IsProcessed(context.Background(), key)
		if err != nil {
			return err
		}
		if !processed {
			t.Fatalf("expected backfill dedupe key %q to be marked processed", key)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify dedupe key: %v", err)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(365*24*time.Hour), 100, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	evaluateJobs := 0
	for _, job := range jobs {
		if job.Kind == domain.JobKindEvaluatePolicy && job.ItemID == "target:movie:movie-backfill-replay-1" {
			evaluateJobs++
		}
	}
	if evaluateJobs != 1 {
		t.Fatalf("expected exactly one evaluate job for replayed backfill item, got %d", evaluateJobs)
	}
}

func TestBackfillAfterLivePlaybackCanAdvanceMissedDowntimePlay(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	base := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return base }

	item := jellyfin.ItemSnapshot{
		ItemID:             "movie-magicians-1",
		ItemType:           "Movie",
		Name:               "The Magicians",
		DateLastMediaAdded: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		LastPlayedAt:       base.Add(-3 * time.Hour),
		PlayCount:          1,
	}
	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{item}); err != nil {
		t.Fatalf("seed backfill: %v", err)
	}

	livePlay := base.Add(-2 * time.Hour)
	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "movie-magicians-1", ItemType: "Movie", NotificationType: "PlaybackStart", EventID: "evt-live-play-1"},
		Raw:        map[string]any{"EventId": "evt-live-play-1"},
		ItemID:     "movie-magicians-1",
		EventID:    "evt-live-play-1",
		EventType:  "PlaybackStart",
		DedupeKey:  "jellyfin:evt-live-play-1",
		OccurredAt: livePlay,
	}); err != nil {
		t.Fatalf("live playback webhook: %v", err)
	}

	missedPlay := base.Add(-time.Hour)
	item.LastPlayedAt = missedPlay
	item.PlayCount = 3
	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{item}); err != nil {
		t.Fatalf("downtime backfill replay: %v", err)
	}

	media := mustGetMedia(t, store, "movie-magicians-1")
	if !media.LastPlayedAt.Equal(missedPlay) {
		t.Fatalf("expected backfill to advance last played after downtime, got=%s want=%s", media.LastPlayedAt, missedPlay)
	}
	if media.PlayCountTotal != 3 {
		t.Fatalf("expected backfill to advance play count after downtime, got %d", media.PlayCountTotal)
	}
}

func TestFullBackfillOverLiveIndexCanAdvancePlaybackWithoutRegression(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	base := time.Date(2026, 4, 8, 15, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return base }

	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "movie-magicians-2", ItemType: "Movie", Name: "The Magicians", NotificationType: "ItemAdded", EventID: "evt-live-add-2"},
		Raw:        map[string]any{"EventId": "evt-live-add-2"},
		ItemID:     "movie-magicians-2",
		EventID:    "evt-live-add-2",
		EventType:  "ItemAdded",
		DedupeKey:  "jellyfin:evt-live-add-2",
		OccurredAt: base.Add(-4 * time.Hour),
	}); err != nil {
		t.Fatalf("seed live item added: %v", err)
	}

	livePlay := base.Add(-3 * time.Hour)
	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "movie-magicians-2", ItemType: "Movie", NotificationType: "PlaybackStart", EventID: "evt-live-play-2"},
		Raw:        map[string]any{"EventId": "evt-live-play-2"},
		ItemID:     "movie-magicians-2",
		EventID:    "evt-live-play-2",
		EventType:  "PlaybackStart",
		DedupeKey:  "jellyfin:evt-live-play-2",
		OccurredAt: livePlay,
	}); err != nil {
		t.Fatalf("seed live playback: %v", err)
	}

	missedPlay := base.Add(-time.Hour)
	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:             "movie-magicians-2",
		ItemType:           "Movie",
		Name:               "The Magicians",
		DateLastMediaAdded: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		LastPlayedAt:       missedPlay,
		PlayCount:          5,
	}}); err != nil {
		t.Fatalf("full backfill replay over live index: %v", err)
	}

	media := mustGetMedia(t, store, "movie-magicians-2")
	if !media.LastPlayedAt.Equal(missedPlay) {
		t.Fatalf("expected full backfill to advance last played over live index, got=%s want=%s", media.LastPlayedAt, missedPlay)
	}
	if media.PlayCountTotal != 5 {
		t.Fatalf("expected full backfill to advance play count over live index, got %d", media.PlayCountTotal)
	}
}

func TestBackfillReplayPreservesArchivedFlowState(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 19, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:movie:movie-archived-1",
			ItemID:      "target:movie:movie-archived-1",
			SubjectType: "movie",
			DisplayName: "The Magicians",
			State:       domain.FlowStateArchived,
			Version:     0,
			CreatedAt:   now.Add(-24 * time.Hour),
			UpdatedAt:   now.Add(-24 * time.Hour),
		}, 0)
	}); err != nil {
		t.Fatalf("seed archived flow: %v", err)
	}

	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:             "movie-archived-1",
		ItemType:           "Movie",
		Name:               "The Magicians",
		DateLastMediaAdded: now.Add(-48 * time.Hour),
	}}); err != nil {
		t.Fatalf("backfill replay: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:movie-archived-1")
	if flow.State != domain.FlowStateArchived {
		t.Fatalf("expected archived flow to remain archived, got %s", flow.State)
	}
}

func TestPlaybackDuringDelayResolvesDelayAndReschedulesEval(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 22, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	// Delay is 90 days out — longer than the 60-day configured review window.
	// A play event must win over the delay and reset the clock to lastPlay+60d.
	delayedUntil := now.Add(90 * 24 * time.Hour)
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.SetMeta(context.Background(), metaReviewDays, "60"); err != nil {
			return err
		}
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:movie:movie-play-win-1",
			ItemID:      "target:movie:movie-play-win-1",
			SubjectType: "movie",
			DisplayName: "Play Wins Movie",
			State:       domain.FlowStateActive,
			HITLOutcome: "delay",
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 0, // falls back to meta (60)
				HITLTimeoutHrs:  24,
				TimeoutAction:   "delete",
			},
			NextActionAt: delayedUntil,
			CreatedAt:    now.Add(-48 * time.Hour),
			UpdatedAt:    now.Add(-48 * time.Hour),
		}, 0); err != nil {
			return err
		}
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:    "movie-play-win-1",
			ItemType:  "Movie",
			Name:      "Play Wins Movie",
			UpdatedAt: now.Add(-48 * time.Hour),
		})
	}); err != nil {
		t.Fatalf("seed delayed flow: %v", err)
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "movie-play-win-1", ItemType: "Movie", NotificationType: "PlaybackStart", EventID: "evt-play-win-1"},
		Raw:        map[string]any{"EventId": "evt-play-win-1"},
		ItemID:     "movie-play-win-1",
		EventID:    "evt-play-win-1",
		EventType:  "PlaybackStart",
		DedupeKey:  "jellyfin:evt-play-win-1",
		OccurredAt: now,
	}); err != nil {
		t.Fatalf("playback webhook: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:movie-play-win-1")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected flow to remain active, got %s", flow.State)
	}
	if flow.HITLOutcome != "played" {
		t.Fatalf("play must resolve delay: expected HITLOutcome=played, got %q", flow.HITLOutcome)
	}
	wantNextAction := now.Add(60 * 24 * time.Hour)
	if !flow.NextActionAt.Equal(wantNextAction) {
		t.Fatalf("play must win over delay: got NextActionAt=%s want=%s", flow.NextActionAt, wantNextAction)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), wantNextAction, 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	var evalJob *domain.JobRecord
	for i := range jobs {
		if jobs[i].ItemID == "target:movie:movie-play-win-1" && jobs[i].Kind == domain.JobKindEvaluatePolicy {
			evalJob = &jobs[i]
			break
		}
	}
	if evalJob == nil {
		t.Fatal("expected evaluate_policy job rescheduled at lastPlay+reviewDays after play")
	}
	if !evalJob.RunAt.Equal(wantNextAction) {
		t.Fatalf("eval job must be at lastPlay+60d: got=%s want=%s", evalJob.RunAt, wantNextAction)
	}
}

func TestBackfillReplayPreservesDelayedActiveFlowSchedule(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 20, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()
	delayedUntil := now.Add(10 * 24 * time.Hour)
	lastPlayed := now.Add(-24 * time.Hour)

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:movie:movie-delayed-1",
			ItemID:      "target:movie:movie-delayed-1",
			SubjectType: "movie",
			DisplayName: "RWBY",
			State:       domain.FlowStateActive,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  24,
				TimeoutAction:   "delete",
			},
			NextActionAt: delayedUntil,
			CreatedAt:    now.Add(-48 * time.Hour),
			UpdatedAt:    now.Add(-48 * time.Hour),
		}, 0); err != nil {
			return err
		}
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:             "movie-delayed-1",
			ItemType:           "Movie",
			Name:               "RWBY",
			CreatedAt:          now.Add(-72 * time.Hour),
			LastPlayedAt:       lastPlayed,
			LastCatalogEventAt: now.Add(-48 * time.Hour),
			UpdatedAt:          now.Add(-48 * time.Hour),
		})
	}); err != nil {
		t.Fatalf("seed delayed flow/media: %v", err)
	}

	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:             "movie-delayed-1",
		ItemType:           "Movie",
		Name:               "RWBY",
		DateLastMediaAdded: now.Add(-72 * time.Hour),
		LastPlayedAt:       lastPlayed,
		PlayCount:          1,
	}}); err != nil {
		t.Fatalf("backfill replay: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:movie-delayed-1")
	if !flow.NextActionAt.Equal(delayedUntil) {
		t.Fatalf("expected delayed next action to be preserved, got=%s want=%s", flow.NextActionAt, delayedUntil)
	}
}

func TestBackfillReplayRecoversPendingReviewFlowWhenPlaybackAdvanced(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 21, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:         "flow:target:movie:movie-pending-backfill-1",
			ItemID:         "target:movie:movie-pending-backfill-1",
			SubjectType:    "movie",
			DisplayName:    "The Magicians",
			State:          domain.FlowStatePendingReview,
			Version:        0,
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 24, TimeoutAction: "delete"},
			Discord:        domain.DiscordContext{ChannelID: "c1", MessageID: "m1"},
			CreatedAt:      now.Add(-48 * time.Hour),
			UpdatedAt:      now.Add(-48 * time.Hour),
		}, 0); err != nil {
			return err
		}
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:       "movie-pending-backfill-1",
			ItemType:     "Movie",
			Name:         "The Magicians",
			LastPlayedAt: now.Add(-40 * 24 * time.Hour),
			UpdatedAt:    now.Add(-48 * time.Hour),
		})
	}); err != nil {
		t.Fatalf("seed pending flow: %v", err)
	}

	newPlay := now.Add(-time.Hour)
	if err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:       "movie-pending-backfill-1",
		ItemType:     "Movie",
		Name:         "The Magicians",
		LastPlayedAt: newPlay,
		PlayCount:    2,
	}}); err != nil {
		t.Fatalf("backfill replay with advanced playback: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:movie-pending-backfill-1")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected pending review flow to recover to active, got %s", flow.State)
	}
	if flow.HITLOutcome != "played" {
		t.Fatalf("expected recovered flow HITL outcome played, got %q", flow.HITLOutcome)
	}
	if flow.NextActionAt.Before(newPlay.Add(29 * 24 * time.Hour)) {
		t.Fatalf("expected recovered flow next action to move beyond new play, got %s", flow.NextActionAt)
	}
}

func TestIngestBackfillItemsEpisodeUsesSeriesProviderIDsAndCache(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 8, 9, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	seriesFetches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/Items":
			if r.URL.Query().Get("Ids") != "series-backfill-1" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			seriesFetches++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"Items":[{"ProviderIds":{"Tvdb":"9000","Imdb":"tt-series-backfill","Tmdb":"9001"}}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc.SetJellyfinClient(jellyfin.NewClient(server.URL, "api-key", server.Client()))

	err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{
		{
			ItemID:       "ep-backfill-provider-1",
			ItemType:     "Episode",
			SeasonID:     "season-backfill-1",
			SeasonName:   "Season 1",
			SeriesID:     "series-backfill-1",
			SeriesName:   "Show B",
			Name:         "Episode 1",
			ProviderIDs:  map[string]string{"imdb": "tt-episode-1", "tmdb": "7001"},
			LastPlayedAt: now,
		},
		{
			ItemID:       "ep-backfill-provider-2",
			ItemType:     "Episode",
			SeasonID:     "season-backfill-1",
			SeasonName:   "Season 1",
			SeriesID:     "series-backfill-1",
			SeriesName:   "Show B",
			Name:         "Episode 2",
			ProviderIDs:  map[string]string{"imdb": "tt-episode-2", "tmdb": "7002"},
			LastPlayedAt: now,
		},
	})
	if err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	media1 := mustGetMedia(t, store, "ep-backfill-provider-1")
	if media1.ProviderIDs["tvdb"] != "9000" {
		t.Fatalf("expected tvdb id from series provider ids for backfill episode, got %q", media1.ProviderIDs["tvdb"])
	}
	if media1.ProviderIDs["imdb"] != "tt-series-backfill" {
		t.Fatalf("expected imdb id from series provider ids for backfill episode, got %q", media1.ProviderIDs["imdb"])
	}
	if media1.ProviderIDs["tmdb"] != "9001" {
		t.Fatalf("expected tmdb id from series provider ids for backfill episode, got %q", media1.ProviderIDs["tmdb"])
	}

	media2 := mustGetMedia(t, store, "ep-backfill-provider-2")
	if media2.ProviderIDs["tvdb"] != "9000" {
		t.Fatalf("expected tvdb id from series provider ids for second backfill episode, got %q", media2.ProviderIDs["tvdb"])
	}
	seasonFlow := mustGetFlow(t, store, "target:season:season-backfill-1")
	if seasonFlow.ProviderIDs["tvdb"] != "9000" {
		t.Fatalf("expected season projection tvdb id from series provider ids, got %q", seasonFlow.ProviderIDs["tvdb"])
	}
	if seasonFlow.ProviderIDs["imdb"] != "tt-series-backfill" {
		t.Fatalf("expected season projection imdb id from series provider ids, got %q", seasonFlow.ProviderIDs["imdb"])
	}
	if seasonFlow.ProviderIDs["tmdb"] != "9001" {
		t.Fatalf("expected season projection tmdb id from series provider ids, got %q", seasonFlow.ProviderIDs["tmdb"])
	}
	if seriesFetches != 1 {
		t.Fatalf("expected one series provider id fetch across multiple episodes, got %d", seriesFetches)
	}
}

func TestIngestBackfillItemsPreservesHigherPlaybackMetrics(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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

func TestBackfillItemDedupeKeyChangesWhenPlaybackRevisionChanges(t *testing.T) {
	added := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	itemA := jellyfin.ItemSnapshot{ItemID: "movie-2", DateLastMediaAdded: added, LastPlayedAt: time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC), PlayCount: 1}
	itemB := itemA
	itemB.LastPlayedAt = itemA.LastPlayedAt.Add(2 * time.Hour)
	itemB.PlayCount = 2

	keyA := backfillItemDedupeKey(itemA, 1)
	keyB := backfillItemDedupeKey(itemB, 1)
	if keyA == keyB {
		t.Fatalf("expected dedupe key to change when playback revision changes: %s", keyA)
	}
}

func TestProcessWebhookBatchesFlushesAtBatchSize(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()
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
	svc.SyncFlowManagerClock()
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
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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

func TestEpisodeRemovalKeepsSeasonProjectionWhenEpisodesRemain(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-rm-one", SeasonID: "season-rm-keep", SeasonName: "Season Keep", UpdatedAt: now}); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-rm-two", SeasonID: "season-rm-keep", SeasonName: "Season Keep", UpdatedAt: now}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:season:season-rm-keep",
			ItemID:      "target:season:season-rm-keep",
			SubjectType: "season",
			DisplayName: "Season Keep of Show",
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
			ItemID:           "ep-rm-one",
			ItemType:         "Episode",
			SeasonID:         "season-rm-keep",
			SeasonName:       "Season Keep",
			NotificationType: "ItemDeleted",
			EventID:          "evt-episode-rm-one",
		},
		Raw:       map[string]any{"EventId": "evt-episode-rm-one"},
		ItemID:    "ep-rm-one",
		EventType: "ItemDeleted",
		DedupeKey: "jellyfin:evt-episode-rm-one",
	})
	if err != nil {
		t.Fatalf("handle episode removal: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetMedia(context.Background(), "ep-rm-one")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected removed episode media deleted")
		}

		_, found, err = tx.GetMedia(context.Background(), "ep-rm-two")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected remaining episode media to stay")
		}

		flow, found, err := tx.GetFlow(context.Background(), "target:season:season-rm-keep")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected season flow to remain while episodes still exist")
		}
		if flow.EpisodeCount != 1 {
			t.Fatalf("expected season episode count to decrement to 1, got %d", flow.EpisodeCount)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify partial episode removal state: %v", err)
	}
}

func TestCatalogStaleEventDoesNotOverwriteNewerMetadata(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 8, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

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
	svc.SyncFlowManagerClock()

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

func TestPlaybackEventClosesOpenHITLAndReschedulesEvaluation(t *testing.T) {
	store := newTestStore(t)
	discordSvc, err := discord.NewService("", nil)
	if err != nil {
		t.Fatalf("new discord service: %v", err)
	}

	svc := NewService(store, nil, nil)
	svc.SetDiscordService(discordSvc)
	now := time.Date(2026, 4, 12, 9, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:       "movie-hitl-play",
			Name:         "Playback Recovery Movie",
			Title:        "Playback Recovery Movie",
			ItemType:     "Movie",
			LastPlayedAt: now.Add(-48 * time.Hour),
			UpdatedAt:    now,
		}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:             "flow:target:movie:movie-hitl-play",
			ItemID:             "target:movie:movie-hitl-play",
			SubjectType:        "movie",
			DisplayName:        "Playback Recovery Movie",
			State:              domain.FlowStatePendingReview,
			Version:            0,
			PolicySnapshot:     domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			Discord:            domain.DiscordContext{ChannelID: "ch-play", MessageID: "msg-play"},
			DecisionDeadlineAt: now.Add(6 * time.Hour),
			NextActionAt:       now.Add(6 * time.Hour),
			CreatedAt:          now.Add(-7 * 24 * time.Hour),
			UpdatedAt:          now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed hitl flow/media: %v", err)
	}

	finalized := false
	discordSvc.SetEditPromptHookForTest(func(ctx context.Context, channelID, messageID, content string) error {
		if channelID != "ch-play" || messageID != "msg-play" {
			t.Fatalf("unexpected finalize target: %s/%s", channelID, messageID)
		}
		if !strings.Contains(content, "Resolved: PLAYED at") || !strings.Contains(content, "for Playback Recovery Movie") {
			t.Fatalf("unexpected finalize content: %s", content)
		}
		finalized = true
		return nil
	})

	err = svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "movie-hitl-play",
			ItemType:         "Movie",
			Name:             "alice is playing Playback Recovery Movie",
			NotificationType: "PlaybackStart",
			EventID:          "evt-hitl-play",
		},
		Raw:        map[string]any{"EventId": "evt-hitl-play"},
		ItemID:     "movie-hitl-play",
		EventID:    "evt-hitl-play",
		EventType:  "PlaybackStart",
		DedupeKey:  "jellyfin:evt-hitl-play",
		OccurredAt: now,
	})
	if err != nil {
		t.Fatalf("handle playback webhook: %v", err)
	}
	if !finalized {
		t.Fatal("expected open HITL prompt finalized after playback")
	}

	flow := mustGetFlow(t, store, "target:movie:movie-hitl-play")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected flow active after playback recovery, got %s", flow.State)
	}
	if !flow.DecisionDeadlineAt.IsZero() {
		t.Fatalf("expected decision deadline cleared, got %s", flow.DecisionDeadlineAt)
	}
	if flow.NextActionAt.Before(now.Add(29 * 24 * time.Hour)) {
		t.Fatalf("expected next action deferred based on playback, got %s", flow.NextActionAt)
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

func TestCatalogEventPrefersDateLastMediaAddedOverDateCreated(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	oldCreated := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	recentAdded := time.Date(2026, 4, 10, 8, 30, 0, 0, time.UTC)
	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:             "movie-ts-2",
			ItemType:           "Movie",
			Name:               "Timestamped 2",
			NotificationType:   "ItemUpdated",
			DateCreated:        oldCreated,
			DateLastMediaAdded: recentAdded,
			EventID:            "evt-ts-2",
		},
		Raw:       map[string]any{"EventId": "evt-ts-2"},
		ItemID:    "movie-ts-2",
		EventID:   "evt-ts-2",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-ts-2",
	})
	if err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	media := mustGetMedia(t, store, "movie-ts-2")
	if !media.CreatedAt.Equal(recentAdded) {
		t.Fatalf("expected media created timestamp from DateLastMediaAdded, got=%s want=%s", media.CreatedAt, recentAdded)
	}
}

func TestCatalogEventFallsBackToServiceClockWhenPayloadDatesMissing(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "movie-ts-3",
			ItemType:         "Movie",
			Name:             "Timestamped 3",
			NotificationType: "ItemUpdated",
			EventID:          "evt-ts-3",
		},
		Raw:       map[string]any{"EventId": "evt-ts-3"},
		ItemID:    "movie-ts-3",
		EventID:   "evt-ts-3",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-ts-3",
	})
	if err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	media := mustGetMedia(t, store, "movie-ts-3")
	if !media.CreatedAt.Equal(now) {
		t.Fatalf("expected media created timestamp to fallback to service clock, got=%s want=%s", media.CreatedAt, now)
	}
}

func TestItemAddedUsesEventTimestampWhenDateLastMediaAddedMissing(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	oldCreated := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	eventAddedAt := time.Date(2026, 4, 10, 12, 30, 0, 0, time.UTC)
	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "movie-ts-item-added",
			ItemType:         "Movie",
			Name:             "Added Movie",
			NotificationType: "ItemAdded",
			DateCreated:      oldCreated,
			EventID:          "evt-ts-item-added",
		},
		Raw:        map[string]any{"EventId": "evt-ts-item-added"},
		ItemID:     "movie-ts-item-added",
		EventID:    "evt-ts-item-added",
		EventType:  "ItemAdded",
		DedupeKey:  "jellyfin:evt-ts-item-added",
		OccurredAt: eventAddedAt,
	})
	if err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	media := mustGetMedia(t, store, "movie-ts-item-added")
	if !media.CreatedAt.Equal(eventAddedAt) {
		t.Fatalf("expected media created timestamp from item-added event time, got=%s want=%s", media.CreatedAt, eventAddedAt)
	}
}

func TestCollectionWebhookDoesNotCreateOperationalFlow(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 10, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "collection-1",
			ItemType:         "BoxSet",
			Name:             "My Action Collection",
			NotificationType: "ItemUpdated",
			EventID:          "evt-collection-1",
		},
		Raw:       map[string]any{"EventId": "evt-collection-1"},
		ItemID:    "collection-1",
		EventID:   "evt-collection-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-collection-1",
	})
	if err != nil {
		t.Fatalf("handle collection webhook: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flows, err := tx.ListFlows(context.Background())
		if err != nil {
			return err
		}
		for _, flow := range flows {
			if strings.Contains(flow.ItemID, "collection-1") {
				return fmt.Errorf("expected no operational flow for collection, found %s", flow.ItemID)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("verify no collection flow created: %v", err)
	}
}

func TestSeriesWebhookDoesNotCreateOperationalFlow(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 10, 45, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "series-123",
			ItemType:         "Series",
			Name:             "Series Only Event",
			NotificationType: "ItemUpdated",
			EventID:          "evt-series-123",
		},
		Raw:       map[string]any{"EventId": "evt-series-123"},
		ItemID:    "series-123",
		EventID:   "evt-series-123",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-series-123",
	})
	if err != nil {
		t.Fatalf("handle series webhook: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flows, err := tx.ListFlows(context.Background())
		if err != nil {
			return err
		}
		if len(flows) != 0 {
			return fmt.Errorf("expected no operational flows from series-only webhook, got %d", len(flows))
		}
		return nil
	}); err != nil {
		t.Fatalf("verify no series flow created: %v", err)
	}
}

func TestSeasonCatalogWebhookDoesNotCreateOperationalFlow(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 12, 10, 50, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "season-magicians-1",
			ItemType:         "Season",
			Name:             "Season 1",
			SeriesName:       "The Magicians",
			NotificationType: "ItemUpdated",
			EventID:          "evt-season-magicians-1",
		},
		Raw:       map[string]any{"EventId": "evt-season-magicians-1"},
		ItemID:    "season-magicians-1",
		EventID:   "evt-season-magicians-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-season-magicians-1",
	})
	if err != nil {
		t.Fatalf("handle season webhook: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flows, err := tx.ListFlows(context.Background())
		if err != nil {
			return err
		}
		if len(flows) != 0 {
			return fmt.Errorf("expected no operational flows from season-only catalog webhook, got %d", len(flows))
		}
		return nil
	}); err != nil {
		t.Fatalf("verify no season flow created: %v", err)
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

	seedFlowForInteraction(t, store, "target:item:item-snowflake", ts.UTC())
	_, err = svc.HandleDiscordComponentInteraction(context.Background(), interaction("archive", "target:item:item-snowflake", 0, id))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}

	flow := mustGetFlow(t, store, "target:item:item-snowflake")
	// FlowManager owns the timestamp — it uses server time, not the
	// Discord snowflake. The important thing is the state transition.
	if flow.State != domain.FlowStateArchived {
		t.Fatalf("expected archive action state, got %s", flow.State)
	}
}

func TestSourceTimestampForPlaybackPrefersLastPlayedAtOverCatalogDate(t *testing.T) {
	oldCatalog := time.Date(2025, 6, 20, 14, 1, 48, 0, time.UTC)
	recentPlay := time.Date(2026, 4, 5, 0, 29, 37, 0, time.UTC)

	event := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			NotificationType:   "PlaybackStart",
			ItemType:           "Movie",
			DateCreated:        oldCatalog,
			DateLastMediaAdded: oldCatalog,
			LastPlayedAt:       recentPlay,
		},
		EventType: "PlaybackStart",
	}

	ts, ok := sourceTimestampForJellyfinEvent(event)
	if !ok {
		t.Fatal("expected source timestamp")
	}
	if !ts.Equal(recentPlay) {
		t.Fatalf("expected playback timestamp to prefer LastPlayedAt, got=%s want=%s", ts, recentPlay)
	}
}

func TestSourceTimestampForCatalogEventPrefersCatalogDateOverLastPlayedAt(t *testing.T) {
	catalog := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	recentPlay := time.Date(2026, 4, 5, 0, 29, 37, 0, time.UTC)

	event := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			NotificationType:   "ItemUpdated",
			ItemType:           "Movie",
			DateLastMediaAdded: catalog,
			LastPlayedAt:       recentPlay,
		},
		EventType: "ItemUpdated",
	}

	ts, ok := sourceTimestampForJellyfinEvent(event)
	if !ok {
		t.Fatal("expected source timestamp")
	}
	if !ts.Equal(catalog) {
		t.Fatalf("expected catalog event timestamp to prefer DateLastMediaAdded, got=%s want=%s", ts, catalog)
	}
}

func TestFetchProviderIDsCachedRetriesAfterPreviousFailure(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Items":[{"ProviderIds":{"Tvdb":"73244"}}]}`))
	}))
	defer server.Close()

	svc.SetJellyfinClient(jellyfin.NewClient(server.URL, "api-key", server.Client()))
	if _, err := svc.fetchProviderIDsCached(context.Background(), "series-retry-1"); err == nil {
		t.Fatal("expected first provider id fetch to fail")
	}
	ids, err := svc.fetchProviderIDsCached(context.Background(), "series-retry-1")
	if err != nil {
		t.Fatalf("expected second provider id fetch to retry and succeed: %v", err)
	}
	if ids["tvdb"] != "73244" {
		t.Fatalf("expected retried provider ids to include tvdb, got %#v", ids)
	}
}

// --- DeleteFailed / resurrection / playback recovery cleanup ---------------

func TestWebhookSkipsFlowMutationsForDeleteQueued(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	itemID := "target:movie:dq-skip"
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "movie",
			DisplayName: "Original Name",
			State:       domain.FlowStateDeleteQueued,
			Version:     5,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:   jellyfin.WebhookPayload{ItemID: "dq-skip", ItemType: "Movie", Name: "Renamed", NotificationType: "ItemUpdated", EventID: "evt-dq-1"},
		Raw:       map[string]any{"ItemId": "dq-skip", "EventId": "evt-dq-1"},
		ItemID:    "dq-skip",
		EventID:   "evt-dq-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-dq-1",
	}); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.Version != 5 {
		t.Fatalf("expected version untouched at 5, got %d", flow.Version)
	}
	if flow.DisplayName != "Original Name" {
		t.Fatalf("expected display name untouched, got %q", flow.DisplayName)
	}
	if flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("expected state untouched DeleteQueued, got %s", flow.State)
	}
}

func TestWebhookSkipsFlowMutationsForDeleteFailedNonItemAdded(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 12, 5, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	itemID := "target:movie:df-skip"
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "movie",
			DisplayName: "Failed Movie",
			State:       domain.FlowStateDeleteFailed,
			Version:     8,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// ItemUpdated (not ItemAdded) on DeleteFailed → skip.
	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:   jellyfin.WebhookPayload{ItemID: "df-skip", ItemType: "Movie", Name: "Updated Title", NotificationType: "ItemUpdated", EventID: "evt-df-1"},
		Raw:       map[string]any{"ItemId": "df-skip", "EventId": "evt-df-1"},
		ItemID:    "df-skip",
		EventID:   "evt-df-1",
		EventType: "ItemUpdated",
		DedupeKey: "jellyfin:evt-df-1",
	}); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.Version != 8 || flow.State != domain.FlowStateDeleteFailed {
		t.Fatalf("expected DeleteFailed unchanged, got version=%d state=%s", flow.Version, flow.State)
	}
	if flow.DisplayName != "Failed Movie" {
		t.Fatalf("expected display name untouched, got %q", flow.DisplayName)
	}
}

func TestWebhookItemAddedResurrectsDeleteFailedFlow(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 12, 10, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	itemID := "target:movie:df-resurrect"
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			SubjectType: "movie",
			DisplayName: "Old Failed Movie",
			State:       domain.FlowStateDeleteFailed,
			Version:     11,
			CreatedAt:   now.Add(-7 * 24 * time.Hour),
			UpdatedAt:   now.Add(-7 * 24 * time.Hour),
		}, 0); err != nil {
			return err
		}
		// Stale jobs left over from the failed delete cycle.
		return tx.EnqueueJob(context.Background(), domain.JobRecord{
			JobID:     "job:eval:scheduled:" + itemID,
			ItemID:    itemID,
			Kind:      domain.JobKindEvaluatePolicy,
			Status:    domain.JobStatusPending,
			RunAt:     now.Add(time.Hour),
			CreatedAt: now,
			UpdatedAt: now,
		})
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:   jellyfin.WebhookPayload{ItemID: "df-resurrect", ItemType: "Movie", Name: "Resurrected Movie", NotificationType: "ItemAdded", EventID: "evt-resurrect"},
		Raw:       map[string]any{"ItemId": "df-resurrect", "EventId": "evt-resurrect"},
		ItemID:    "df-resurrect",
		EventID:   "evt-resurrect",
		EventType: "ItemAdded",
		DedupeKey: "jellyfin:evt-resurrect",
	}); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected resurrected flow to be Active, got %s", flow.State)
	}
	if flow.Version <= 11 {
		t.Fatalf("expected version to advance past 11, got %d", flow.Version)
	}
	if flow.DisplayName != "Resurrected Movie" {
		t.Fatalf("expected display name from new event, got %q", flow.DisplayName)
	}

	// Stale eval should have been purged then a fresh eval re-created by RequestEval.
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		freshJob, found, err := tx.GetJob(context.Background(), "job:eval:scheduled:"+itemID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected fresh eval job to be re-scheduled after resurrection")
		}
		if freshJob.Status != domain.JobStatusPending {
			t.Fatalf("expected fresh eval to be pending, got %s", freshJob.Status)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify eval: %v", err)
	}
}

func TestWebhookPlaybackRecoveryPurgesStaleHITLJobs(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 9, 12, 15, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	itemID := "target:movie:recovery"
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:             "flow:" + itemID,
			ItemID:             itemID,
			SubjectType:        "movie",
			DisplayName:        "Recovery Movie",
			State:              domain.FlowStatePendingReview,
			DecisionDeadlineAt: now.Add(24 * time.Hour),
			Version:            3,
			CreatedAt:          now,
			UpdatedAt:          now,
		}, 0); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "recovery", ItemType: "Movie", LastPlayedAt: now, UpdatedAt: now}); err != nil {
			return err
		}
		// Seed a stale prompt and stale timeout — both should be purged.
		for _, j := range []domain.JobRecord{
			{JobID: "job:prompt:" + itemID + ":1", ItemID: itemID, Kind: domain.JobKindSendHITLPrompt, Status: domain.JobStatusPending, RunAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now},
			{JobID: "job:timeout:" + itemID + ":1", ItemID: itemID, Kind: domain.JobKindHITLTimeout, Status: domain.JobStatusPending, RunAt: now.Add(2 * time.Hour), CreatedAt: now, UpdatedAt: now},
		} {
			if err := tx.EnqueueJob(context.Background(), j); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), jellyfin.WebhookEvent{
		Payload:    jellyfin.WebhookPayload{ItemID: "recovery", ItemType: "Movie", LastPlayedAt: now, NotificationType: "PlaybackStart", EventID: "evt-recovery"},
		Raw:        map[string]any{"ItemId": "recovery", "EventId": "evt-recovery"},
		ItemID:     "recovery",
		EventID:    "evt-recovery",
		EventType:  "PlaybackStart",
		DedupeKey:  "jellyfin:evt-recovery",
		OccurredAt: now,
	}); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected playback to recover flow to Active, got %s", flow.State)
	}
	if flow.HITLOutcome != "played" {
		t.Fatalf("expected HITLOutcome=played after recovery, got %q", flow.HITLOutcome)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		for _, jid := range []string{
			"job:prompt:" + itemID + ":1",
			"job:timeout:" + itemID + ":1",
		} {
			_, found, err := tx.GetJob(context.Background(), jid)
			if err != nil {
				return err
			}
			if found {
				t.Errorf("expected stale job %s to be purged after playback recovery", jid)
			}
		}
		// Eval should have been re-created by RequestEval.
		evalJob, found, err := tx.GetJob(context.Background(), "job:eval:scheduled:"+itemID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected fresh eval job after recovery")
		}
		if evalJob.Status != domain.JobStatusPending {
			t.Fatalf("expected fresh eval pending, got %s", evalJob.Status)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify jobs: %v", err)
	}
}

func TestReconcileStaleFlows(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	// Seed a pending_review flow with no Discord message — should be reconciled.
	staleItem := "target:movie:stale-pending"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:" + staleItem,
			ItemID:      staleItem,
			SubjectType: "movie",
			DisplayName: "Stale Pending Movie",
			State:       domain.FlowStatePendingReview,
			Version:     0,
			NextActionAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			CreatedAt:   now.Add(-365 * 24 * time.Hour),
			UpdatedAt:   now.Add(-365 * 24 * time.Hour),
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed stale flow: %v", err)
	}

	// Seed an active flow with past nextActionAt — should also be reconciled.
	staleActive := "target:movie:stale-active"
	err = store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:       "flow:" + staleActive,
			ItemID:       staleActive,
			SubjectType:  "movie",
			DisplayName:  "Stale Active Movie",
			State:        domain.FlowStateActive,
			Version:      0,
			NextActionAt: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			CreatedAt:    now.Add(-730 * 24 * time.Hour),
			UpdatedAt:    now.Add(-730 * 24 * time.Hour),
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed stale active flow: %v", err)
	}

	reconciled, err := svc.ReconcileStaleFlows(context.Background())
	if err != nil {
		t.Fatalf("reconcile stale flows: %v", err)
	}
	if reconciled != 2 {
		t.Fatalf("expected 2 reconciled flows, got %d", reconciled)
	}
}

func TestRequestImmediateReview(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	svc.SyncFlowManagerClock()

	itemID := "target:movie:review-now"
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:       "flow:" + itemID,
			ItemID:       itemID,
			SubjectType:  "movie",
			DisplayName:  "Review Now Movie",
			State:        domain.FlowStateActive,
			Version:      0,
			NextActionAt: now.Add(30 * 24 * time.Hour),
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			CreatedAt:    now,
			UpdatedAt:    now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	if err := svc.RequestImmediateReview(context.Background(), itemID); err != nil {
		t.Fatalf("request immediate review: %v", err)
	}

	flow := mustGetFlow(t, store, itemID)
	if flow.State != domain.FlowStatePendingReview {
		t.Fatalf("expected pending_review, got %s", flow.State)
	}
	if flow.NextActionAt != now {
		t.Fatalf("expected next_action_at to be now, got %s", flow.NextActionAt)
	}
}
