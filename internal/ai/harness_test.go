package ai

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/app"
	"jellyreaper/internal/domain"
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

func seedFlow(t *testing.T, store *bboltrepo.Store, itemID string, title string, state domain.FlowState) {
	t.Helper()
	now := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC)
	subjectType := "movie"
	parts := strings.SplitN(itemID, ":", 3)
	if len(parts) == 3 && parts[0] == "target" && strings.TrimSpace(parts[1]) != "" {
		subjectType = strings.TrimSpace(parts[1])
	}
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow := domain.Flow{
			FlowID:         "flow:" + itemID,
			ItemID:         itemID,
			SubjectType:    subjectType,
			DisplayName:    title,
			State:          state,
			Version:        0,
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 90, HITLTimeoutHrs: 24, TimeoutAction: "delete"},
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if state == domain.FlowStateActive {
			flow.NextActionAt = now.Add(-time.Hour)
		}
		return tx.UpsertFlowCAS(context.Background(), flow, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}
}

func seedMedia(t *testing.T, store *bboltrepo.Store, itemID string, seasonID string, seriesID string, seriesName string) {
	t.Helper()
	now := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC)
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:     itemID,
			ItemType:   "Episode",
			SeasonID:   seasonID,
			SeriesID:   seriesID,
			SeriesName: seriesName,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	})
	if err != nil {
		t.Fatalf("seed media: %v", err)
	}
}

func mustGetFlow(t *testing.T, store *bboltrepo.Store, itemID string) domain.Flow {
	t.Helper()
	var flow domain.Flow
	var found bool
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		f, ok, err := tx.GetFlow(context.Background(), itemID)
		if err != nil {
			return err
		}
		flow, found = f, ok
		return nil
	})
	if err != nil {
		t.Fatalf("get flow: %v", err)
	}
	if !found {
		t.Fatalf("flow %s not found", itemID)
	}
	return flow
}

func TestQueryTargetState_SelectCandidateByNumber(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "m-1", "Alien", domain.FlowStateActive)
	seedFlow(t, store, "m-2", "Alien 3", domain.FlowStateArchived)

	h := NewHarness(store, "", "")
	threadID := "thread-1"

	out, _, err := h.queryTargetState(context.Background(), threadID, "alien")
	if err != nil {
		t.Fatalf("query target state: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"needs_selection\"") {
		t.Fatalf("expected disambiguation response, got: %s", out)
	}

	out, _, err = h.handleFollowUp(context.Background(), threadID, "2")
	if err != nil {
		t.Fatalf("follow-up select: %v", err)
	}
	if !strings.Contains(out, "\"title\":\"Alien 3\"") {
		t.Fatalf("expected selected flow summary, got: %s", out)
	}
	if !strings.Contains(out, "\"archived\":true") {
		t.Fatalf("expected archived state in summary, got: %s", out)
	}
}

func TestArchiveConfirmFlow_YesArchivesSelection(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "m-7", "Blade Runner", domain.FlowStateActive)

	h := NewHarness(store, "", "")
	threadID := "thread-archive"

	out, _, err := h.setArchiveState(context.Background(), threadID, "blade", true)
	if err != nil {
		t.Fatalf("set archive state: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"needs_confirmation\"") {
		t.Fatalf("expected confirmation prompt, got: %s", out)
	}

	out, _, err = h.handleFollowUp(context.Background(), threadID, "yes")
	if err != nil {
		t.Fatalf("archive confirm: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"done\"") || !strings.Contains(out, "\"title\":\"Blade Runner\"") {
		t.Fatalf("expected archived response, got: %s", out)
	}

	flow := mustGetFlow(t, store, "m-7")
	if flow.State != domain.FlowStateArchived {
		t.Fatalf("expected archived state, got %s", flow.State)
	}
}

func TestArchiveConfirmFlow_NoCancels(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "m-8", "Arrival", domain.FlowStateActive)

	h := NewHarness(store, "", "")
	threadID := "thread-cancel"

	_, _, err := h.setArchiveState(context.Background(), threadID, "arrival", true)
	if err != nil {
		t.Fatalf("set archive state: %v", err)
	}

	out, _, err := h.handleFollowUp(context.Background(), threadID, "no")
	if err != nil {
		t.Fatalf("archive cancel: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"cancelled\"") {
		t.Fatalf("expected cancel response, got: %s", out)
	}

	flow := mustGetFlow(t, store, "m-8")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("expected active state after cancel, got %s", flow.State)
	}
}

func TestScheduleDelete_ConfirmQueuesDeleteJob(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "m-9", "Interstellar", domain.FlowStateActive)

	h := NewHarness(store, "", "")
	h.SetDecisionService(app.NewService(store, nil, nil))
	threadID := "thread-delete"

	out, _, err := h.setDeleteState(context.Background(), threadID, "interstellar")
	if err != nil {
		t.Fatalf("set delete state: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"needs_confirmation\"") {
		t.Fatalf("expected confirmation payload, got: %s", out)
	}

	out, _, err = h.handleFollowUp(context.Background(), threadID, "yes")
	if err != nil {
		t.Fatalf("confirm schedule delete: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"done\"") || !strings.Contains(out, "scheduled_delete") {
		t.Fatalf("expected done scheduled delete payload, got: %s", out)
	}

	flow := mustGetFlow(t, store, "m-9")
	if flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("expected delete_queued state, got %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), time.Now().UTC().Add(time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	foundDelete := false
	for _, job := range jobs {
		if job.ItemID == "m-9" && job.Kind == domain.JobKindExecuteDelete {
			foundDelete = true
			break
		}
	}
	if !foundDelete {
		t.Fatal("expected execute_delete job to be queued")
	}
}

func TestHistoryRingBufferKeepsRecentWindow(t *testing.T) {
	store := newTestStore(t)
	h := NewHarness(store, "", "")
	threadID := "thread-ring"

	for i := 1; i <= 25; i++ {
		h.appendHistory(threadID, fmt.Sprintf("line-%d", i))
	}

	history := h.getHistory(threadID)
	if len(history) != 8 {
		t.Fatalf("expected 8 recent entries in prompt history, got %d", len(history))
	}
	if history[0] != "line-18" || history[7] != "line-25" {
		t.Fatalf("unexpected history window: first=%s last=%s", history[0], history[7])
	}
}

func TestHistoryRestoresFromDiscordOnMiss(t *testing.T) {
	store := newTestStore(t)
	h := NewHarness(store, "", "")
	threadID := "thread-restore"
	called := 0

	h.SetHistoryRestorer(func(ctx context.Context, key string, limit int) ([]string, error) {
		called++
		if key != threadID {
			t.Fatalf("unexpected thread key: %s", key)
		}
		if limit != 20 {
			t.Fatalf("unexpected restore limit: %d", limit)
		}
		return []string{"alice: hello", "assistant: hi there"}, nil
	})

	h.ensureHistoryLoaded(context.Background(), threadID)
	h.ensureHistoryLoaded(context.Background(), threadID)

	if called != 1 {
		t.Fatalf("expected restorer to be called once, got %d", called)
	}
	history := h.getHistory(threadID)
	if len(history) != 2 {
		t.Fatalf("expected restored history length 2, got %d", len(history))
	}
	if history[0] != "alice: hello" || history[1] != "assistant: hi there" {
		t.Fatalf("unexpected restored history: %#v", history)
	}
}

func TestThreadContextCapEvictsLeastRecentlyUsed(t *testing.T) {
	store := newTestStore(t)
	h := NewHarness(store, "", "")
	h.SetMaxThreadContexts(2)

	h.appendHistory("thread-1", "alice: one")
	h.appendHistory("thread-2", "alice: two")

	_ = h.getHistory("thread-1")
	h.appendHistory("thread-3", "alice: three")

	if _, ok := h.history["thread-1"]; !ok {
		t.Fatal("expected thread-1 to remain after LRU refresh")
	}
	if _, ok := h.history["thread-3"]; !ok {
		t.Fatal("expected thread-3 to be present")
	}
	if _, ok := h.history["thread-2"]; ok {
		t.Fatal("expected thread-2 to be evicted as LRU")
	}
}

func TestAliasMemorySupportsTitleLabelFollowUp(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "m-21", "Dune", domain.FlowStateActive)
	seedFlow(t, store, "m-22", "Dune Part Two", domain.FlowStateActive)

	h := NewHarness(store, "", "")
	threadID := "thread-alias"

	out, _, err := h.setArchiveState(context.Background(), threadID, "dune", false)
	if err != nil {
		t.Fatalf("set archive state: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"needs_selection\"") {
		t.Fatalf("expected needs_selection, got %s", out)
	}

	out, _, err = h.handleFollowUp(context.Background(), threadID, "title b")
	if err != nil {
		t.Fatalf("title label follow-up: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"needs_confirmation\"") {
		t.Fatalf("expected needs_confirmation after alias pick, got %s", out)
	}
	if !strings.Contains(out, "\"title\":\"Dune Part Two\"") {
		t.Fatalf("expected Dune Part Two confirmation, got %s", out)
	}
}

func TestRememberAliasToolStoresCustomPhrase(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "m-41", "The Matrix", domain.FlowStateActive)

	h := NewHarness(store, "", "")
	threadID := "thread-remember-alias"

	_, _, err := h.queryTargetState(context.Background(), threadID, "matrix")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}

	out, _, err := h.rememberAlias(context.Background(), threadID, "neo movie", "")
	if err != nil {
		t.Fatalf("remember alias: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"ok\"") {
		t.Fatalf("expected ok remember alias result, got %s", out)
	}

	resolved := h.resolveAlias(threadID, "neo movie")
	if resolved != "m-41" {
		t.Fatalf("expected alias to resolve to m-41, got %s", resolved)
	}
}

func TestScheduleDeleteProjection_SeriesSchedulesAllSeasonTargets(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "target:season:s-1", "Season 1 of The Office", domain.FlowStateActive)
	seedFlow(t, store, "target:season:s-2", "Season 2 of The Office", domain.FlowStateActive)
	seedMedia(t, store, "ep-1", "s-1", "series-1", "The Office")
	seedMedia(t, store, "ep-2", "s-2", "series-1", "The Office")

	h := NewHarness(store, "", "")
	h.SetDecisionService(app.NewService(store, nil, nil))
	threadID := "thread-series-delete"

	out, _, err := h.setDeleteProjectionState(context.Background(), threadID, "the office", "series")
	if err != nil {
		t.Fatalf("set delete projection: %v", err)
	}
	if !strings.Contains(out, "\"status\":\"needs_confirmation\"") {
		t.Fatalf("expected confirmation status, got: %s", out)
	}

	out, _, err = h.handleFollowUp(context.Background(), threadID, "yes")
	if err != nil {
		t.Fatalf("confirm delete projection: %v", err)
	}
	if !strings.Contains(out, "scheduled_delete_projection") {
		t.Fatalf("expected projection delete done output, got: %s", out)
	}

	flow1 := mustGetFlow(t, store, "target:season:s-1")
	flow2 := mustGetFlow(t, store, "target:season:s-2")
	if flow1.State != domain.FlowStateDeleteQueued || flow2.State != domain.FlowStateDeleteQueued {
		t.Fatalf("expected both season flows delete_queued, got %s and %s", flow1.State, flow2.State)
	}
}

func TestFuzzySearchTargets_FiltersBySubjectType(t *testing.T) {
	store := newTestStore(t)
	seedFlow(t, store, "target:movie:m-1", "Dune", domain.FlowStateActive)
	seedFlow(t, store, "target:season:s-9", "Season 1 of Dune Series", domain.FlowStateActive)

	h := NewHarness(store, "", "")
	out, _, err := h.fuzzySearchTargets(context.Background(), "dune", "movie", 10)
	if err != nil {
		t.Fatalf("fuzzy search targets: %v", err)
	}
	if !strings.Contains(out, "\"count\":1") {
		t.Fatalf("expected one movie result, got: %s", out)
	}
	if !strings.Contains(out, "\"item_id\":\"target:movie:m-1\"") {
		t.Fatalf("expected movie target in search results, got: %s", out)
	}
}
