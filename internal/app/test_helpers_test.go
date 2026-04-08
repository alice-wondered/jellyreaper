package app

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/discord"
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

func seedFlowForInteraction(t *testing.T, store *bboltrepo.Store, itemID string, now time.Time) {
	t.Helper()
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:         "flow:" + itemID,
			ItemID:         itemID,
			SubjectType:    inferSubjectType(itemID),
			DisplayName:    itemID,
			State:          domain.FlowStatePendingReview,
			Version:        0,
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			CreatedAt:      now,
			UpdatedAt:      now,
		}, 0)
	})
	if err != nil {
		t.Fatalf("seed flow %s: %v", itemID, err)
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
