package app

import (
	"context"
	"fmt"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

const identityReconciledAtKey = "identity.reconciled_at"

type IdentityReconcileResult struct {
	Moved     int // identity changed: legacy blanks filled or upstream renumbering
	Unplaced  int // rows still unlinked because Jellyfin has not placed them yet
	Recounted int // season flows whose EpisodeCount was recomputed
	Skipped   bool
}

// ReconcileMediaIdentities syncs every episode row's identity to Jellyfin at
// most once per minInterval. Jellyfin is the authority: legacy rows get their
// season, episodes renumbered upstream (TVDB/TMDB reorders) move, and every
// season whose membership changed gets its EpisodeCount recounted.
func (s *Service) ReconcileMediaIdentities(ctx context.Context, minInterval time.Duration) (IdentityReconcileResult, error) {
	var res IdentityReconcileResult
	if s.jellyfinClient == nil {
		res.Skipped = true
		return res, nil
	}
	now := s.now().UTC()
	var last time.Time
	if err := s.repository.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
		raw, ok, err := tx.GetMeta(ctx, identityReconciledAtKey)
		if ok {
			last, _ = time.Parse(time.RFC3339Nano, raw)
		}
		return err
	}); err != nil {
		return res, err
	}
	if !last.IsZero() && now.Sub(last) < minInterval {
		res.Skipped = true
		return res, nil
	}

	identities, err := s.jellyfinClient.ListEpisodeIdentities(ctx, 500)
	if err != nil {
		return res, fmt.Errorf("list jellyfin episode identities: %w", err)
	}
	err = s.repository.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
		res = IdentityReconcileResult{}
		touched := map[string]struct{}{}
		for itemID, jf := range identities {
			media, found, err := tx.GetMedia(ctx, itemID)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if jf.Identity.Validate() != nil {
				if media.Identity().Validate() != nil {
					res.Unplaced++
				}
				continue
			}
			before, changed, err := tx.ReconcileMediaIdentity(ctx, itemID, jf.Identity, jf.SeasonName)
			if err != nil {
				return err
			}
			if changed && before != jf.Identity {
				res.Moved++
				touched[before.SeasonID] = struct{}{}
				touched[jf.Identity.SeasonID] = struct{}{}
			}
		}
		for seasonID := range touched {
			recounted, err := recountSeasonEpisodes(ctx, tx, seasonID, now)
			if err != nil {
				return err
			}
			if recounted {
				res.Recounted++
			}
		}
		return tx.SetMeta(ctx, identityReconciledAtKey, now.Format(time.RFC3339Nano))
	})
	if err == nil {
		s.logger.Info("media identity reconcile complete", "lex", "CATALOG-INDEX", "moved", res.Moved, "unplaced", res.Unplaced, "seasons_recounted", res.Recounted, "jellyfin_episodes", len(identities))
	}
	return res, err
}

// recountSeasonEpisodes sets a season flow's EpisodeCount from its linked
// media. The version is not bumped: the count is data, not a state
// transition, and a bump would strand the flow's queued eval/timeout jobs.
func recountSeasonEpisodes(ctx context.Context, tx repo.TxRepository, seasonID string, now time.Time) (bool, error) {
	if seasonID == "" {
		return false, nil
	}
	flow, found, err := tx.GetFlow(ctx, "target:season:"+domain.NormalizeID(seasonID))
	if err != nil || !found {
		return false, err
	}
	children, err := tx.ListMediaBySubject(ctx, "season", seasonID)
	if err != nil {
		return false, err
	}
	if flow.EpisodeCount == len(children) {
		return false, nil
	}
	flow.EpisodeCount = len(children)
	flow.UpdatedAt = now
	return true, tx.UpsertFlowCAS(ctx, flow, flow.Version)
}
