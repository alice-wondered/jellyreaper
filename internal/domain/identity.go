package domain

import (
	"errors"
	"fmt"
	"strings"
)

// MediaIdentity is what Jellyfin owns about an item: what it is and where it
// sits. It is fixed when the media row is created and never patched, so no
// later event (stale, partial, or playback-only) can unlink an episode from
// its season.
type MediaIdentity struct {
	ItemType     string
	SeriesID     string
	SeasonID     string
	SeasonNumber int
	// Placed is false until Jellyfin assigns a season number. An unplaced
	// episode ("Season Unknown") is transitional and must never be indexed,
	// or it would be frozen into a placeholder season.
	Placed bool
}

var (
	// ErrIncompleteIdentity: the row must not be created. Recoverable by
	// resolving the identity from Jellyfin and retrying (backfill does).
	ErrIncompleteIdentity = errors.New("media identity incomplete")
	// ErrIdentityImmutable: a caller tried to change identity after creation.
	// Not recoverable by retry; it is a programming error.
	ErrIdentityImmutable = errors.New("media identity is immutable")
)

func (m MediaItem) Identity() MediaIdentity {
	id := MediaIdentity{ItemType: m.ItemType, SeriesID: NormalizeID(m.SeriesID), SeasonID: NormalizeID(m.SeasonID)}
	if m.SeasonNumber != nil {
		id.SeasonNumber, id.Placed = *m.SeasonNumber, true
	}
	return id
}

// Apply writes the identity onto m; the only path that sets identity fields.
func (id MediaIdentity) Apply(m *MediaItem) {
	m.ItemType, m.SeriesID, m.SeasonID, m.SeasonNumber = id.ItemType, id.SeriesID, id.SeasonID, nil
	if id.Placed {
		n := id.SeasonNumber
		m.SeasonNumber = &n
	}
}

// IsSpecials: Jellyfin's season 0. A real season, reviewed and deleted like
// any other; distinct from an unplaced episode.
func (id MediaIdentity) IsSpecials() bool {
	return id.IsEpisode() && id.Placed && id.SeasonNumber == 0
}

func (id MediaIdentity) IsEpisode() bool {
	return strings.EqualFold(strings.TrimSpace(id.ItemType), "episode")
}

// Validate reports which fact is missing. An episode without a season cannot
// be attached to a season flow, so it would be invisible to review.
func (id MediaIdentity) Validate() error {
	switch {
	case strings.TrimSpace(id.ItemType) == "":
		return fmt.Errorf("%w: item type", ErrIncompleteIdentity)
	case id.IsEpisode() && id.SeriesID == "":
		return fmt.Errorf("%w: episode series id", ErrIncompleteIdentity)
	case id.IsEpisode() && (id.SeasonID == "" || !id.Placed):
		return fmt.Errorf("%w: episode not placed in a season yet", ErrIncompleteIdentity)
	case id.IsEpisode() && id.SeasonNumber < 0:
		return fmt.Errorf("%w: negative season number", ErrIncompleteIdentity)
	}
	return nil
}
