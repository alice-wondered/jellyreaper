package domain

import (
	"errors"
	"testing"
)

func TestMediaIdentityValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   MediaIdentity
		ok   bool
	}{
		{"movie", MediaIdentity{ItemType: "Movie"}, true},
		{"episode season 1", MediaIdentity{ItemType: "Episode", SeriesID: "s", SeasonID: "se", SeasonNumber: 1, Placed: true}, true},
		{"specials (season 0)", MediaIdentity{ItemType: "Episode", SeriesID: "s", SeasonID: "sp", SeasonNumber: 0, Placed: true}, true},
		{"season unknown: id but no number", MediaIdentity{ItemType: "Episode", SeriesID: "s", SeasonID: "virtual"}, false},
		{"no type", MediaIdentity{}, false},
		{"episode no season", MediaIdentity{ItemType: "Episode", SeriesID: "s"}, false},
		{"episode no series", MediaIdentity{ItemType: "episode", SeasonID: "se", Placed: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.id.Validate()
			if tc.ok != (err == nil) {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
			if err != nil && !errors.Is(err, ErrIncompleteIdentity) {
				t.Fatalf("error %v must wrap ErrIncompleteIdentity", err)
			}
		})
	}
}

func TestSpecialsIsNotUnplaced(t *testing.T) {
	specials := MediaIdentity{ItemType: "Episode", SeriesID: "s", SeasonID: "sp", Placed: true}
	unplaced := MediaIdentity{ItemType: "Episode", SeriesID: "s", SeasonID: "sp"}
	if !specials.IsSpecials() || unplaced.IsSpecials() {
		t.Fatalf("specials=%v unplaced=%v", specials.IsSpecials(), unplaced.IsSpecials())
	}
	var m MediaItem
	specials.Apply(&m)
	if m.SeasonNumber == nil || *m.SeasonNumber != 0 || m.Identity() != specials {
		t.Fatalf("specials must round-trip through MediaItem: %+v", m.Identity())
	}
	unplaced.Apply(&m)
	if m.SeasonNumber != nil {
		t.Fatal("an unplaced identity must not store season 0")
	}
}
