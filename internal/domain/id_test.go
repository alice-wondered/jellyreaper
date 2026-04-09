package domain

import "testing"

func TestNormalizeID(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "trimmed uuid", in: "  BDA444ED-C4E7-4BBB-6677-3CBE94938D10  ", want: "bda444edc4e74bbb66773cbe94938d10"},
		{name: "nodash uuid stays nodash", in: "BDA444EDC4E74BBB66773CBE94938D10", want: "bda444edc4e74bbb66773cbe94938d10"},
		{name: "dashed hex guid", in: "F8F13C13-EAE5-0047-57EB-3308105503B9", want: "f8f13c13eae5004757eb3308105503b9"},
		{name: "nodash hex guid", in: "F8F13C13EAE5004757EB3308105503B9", want: "f8f13c13eae5004757eb3308105503b9"},
		{name: "non uuid preserved", in: "series-provider-1", want: "series-provider-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeID(tc.in)
			if got != tc.want {
				t.Fatalf("NormalizeID(%q)=%q want=%q", tc.in, got, tc.want)
			}
		})
	}
}

func TestAlternateIDForms(t *testing.T) {
	forms := AlternateIDForms("BDA444EDC4E74BBB66773CBE94938D10")
	if len(forms) != 2 {
		t.Fatalf("expected two alternate forms, got %d (%v)", len(forms), forms)
	}
	if forms[0] != "bda444edc4e74bbb66773cbe94938d10" {
		t.Fatalf("unexpected normalized form: %q", forms[0])
	}
	if forms[1] != "bda444ed-c4e7-4bbb-6677-3cbe94938d10" {
		t.Fatalf("unexpected dashed form: %q", forms[1])
	}

	plain := AlternateIDForms("series-provider-1")
	if len(plain) != 1 || plain[0] != "series-provider-1" {
		t.Fatalf("expected single non-uuid form, got %v", plain)
	}
}

func TestNormalizeProviderIDs(t *testing.T) {
	got := NormalizeProviderIDs(map[string]string{
		" TvDb ": " 73244 ",
		"Imdb":   " tt0386676 ",
		"":       "ignored",
		"tmdb":   "",
	})
	if got["tvdb"] != "73244" {
		t.Fatalf("expected normalized tvdb, got %q", got["tvdb"])
	}
	if got["imdb"] != "tt0386676" {
		t.Fatalf("expected normalized imdb, got %q", got["imdb"])
	}
	if _, ok := got["tmdb"]; ok {
		t.Fatal("expected empty tmdb to be dropped")
	}
}

func TestMergeProviderIDs(t *testing.T) {
	merged := MergeProviderIDs(
		map[string]string{"tvdb": "111", "imdb": "tt-old"},
		map[string]string{"tvdb": "222", "tmdb": "603", "imdb": "tt-new"},
	)
	if merged["tvdb"] != "222" {
		t.Fatalf("expected incoming tvdb to override, got %q", merged["tvdb"])
	}
	if merged["imdb"] != "tt-new" {
		t.Fatalf("expected incoming imdb to override, got %q", merged["imdb"])
	}
	if merged["tmdb"] != "603" {
		t.Fatalf("expected incoming tmdb to be present, got %q", merged["tmdb"])
	}
}
