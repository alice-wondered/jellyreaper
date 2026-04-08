package discord

import "testing"

func TestNormalizeHTTPURL_StrictValidation(t *testing.T) {
	if got := normalizeHTTPURL("https://example.com/image.jpg"); got == "" {
		t.Fatal("expected valid https URL")
	}
	if got := normalizeHTTPURL("http://example.com/a\n.jpg"); got != "" {
		t.Fatalf("expected URL with control character to be rejected, got %q", got)
	}
	if got := normalizeHTTPURL("javascript:alert(1)"); got != "" {
		t.Fatalf("expected non-http scheme to be rejected, got %q", got)
	}
}

func TestShouldRetryHITLPromptWithoutEmbed(t *testing.T) {
	err := testErr("HTTP 400 Bad Request, {\"message\":\"Invalid Form Body\",\"errors\":{\"embeds\":{\"0\":{\"image\":{\"url\":{\"_errors\":[{\"code\":\"URL_TYPE_INVALID_URL\"}]}}}}}}")
	if !shouldRetryHITLPromptWithoutEmbed(err) {
		t.Fatal("expected embed URL error to trigger retry")
	}
	if shouldRetryHITLPromptWithoutEmbed(testErr("HTTP 403 Forbidden")) {
		t.Fatal("expected non-embed error not to trigger retry")
	}
}

func TestFinalizeEmojiForOutcome(t *testing.T) {
	if got := finalizeEmojiForOutcome("Resolved: DELETED for X"); got != "🗑" {
		t.Fatalf("unexpected delete emoji: %q", got)
	}
	if got := finalizeEmojiForOutcome("Resolved: DELAYED for X"); got != "⏸" {
		t.Fatalf("unexpected delay emoji: %q", got)
	}
}

type testErr string

func (e testErr) Error() string { return string(e) }
