package discord

import (
	"testing"

	"github.com/bwmarrin/discordgo"
)

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

func TestIsThreadChannel(t *testing.T) {
	if isThreadChannel(nil) {
		t.Fatal("nil should not be a thread")
	}
	ch := &discordgo.Channel{Type: discordgo.ChannelTypeGuildPublicThread}
	if !isThreadChannel(ch) {
		t.Fatal("public thread should be detected")
	}
	ch.Type = discordgo.ChannelTypeGuildPrivateThread
	if !isThreadChannel(ch) {
		t.Fatal("private thread should be detected")
	}
	ch.Type = discordgo.ChannelTypeGuildText
	if isThreadChannel(ch) {
		t.Fatal("text channel should not be detected as thread")
	}
}

func TestTrackAndDetectActiveThread(t *testing.T) {
	svc := &Service{}
	if svc.isActiveThread("thread-1") {
		t.Fatal("empty service should not have active threads")
	}
	svc.TrackThread("thread-1")
	if !svc.isActiveThread("thread-1") {
		t.Fatal("tracked thread should be detected as active")
	}
	if svc.isActiveThread("thread-other") {
		t.Fatal("untracked thread should not be active")
	}
}

type testErr string

func (e testErr) Error() string { return string(e) }
