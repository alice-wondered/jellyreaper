package jellyfin

import "testing"

func TestBuildWebhookEventDerivesPrimaryImageURL(t *testing.T) {
	event := BuildWebhookEvent(WebhookPayload{
		ItemID:          "item-1",
		ServerURL:       "http://localhost:8096",
		PrimaryImageTag: "abc123",
	}, map[string]any{"ItemId": "item-1"})

	if event.Payload.PrimaryImageURL == "" {
		t.Fatal("expected derived primary image url")
	}
	if want := "http://localhost:8096/Items/item-1/Images/Primary?tag=abc123"; event.Payload.PrimaryImageURL != want {
		t.Fatalf("unexpected derived url: got=%s want=%s", event.Payload.PrimaryImageURL, want)
	}
}
