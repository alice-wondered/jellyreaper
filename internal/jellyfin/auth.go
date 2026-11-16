package jellyfin

import "net/http"

// SetAuth sets the only API-key credential Jellyfin accepts once
// EnableLegacyAuthorization=false (the default from 12.x): X-Emby-Token,
// X-MediaBrowser-Token and ?api_key= all return 401.
func SetAuth(h http.Header, apiKey string) {
	h.Set("Authorization", `MediaBrowser Token="`+apiKey+`"`)
}
