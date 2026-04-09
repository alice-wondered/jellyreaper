package domain

import (
	"regexp"
	"strings"
)

var dashedHexIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var nodashHexIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{32}$`)

func NormalizeID(raw string) string {
	id := strings.TrimSpace(raw)
	if id == "" {
		return ""
	}
	lower := strings.ToLower(id)
	if dashedHexIDPattern.MatchString(lower) {
		return strings.ReplaceAll(lower, "-", "")
	}
	if nodashHexIDPattern.MatchString(lower) {
		return lower
	}
	return id
}

func AlternateIDForms(raw string) []string {
	normalized := NormalizeID(raw)
	if normalized == "" {
		return nil
	}
	forms := []string{normalized}
	if !nodashHexIDPattern.MatchString(normalized) {
		return forms
	}
	dashed := normalized[:8] + "-" + normalized[8:12] + "-" + normalized[12:16] + "-" + normalized[16:20] + "-" + normalized[20:]
	if dashed != normalized {
		forms = append(forms, dashed)
	}
	return forms
}

func NormalizeProviderIDs(raw map[string]string) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		key := strings.ToLower(strings.TrimSpace(k))
		val := strings.TrimSpace(v)
		if key == "" || val == "" {
			continue
		}
		out[key] = val
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func MergeProviderIDs(existing map[string]string, incoming map[string]string) map[string]string {
	merged := NormalizeProviderIDs(existing)
	next := NormalizeProviderIDs(incoming)
	if len(merged) == 0 && len(next) == 0 {
		return nil
	}
	if merged == nil {
		merged = map[string]string{}
	}
	for k, v := range next {
		if strings.TrimSpace(v) != "" {
			merged[k] = v
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}
