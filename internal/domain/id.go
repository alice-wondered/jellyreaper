package domain

import (
	"strings"

	"github.com/google/uuid"
)

func NormalizeID(raw string) string {
	id := strings.TrimSpace(raw)
	if id == "" {
		return ""
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return id
	}
	return strings.ToLower(parsed.String())
}

func AlternateIDForms(raw string) []string {
	normalized := NormalizeID(raw)
	if normalized == "" {
		return nil
	}
	forms := []string{normalized}
	if _, err := uuid.Parse(normalized); err != nil {
		return forms
	}
	nodash := strings.ReplaceAll(normalized, "-", "")
	if nodash != normalized {
		forms = append(forms, nodash)
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
