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
	nodash := strings.ReplaceAll(normalized, "-", "")
	if nodash != normalized {
		forms = append(forms, nodash)
	}
	return forms
}
