package util

import "strings"

func ParseCommaSeparatedCommunities(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	raw := values[0] // DON'T TrimSpace the whole thing if you want to preserve leading/trailing quotes in names
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p) // remove spaces around comma, keeps quotes if they exist
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
