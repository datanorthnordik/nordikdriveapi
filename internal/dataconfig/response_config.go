package dataconfig

import (
	"encoding/json"
	"strings"
	"unicode"

	"gorm.io/datatypes"
)

func configForResponse(raw datatypes.JSON) datatypes.JSON {
	if len(raw) == 0 {
		return raw
	}

	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return raw
	}

	candidate, fromSourceFile := preferredConfigPayload(payload)
	filtered, changed, keep := filterAdditionalFieldEntries(candidate)
	if !keep {
		return raw
	}

	if !fromSourceFile && !changed {
		return raw
	}

	jsonBytes, err := json.Marshal(filtered)
	if err != nil {
		return raw
	}
	return datatypes.JSON(jsonBytes)
}

func preferredConfigPayload(payload any) (any, bool) {
	if sourceConfig, ok := findSourceFileConfig(payload); ok {
		return sourceConfig, true
	}
	return payload, false
}

func findSourceFileConfig(value any) (any, bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if normalizeConfigKey(key) != "source_file" {
				continue
			}
			sourceMap, ok := child.(map[string]any)
			if !ok {
				continue
			}
			if candidate, ok := extractSourceFileConfig(sourceMap); ok {
				return candidate, true
			}
		}
		for _, child := range v {
			if candidate, ok := findSourceFileConfig(child); ok {
				return candidate, true
			}
		}
	case []any:
		for _, item := range v {
			if candidate, ok := findSourceFileConfig(item); ok {
				return candidate, true
			}
		}
	}

	return nil, false
}

func extractSourceFileConfig(source map[string]any) (any, bool) {
	for key, value := range source {
		switch normalizeConfigKey(key) {
		case "data_config", "config":
			return value, true
		}
	}

	if _, ok := source["fields"]; ok {
		return source, true
	}
	for key := range source {
		if normalizeConfigKey(key) == "fields" {
			return source, true
		}
	}

	return nil, false
}

func filterAdditionalFieldEntries(value any) (filtered any, changed bool, keep bool) {
	switch v := value.(type) {
	case map[string]any:
		if isAdditionalFieldEntry(v) {
			return nil, true, false
		}

		out := make(map[string]any, len(v))
		changed = false
		for key, child := range v {
			filteredChild, childChanged, childKeep := filterAdditionalFieldEntries(child)
			if !childKeep {
				changed = true
				continue
			}
			if childChanged {
				changed = true
			}
			out[key] = filteredChild
		}
		if !changed {
			return value, false, true
		}
		return out, true, true

	case []any:
		out := make([]any, 0, len(v))
		changed = false
		for _, item := range v {
			filteredItem, itemChanged, itemKeep := filterAdditionalFieldEntries(item)
			if !itemKeep {
				changed = true
				continue
			}
			if itemChanged {
				changed = true
			}
			out = append(out, filteredItem)
		}
		if !changed {
			return value, false, true
		}
		return out, true, true

	default:
		return value, false, true
	}
}

func isAdditionalFieldEntry(value map[string]any) bool {
	for key, raw := range value {
		if normalizeConfigKey(key) != "additional_field" {
			continue
		}
		return parseAdditionalFieldFlag(raw)
	}
	return false
}

func parseAdditionalFieldFlag(raw any) bool {
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func normalizeConfigKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(value) + 4)

	var prev rune
	for i, r := range value {
		switch {
		case r == '-' || unicode.IsSpace(r):
			builder.WriteByte('_')
		case unicode.IsUpper(r):
			if i > 0 && prev != '_' && (unicode.IsLower(prev) || unicode.IsDigit(prev)) {
				builder.WriteByte('_')
			}
			builder.WriteRune(unicode.ToLower(r))
		default:
			builder.WriteRune(unicode.ToLower(r))
		}
		prev = r
	}

	normalized := builder.String()
	for strings.Contains(normalized, "__") {
		normalized = strings.ReplaceAll(normalized, "__", "_")
	}
	return strings.Trim(normalized, "_")
}
