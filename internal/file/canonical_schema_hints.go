package file

import (
	"encoding/json"
	"strings"
	"unicode"

	"nordik-drive-api/internal/dataconfig"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type normalizationSchemaHints struct {
	ConceptFields map[string][]string
}

func loadNormalizationSchemaHints(db *gorm.DB, fileID uint) (*normalizationSchemaHints, error) {
	if db == nil || fileID == 0 {
		return nil, nil
	}
	if !db.Migrator().HasTable(&dataconfig.DataConfig{}) {
		return nil, nil
	}

	var cfg dataconfig.DataConfig
	if err := db.
		Where("file_id = ? AND is_active = ?", fileID, true).
		Order("updated_at DESC").
		Order("id DESC").
		Take(&cfg).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	hints, err := parseNormalizationSchemaHints(cfg.Config)
	if err != nil {
		return nil, nil
	}
	return hints, nil
}

func parseNormalizationSchemaHints(raw datatypes.JSON) (*normalizationSchemaHints, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, nil
	}

	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}

	hints := &normalizationSchemaHints{
		ConceptFields: make(map[string][]string),
	}

	collectSchemaHintsFromPayload(hints, payload)
	if preferred, ok := findSourceFileDataConfigPayload(payload); ok {
		collectSchemaHintsFromPayload(hints, preferred)
	}

	if len(hints.ConceptFields) == 0 {
		return nil, nil
	}
	return hints, nil
}

func collectSchemaHintsFromPayload(hints *normalizationSchemaHints, value any) {
	switch v := value.(type) {
	case map[string]any:
		collectSchemaHintConcepts(hints, v)
		for key, child := range v {
			normalizedKey := normalizeSchemaKey(key)
			switch normalizedKey {
			case "columns", "fields":
				if items, ok := child.([]any); ok {
					collectSchemaHintsFromColumns(hints, items)
					continue
				}
			case "chat_schema", "chat_config", "llm_schema", "canonical_fields", "canonical_map":
				collectSchemaHintConceptValue(hints, child)
				continue
			}
			collectSchemaHintsFromPayload(hints, child)
		}
	case []any:
		for _, item := range v {
			collectSchemaHintsFromPayload(hints, item)
		}
	}
}

func collectSchemaHintConceptValue(hints *normalizationSchemaHints, value any) {
	switch v := value.(type) {
	case map[string]any:
		collectSchemaHintConcepts(hints, v)
		for key, child := range v {
			switch normalizeSchemaKey(key) {
			case "concepts", "field_map", "fields_map", "columns_map":
				collectSchemaHintConceptValue(hints, child)
			}
		}
	case []any:
		collectSchemaHintsFromColumns(hints, v)
	}
}

func collectSchemaHintConcepts(hints *normalizationSchemaHints, value map[string]any) {
	for key, rawValue := range value {
		concept := normalizeSchemaKey(key)
		if !isSupportedCanonicalConcept(concept) {
			continue
		}
		for _, fieldName := range parseSchemaFieldNames(rawValue) {
			addSchemaHintField(hints, concept, fieldName)
		}
	}
}

func collectSchemaHintsFromColumns(hints *normalizationSchemaHints, items []any) {
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		concept := extractColumnConcept(entry)
		if !isSupportedCanonicalConcept(concept) {
			continue
		}

		fieldName := firstNonEmpty(
			stringValue(entry["label"]),
			stringValue(entry["key"]),
			stringValue(entry["name"]),
			stringValue(entry["field"]),
		)
		if fieldName == "" {
			continue
		}
		addSchemaHintField(hints, concept, fieldName)
	}
}

func extractColumnConcept(entry map[string]any) string {
	for _, key := range []string{"chat_concept", "concept", "canonical_field", "semantic_role", "semantic_type"} {
		concept := normalizeSchemaKey(stringValue(entry[key]))
		if isSupportedCanonicalConcept(concept) {
			return concept
		}
	}
	return ""
}

func parseSchemaFieldNames(value any) []string {
	switch v := value.(type) {
	case string:
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if fieldName := stringValue(item); fieldName != "" {
				out = append(out, fieldName)
			}
		}
		return out
	default:
		return nil
	}
}

func addSchemaHintField(hints *normalizationSchemaHints, concept string, fieldName string) {
	if hints == nil {
		return
	}
	concept = normalizeSchemaKey(concept)
	fieldName = strings.TrimSpace(fieldName)
	if concept == "" || fieldName == "" {
		return
	}
	fields := hints.ConceptFields[concept]
	for _, existing := range fields {
		if strings.EqualFold(existing, fieldName) {
			return
		}
	}
	hints.ConceptFields[concept] = append(fields, fieldName)
}

func hintedConceptFields(hints *normalizationSchemaHints, concept string) []string {
	if hints == nil {
		return nil
	}
	fields := hints.ConceptFields[normalizeSchemaKey(concept)]
	if len(fields) == 0 {
		return nil
	}
	return append([]string(nil), fields...)
}

func findSourceFileDataConfigPayload(value any) (any, bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if normalizeSchemaKey(key) != "source_file" {
				continue
			}
			sourceMap, ok := child.(map[string]any)
			if !ok {
				continue
			}
			for childKey, sourceChild := range sourceMap {
				if normalizeSchemaKey(childKey) == "data_config" || normalizeSchemaKey(childKey) == "config" {
					return sourceChild, true
				}
			}
		}
		for _, child := range v {
			if candidate, ok := findSourceFileDataConfigPayload(child); ok {
				return candidate, true
			}
		}
	case []any:
		for _, item := range v {
			if candidate, ok := findSourceFileDataConfigPayload(item); ok {
				return candidate, true
			}
		}
	}
	return nil, false
}

func normalizeSchemaKey(value string) string {
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

func isSupportedCanonicalConcept(concept string) bool {
	switch normalizeSchemaKey(concept) {
	case "display_name",
		"first_name",
		"middle_names",
		"last_name",
		"indigenous_name",
		"identifier",
		"community",
		"school",
		"deceased_status",
		"parents_names",
		"mapping_location",
		"lat",
		"lng",
		"birth_date",
		"admitted_date",
		"discharged_date",
		"notes",
		"additional_information",
		"death_details",
		"photos":
		return true
	default:
		return false
	}
}

func stringValue(value any) string {
	return stringifyRowValue(value)
}
