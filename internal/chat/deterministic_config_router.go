package chat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"nordik-drive-api/internal/dataconfig"
)

type configuredDeterministicField struct {
	Key         string
	Label       string
	Aliases     []string
	SourceNames []string
}

func (cs *ChatService) tryConfiguredDistinctValuesRoute(fileID uint, rows []cachedStructuredChatRow, question string, communities []string) (deterministicChatRoute, bool) {
	normalizedQuestion := normalizeChatSearchValue(question)
	if !looksLikeDeterministicDistinctValueListQuestion(normalizedQuestion) {
		return deterministicChatRoute{}, false
	}

	fields := cs.loadConfiguredDeterministicFields(fileID)
	field, ok := matchConfiguredDeterministicField(fields, normalizedQuestion)
	if !ok {
		return deterministicChatRoute{}, false
	}

	ctx := buildDeterministicQuestionContext(rows, question)
	if hasUnsupportedDistinctValueListTokens(ctx.Profile.Tokens, ctx.ConsumedTokens, configuredDeterministicFieldQuestionAliases(field)) {
		return deterministicChatRoute{}, false
	}
	selectedIndexes := applyDeterministicFilters(rows, communities, ctx.Filters)
	values := configuredDistinctFieldValues(rows, selectedIndexes, field)
	displayLabel := firstNonEmptyString(field.Label, strings.ReplaceAll(field.Key, "_", " "), "configured field")

	answer := fmt.Sprintf("I couldn't find any values for %s in the matching data.", displayLabel)
	if len(values) == 1 {
		answer = fmt.Sprintf("The value for %s in the data is %s.", displayLabel, values[0])
	} else if len(values) > 1 {
		answer = fmt.Sprintf("I found %d distinct values for %s: %s.", len(values), displayLabel, joinNaturalStrings(values))
	}

	return deterministicChatRoute{
		QueryType:     "distinct_values",
		RetrievalMode: "deterministic_config_distinct_values",
		Answer:        answer,
		RowsSelected:  len(selectedIndexes),
	}, true
}

func (cs *ChatService) loadConfiguredDeterministicFields(fileID uint) []configuredDeterministicField {
	if cs == nil || cs.DB == nil || fileID == 0 {
		return nil
	}

	var cfg dataconfig.DataConfig
	if err := cs.DB.
		Select("config").
		Where("file_id = ? AND is_active = ?", int64(fileID), true).
		Order("updated_at DESC").
		Order("id DESC").
		Take(&cfg).Error; err != nil {
		return nil
	}

	var payload any
	if err := json.Unmarshal(cfg.Config, &payload); err != nil {
		return nil
	}

	fields := make([]configuredDeterministicField, 0, 16)
	collectConfiguredDeterministicFields(payload, &fields)
	return uniqueConfiguredDeterministicFields(fields)
}

func collectConfiguredDeterministicFields(value any, out *[]configuredDeterministicField) {
	switch current := value.(type) {
	case map[string]any:
		if field, ok := configuredDeterministicFieldFromMap(current); ok {
			*out = append(*out, field)
		}
		for _, child := range current {
			collectConfiguredDeterministicFields(child, out)
		}
	case []any:
		for _, child := range current {
			collectConfiguredDeterministicFields(child, out)
		}
	}
}

func configuredDeterministicFieldFromMap(value map[string]any) (configuredDeterministicField, bool) {
	key := configuredStringValue(value, "key", "field_key", "fieldKey")
	label := configuredStringValue(value, "label", "title", "display_name", "displayName")
	if key == "" && label == "" {
		return configuredDeterministicField{}, false
	}

	aliases := []string{key, label}
	for _, aliasKey := range []string{
		"name", "field", "column", "header", "source", "source_key", "sourceKey", "source_field", "sourceField",
	} {
		aliases = append(aliases, configuredStringValue(value, aliasKey))
	}

	normalizedAliases := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		alias = normalizeChatSearchValue(strings.ReplaceAll(alias, "_", " "))
		if alias != "" {
			normalizedAliases = append(normalizedAliases, alias)
		}
	}
	normalizedAliases = uniqueChatTokens(normalizedAliases)
	if len(normalizedAliases) == 0 {
		return configuredDeterministicField{}, false
	}

	return configuredDeterministicField{
		Key:         strings.TrimSpace(key),
		Label:       strings.TrimSpace(label),
		Aliases:     normalizedAliases,
		SourceNames: uniqueConfiguredSourceNames(aliases),
	}, true
}

func uniqueConfiguredSourceNames(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		identity := strings.ToLower(value)
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		out = append(out, value)
	}
	return out
}

func configuredStringValue(value map[string]any, keys ...string) string {
	for _, key := range keys {
		raw, ok := value[key]
		if !ok {
			continue
		}
		text, ok := raw.(string)
		if ok && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func uniqueConfiguredDeterministicFields(fields []configuredDeterministicField) []configuredDeterministicField {
	indexes := make(map[string]int, len(fields))
	out := make([]configuredDeterministicField, 0, len(fields))
	for _, field := range fields {
		identity := normalizeChatSearchValue(firstNonEmptyString(field.Key, field.Label))
		if identity == "" {
			continue
		}
		if index, ok := indexes[identity]; ok {
			merged := &out[index]
			merged.Aliases = uniqueChatTokens(append(merged.Aliases, field.Aliases...))
			merged.SourceNames = uniqueConfiguredSourceNames(append(merged.SourceNames, field.SourceNames...))
			if merged.Key == "" {
				merged.Key = field.Key
			}
			if merged.Label == "" {
				merged.Label = field.Label
			}
			continue
		}
		indexes[identity] = len(out)
		out = append(out, field)
	}
	return out
}

func matchConfiguredDeterministicField(fields []configuredDeterministicField, normalizedQuestion string) (configuredDeterministicField, bool) {
	bestIndex := -1
	bestLength := 0
	ambiguous := false
	for index, field := range fields {
		fieldBest := 0
		for _, questionAlias := range configuredDeterministicFieldQuestionAliases(field) {
			if containsStructuredTokenSequence(normalizedQuestion, questionAlias) && len(questionAlias) > fieldBest {
				fieldBest = len(questionAlias)
			}
		}
		if fieldBest == 0 {
			continue
		}
		if fieldBest > bestLength {
			bestIndex = index
			bestLength = fieldBest
			ambiguous = false
			continue
		}
		if fieldBest == bestLength {
			ambiguous = true
		}
	}
	if bestIndex < 0 || ambiguous {
		return configuredDeterministicField{}, false
	}
	return fields[bestIndex], true
}

func configuredDeterministicFieldQuestionAliases(field configuredDeterministicField) []string {
	aliases := make([]string, 0, len(field.Aliases)*2)
	for _, alias := range field.Aliases {
		aliases = append(aliases, configuredFieldQuestionAliases(alias)...)
	}
	return uniqueChatTokens(aliases)
}

func configuredFieldQuestionAliases(alias string) []string {
	alias = normalizeChatSearchValue(alias)
	if alias == "" {
		return nil
	}

	aliases := []string{alias}
	parts := strings.Fields(alias)
	if len(parts) == 0 {
		return aliases
	}
	last := parts[len(parts)-1]
	switch {
	case strings.HasSuffix(last, "y") && len(last) > 1:
		parts[len(parts)-1] = strings.TrimSuffix(last, "y") + "ies"
	case !strings.HasSuffix(last, "s"):
		parts[len(parts)-1] = last + "s"
	}
	aliases = append(aliases, strings.Join(parts, " "))
	return uniqueChatTokens(aliases)
}

func configuredDistinctFieldValues(rows []cachedStructuredChatRow, indexes []int, field configuredDeterministicField) []string {
	aliases := make(map[string]struct{}, len(field.Aliases))
	for _, alias := range field.Aliases {
		aliases[normalizeChatSearchValue(alias)] = struct{}{}
	}

	values := make(map[string]string)
	for _, index := range indexes {
		if index < 0 || index >= len(rows) {
			continue
		}
		for _, sourceField := range rows[index].SourceFields {
			if _, ok := aliases[sourceField.NormalizedName]; !ok {
				continue
			}
			normalizedValue := normalizeChatSearchValue(firstNonEmptyString(sourceField.NormalizedValue, sourceField.Raw))
			displayValue := strings.TrimSpace(firstNonEmptyString(sourceField.Raw, sourceField.NormalizedValue))
			if normalizedValue == "" || displayValue == "" {
				continue
			}
			if _, exists := values[normalizedValue]; !exists {
				values[normalizedValue] = displayValue
			}
		}
	}

	out := make([]string, 0, len(values))
	for _, display := range values {
		out = append(out, display)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out
}
