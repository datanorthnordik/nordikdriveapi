package chat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	f "nordik-drive-api/internal/file"

	"gorm.io/datatypes"
)

type chatStructuredDatasetCacheEntry struct {
	rows []cachedStructuredChatRow
}

type cachedStructuredChatRow struct {
	SourceRowID         int
	CanonicalName       string
	NameAliasText       string
	IdentifierText      string
	CanonicalCommunity  string
	CanonicalSchool     string
	CoreSearchText      string
	SearchText          string
	DefaultBundle       structuredChatDefaultBundle
	NarrativeBundle     structuredChatNarrativeBundle
	DefaultBundleJSON   string
	NarrativeBundleJSON string
	HasNarrative        bool
}

type preparedStructuredChatDataset struct {
	PromptJSON           string
	RowRefToID           map[string]int
	TotalRows            int
	SelectedRows         int
	NarrativeRows        int
	RetrievalMode        string
	PromptProjectionMode string
}

type structuredChatRowDB struct {
	SourceRowID        uint           `gorm:"column:source_row_id"`
	CanonicalName      string         `gorm:"column:canonical_name"`
	CanonicalCommunity string         `gorm:"column:canonical_community"`
	CanonicalSchool    string         `gorm:"column:canonical_school"`
	SearchText         string         `gorm:"column:search_text"`
	RowDataNormalized  datatypes.JSON `gorm:"column:row_data_normalized"`
}

type structuredChatNormalizedPayload struct {
	Canonical *structuredChatCanonicalPayload `json:"canonical,omitempty"`
	Chat      *structuredChatBundlePayload    `json:"chat,omitempty"`
}

type structuredChatCanonicalPayload struct {
	DisplayName      string                       `json:"display_name,omitempty"`
	NameAliases      []string                     `json:"name_aliases,omitempty"`
	StudentNumber    string                       `json:"student_number,omitempty"`
	StudentNumberRaw string                       `json:"student_number_raw,omitempty"`
	Community        string                       `json:"community,omitempty"`
	School           string                       `json:"school,omitempty"`
	DeceasedStatus   string                       `json:"deceased_status,omitempty"`
	ParentsNames     string                       `json:"parents_names,omitempty"`
	MappingLocation  string                       `json:"mapping_location,omitempty"`
	Dates            structuredChatCanonicalDates `json:"dates,omitempty"`
}

type structuredChatCanonicalDates struct {
	Birth      *structuredChatCanonicalDate `json:"birth,omitempty"`
	Admitted   *structuredChatCanonicalDate `json:"admitted,omitempty"`
	Discharged *structuredChatCanonicalDate `json:"discharged,omitempty"`
}

type structuredChatCanonicalDate struct {
	Raw string `json:"raw,omitempty"`
	ISO string `json:"iso,omitempty"`
}

type structuredChatBundlePayload struct {
	DefaultBundle   map[string]any `json:"default_bundle,omitempty"`
	NarrativeBundle map[string]any `json:"narrative_bundle,omitempty"`
}

type structuredChatDefaultBundle struct {
	RecordProfile            string   `json:"record_profile,omitempty"`
	Name                     string   `json:"name,omitempty"`
	Aliases                  []string `json:"aliases,omitempty"`
	StudentNumber            string   `json:"student_number,omitempty"`
	Community                string   `json:"community,omitempty"`
	School                   string   `json:"school,omitempty"`
	DeceasedStatus           string   `json:"deceased_status,omitempty"`
	DateOfBirth              string   `json:"date_of_birth,omitempty"`
	Admitted                 string   `json:"admitted,omitempty"`
	Discharged               string   `json:"discharged,omitempty"`
	ParentsNames             string   `json:"parents_names,omitempty"`
	MappingLocation          string   `json:"mapping_location,omitempty"`
	HasNotes                 bool     `json:"has_notes,omitempty"`
	HasAdditionalInformation bool     `json:"has_additional_information,omitempty"`
	HasDeathDetails          bool     `json:"has_death_details,omitempty"`
	HasPhotos                bool     `json:"has_photos,omitempty"`
}

type structuredChatNarrativeBundle struct {
	Notes                 string `json:"notes,omitempty"`
	AdditionalInformation string `json:"additional_information,omitempty"`
	DeathDetails          string `json:"death_details,omitempty"`
}

const chatCompactDataInstruction = `
- Each item includes a row_ref and a default_bundle with the structured facts most relevant to the question.
- A narrative_bundle is included only when longer notes/details are likely relevant to the question.
- Some non-essential fields may be omitted to keep the prompt compact.
- Only rely on the fields that are actually shown in the bundles below.
`

func (cs *ChatService) getPreparedStructuredChatDataset(fileID uint, version int, question string, communities []string) (*preparedStructuredChatDataset, error) {
	dataset, err := cs.getOrLoadStructuredChatDataset(fileID, version)
	if err != nil {
		return nil, err
	}
	if dataset == nil || len(dataset.rows) == 0 {
		return nil, fmt.Errorf("structured dataset unavailable")
	}

	selection := selectStructuredChatRows(dataset.rows, question, communities)
	projection := buildChatPromptProjectionProfile(question, selection.Mode, len(selection.Indexes), selection.IncludeNarrative)
	promptJSON, rowRefToID, narrativeRows, err := buildStructuredPromptJSONArray(dataset.rows, selection.Indexes, selection.IncludeNarrative, projection)
	if err != nil {
		return nil, err
	}

	return &preparedStructuredChatDataset{
		PromptJSON:           promptJSON,
		RowRefToID:           rowRefToID,
		TotalRows:            len(dataset.rows),
		SelectedRows:         len(selection.Indexes),
		NarrativeRows:        narrativeRows,
		RetrievalMode:        selection.Mode,
		PromptProjectionMode: projection.Mode,
	}, nil
}

func (cs *ChatService) getOrLoadStructuredChatDataset(fileID uint, version int) (*chatStructuredDatasetCacheEntry, error) {
	cacheKey := chatDatasetCacheKey(fileID, version)
	if cached, ok := cs.structuredDatasetCache.Load(cacheKey); ok {
		if entry, ok := cached.(*chatStructuredDatasetCacheEntry); ok {
			return entry, nil
		}
	}

	var rawRows []structuredChatRowDB
	err := cs.DB.Table("file_data_normalized").
		Select("source_row_id, canonical_name, canonical_community, canonical_school, search_text, row_data_normalized").
		Where("file_id = ? AND version = ? AND status = ? AND normalization_version = ?", fileID, version, "ready", f.CurrentNormalizationVersion()).
		Order("source_row_id ASC").
		Scan(&rawRows).Error
	if err != nil {
		return nil, err
	}
	if len(rawRows) == 0 {
		return nil, fmt.Errorf("no normalized rows found")
	}

	rows := make([]cachedStructuredChatRow, 0, len(rawRows))
	for _, rawRow := range rawRows {
		row, ok := buildCachedStructuredChatRow(rawRow)
		if !ok {
			continue
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no structured chat rows available")
	}

	entry := &chatStructuredDatasetCacheEntry{rows: rows}
	actual, _ := cs.structuredDatasetCache.LoadOrStore(cacheKey, entry)
	if cached, ok := actual.(*chatStructuredDatasetCacheEntry); ok {
		return cached, nil
	}
	return entry, nil
}

func buildCachedStructuredChatRow(rawRow structuredChatRowDB) (cachedStructuredChatRow, bool) {
	row := cachedStructuredChatRow{
		SourceRowID:        int(rawRow.SourceRowID),
		CanonicalName:      strings.TrimSpace(rawRow.CanonicalName),
		CanonicalCommunity: strings.TrimSpace(rawRow.CanonicalCommunity),
		CanonicalSchool:    strings.TrimSpace(rawRow.CanonicalSchool),
		SearchText:         normalizeChatSearchValue(rawRow.SearchText),
	}

	var payload structuredChatNormalizedPayload
	if err := json.Unmarshal(rawRow.RowDataNormalized, &payload); err != nil {
		return cachedStructuredChatRow{}, false
	}

	defaultBundle := map[string]any{}
	narrativeBundle := map[string]any{}
	if payload.Chat != nil {
		if payload.Chat.DefaultBundle != nil {
			defaultBundle = payload.Chat.DefaultBundle
		}
		if payload.Chat.NarrativeBundle != nil {
			narrativeBundle = payload.Chat.NarrativeBundle
		}
	}
	if len(defaultBundle) == 0 {
		defaultBundle = buildFallbackDefaultBundle(payload.Canonical)
	}

	defaultBundleJSON, err := json.Marshal(defaultBundle)
	if err != nil {
		return cachedStructuredChatRow{}, false
	}

	row.DefaultBundleJSON = string(defaultBundleJSON)
	_ = json.Unmarshal(defaultBundleJSON, &row.DefaultBundle)
	row.HasNarrative = len(narrativeBundle) > 0
	if row.HasNarrative {
		narrativeBundleJSON, err := json.Marshal(narrativeBundle)
		if err != nil {
			return cachedStructuredChatRow{}, false
		}
		row.NarrativeBundleJSON = string(narrativeBundleJSON)
		_ = json.Unmarshal(narrativeBundleJSON, &row.NarrativeBundle)
	}

	if payload.Canonical != nil {
		identifierValues := []string{
			payload.Canonical.StudentNumber,
			payload.Canonical.StudentNumberRaw,
		}
		coreValues := []string{
			payload.Canonical.DisplayName,
			strings.Join(payload.Canonical.NameAliases, " "),
			payload.Canonical.StudentNumber,
			payload.Canonical.StudentNumberRaw,
			payload.Canonical.Community,
			payload.Canonical.School,
			payload.Canonical.DeceasedStatus,
			payload.Canonical.ParentsNames,
			payload.Canonical.MappingLocation,
			displayStructuredDate(payload.Canonical.Dates.Birth),
			displayStructuredDate(payload.Canonical.Dates.Admitted),
			displayStructuredDate(payload.Canonical.Dates.Discharged),
		}
		row.NameAliasText = normalizeChatSearchValue(strings.Join(payload.Canonical.NameAliases, " "))
		row.IdentifierText = normalizeChatSearchValue(strings.Join(identifierValues, " "))
		row.CoreSearchText = normalizeChatSearchValue(strings.Join(coreValues, " "))
		if row.CanonicalName == "" {
			row.CanonicalName = normalizeChatSearchValue(payload.Canonical.DisplayName)
		}
		if row.CanonicalCommunity == "" {
			row.CanonicalCommunity = normalizeChatSearchValue(payload.Canonical.Community)
		}
		if row.CanonicalSchool == "" {
			row.CanonicalSchool = normalizeChatSearchValue(payload.Canonical.School)
		}
	}
	if row.CanonicalName == "" {
		row.CanonicalName = normalizeChatSearchValue(row.DefaultBundle.Name)
	}
	if row.CanonicalCommunity == "" {
		row.CanonicalCommunity = normalizeChatSearchValue(row.DefaultBundle.Community)
	}
	if row.CanonicalSchool == "" {
		row.CanonicalSchool = normalizeChatSearchValue(row.DefaultBundle.School)
	}

	if row.CoreSearchText == "" {
		row.CoreSearchText = normalizeChatSearchValue(strings.Join(extractBundleStringValues(defaultBundle), " "))
	}
	if row.SearchText == "" {
		row.SearchText = row.CoreSearchText
	}
	if row.CanonicalName == "" && row.CoreSearchText == "" && row.SearchText == "" {
		return cachedStructuredChatRow{}, false
	}
	return row, true
}

func buildFallbackDefaultBundle(canonical *structuredChatCanonicalPayload) map[string]any {
	if canonical == nil {
		return map[string]any{}
	}

	bundle := map[string]any{}
	setBundleField(bundle, "name", canonical.DisplayName)
	setBundleField(bundle, "student_number", firstNonEmptyString(canonical.StudentNumberRaw, canonical.StudentNumber))
	setBundleField(bundle, "community", canonical.Community)
	setBundleField(bundle, "school", canonical.School)
	setBundleField(bundle, "deceased_status", canonical.DeceasedStatus)
	setBundleField(bundle, "date_of_birth", displayStructuredDate(canonical.Dates.Birth))
	setBundleField(bundle, "admitted", displayStructuredDate(canonical.Dates.Admitted))
	setBundleField(bundle, "discharged", displayStructuredDate(canonical.Dates.Discharged))
	setBundleField(bundle, "parents_names", canonical.ParentsNames)
	setBundleField(bundle, "mapping_location", canonical.MappingLocation)
	if len(canonical.NameAliases) > 0 {
		bundle["aliases"] = append([]string(nil), canonical.NameAliases...)
	}
	return bundle
}

func buildStructuredPromptJSONArray(rows []cachedStructuredChatRow, indexes []int, includeNarrative bool, projection chatPromptProjectionProfile) (string, map[string]int, int, error) {
	if len(indexes) == 0 {
		return "[]", map[string]int{}, 0, nil
	}

	var builder strings.Builder
	rowRefToID := make(map[string]int, len(indexes))
	narrativeRows := 0

	builder.WriteByte('[')
	for idx, rowIndex := range indexes {
		row := rows[rowIndex]
		if idx > 0 {
			builder.WriteByte(',')
		}

		rowRef := buildPromptRowRef(idx + 1)
		rowRefToID[rowRef] = row.SourceRowID

		builder.WriteString(`{"row_ref":`)
		builder.WriteString(strconv.Quote(rowRef))
		builder.WriteString(`,"default_bundle":`)
		defaultBundleJSON, err := marshalProjectedDefaultBundle(row.DefaultBundle, projection)
		if err != nil {
			return "", nil, 0, err
		}
		builder.WriteString(defaultBundleJSON)
		if includeNarrative && row.HasNarrative && row.NarrativeBundleJSON != "" {
			builder.WriteString(`,"narrative_bundle":`)
			narrativeBundleJSON, err := marshalProjectedNarrativeBundle(row.NarrativeBundle, projection)
			if err != nil {
				return "", nil, 0, err
			}
			builder.WriteString(narrativeBundleJSON)
			narrativeRows++
		}
		builder.WriteByte('}')
	}
	builder.WriteByte(']')

	return builder.String(), rowRefToID, narrativeRows, nil
}

func displayStructuredDate(date *structuredChatCanonicalDate) string {
	if date == nil {
		return ""
	}
	return strings.TrimSpace(firstNonEmptyString(date.Raw, date.ISO))
}

func extractBundleStringValues(bundle map[string]any) []string {
	if len(bundle) == 0 {
		return nil
	}

	keys := make([]string, 0, len(bundle))
	for key := range bundle {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	values := make([]string, 0, len(bundle))
	for _, key := range keys {
		switch v := bundle[key].(type) {
		case string:
			if strings.TrimSpace(v) != "" {
				values = append(values, v)
			}
		case []string:
			values = append(values, v...)
		case []any:
			for _, item := range v {
				if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
					values = append(values, text)
				}
			}
		}
	}
	return values
}

func setBundleField(bundle map[string]any, key string, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	bundle[key] = value
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
