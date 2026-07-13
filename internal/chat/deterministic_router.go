package chat

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type deterministicChatRoute struct {
	QueryType     string
	RetrievalMode string
	Answer        string
	MatchedRowID  *int
	RowsSelected  int
}

type deterministicQuestionContext struct {
	Question           string
	NormalizedQuestion string
	Profile            chatQuestionProfile
	Filters            deterministicRowFilter
	ConsumedTokens     map[string]struct{}
}

type deterministicRowFilter struct {
	CommunityNormalized string
	CommunityDisplay    string
	SchoolNormalized    string
	SchoolDisplay       string
	DeceasedStatus      *string
	RequireNotes        *bool
	RequireAddInfo      *bool
	RequireDeathDetails *bool
	RequirePhotos       *bool
}

type deterministicFieldValueMatch struct {
	Normalized string
	Display    string
}

type deterministicGroupedCount struct {
	Normalized string
	Display    string
	Count      int
}

var deterministicRouteNoiseTokens = map[string]struct{}{
	"count": {}, "counts": {}, "each": {}, "fewest": {}, "highest": {}, "largest": {}, "least": {}, "many": {},
	"most": {}, "number": {}, "numbers": {}, "per": {}, "smallest": {}, "total": {},
}

var deterministicLookupFieldPhrases = []struct {
	Field   string
	Phrases []string
}{
	{Field: "date_of_birth", Phrases: []string{"date of birth", "birth date", "dob", "born"}},
	{Field: "student_number", Phrases: []string{"student number", "student id", "id number", "record number", "registration number", "admission number", "identifier"}},
	{Field: "parents_names", Phrases: []string{"parents names", "parent names", "parents"}},
	{Field: "mapping_location", Phrases: []string{"mapping location", "map location", "location", "place"}},
	{Field: "deceased_status", Phrases: []string{"deceased status", "deceased", "alive", "living", "passed away", "did die"}},
	{Field: "admitted", Phrases: []string{"date admitted", "admission date", "admitted"}},
	{Field: "discharged", Phrases: []string{"date discharged", "discharge date", "discharged"}},
	{Field: "school", Phrases: []string{"residential school", "school", "institution", "attend", "attended"}},
	{Field: "community", Phrases: []string{"first nation", "community", "reserve", "band"}},
}

var deterministicGroupFieldPhrases = []struct {
	Field   string
	Phrases []string
}{
	{Field: "community", Phrases: []string{"first nation", "communities", "community", "reserves", "reserve", "bands", "band"}},
	{Field: "school", Phrases: []string{"residential schools", "residential school", "schools", "school", "institutions", "institution"}},
}

func (cs *ChatService) tryDeterministicChat(input ChatQueryInput) (*ChatResult, bool, error) {
	start := time.Now()

	question := strings.TrimSpace(input.Question)
	if question == "" {
		return nil, false, nil
	}

	dataset, err := cs.getOrLoadStructuredChatDataset(input.FileID, input.Version)
	if err != nil || dataset == nil || len(dataset.rows) == 0 {
		return nil, false, nil
	}

	filteredCommunities := normalizeCommunities(input.Communities)
	route, ok := buildDeterministicChatRoute(dataset.rows, question, filteredCommunities)
	if !ok {
		return nil, false, nil
	}

	debug := &ChatDebugMetrics{
		Strategy:             "deterministic_router",
		ExecutionMode:        "deterministic",
		QueryType:            route.QueryType,
		RetrievalMode:        route.RetrievalMode,
		PromptProjectionMode: "deterministic",
		Version:              input.Version,
		CommunityFilterCount: len(filteredCommunities),
		TotalRowsLoaded:      len(dataset.rows),
		RowsSelected:         route.RowsSelected,
		PromptChars:          0,
		PromptBytes:          0,
		AudioIncluded:        false,
		PreparationMillis:    time.Since(start).Milliseconds(),
	}

	return &ChatResult{
		Answer:       route.Answer,
		MatchedRowID: route.MatchedRowID,
		Debug:        debug,
	}, true, nil
}

func buildDeterministicChatRoute(rows []cachedStructuredChatRow, question string, communities []string) (deterministicChatRoute, bool) {
	ctx := buildDeterministicQuestionContext(rows, question)
	if ctx.NormalizedQuestion == "" {
		return deterministicChatRoute{}, false
	}

	if route, ok := tryDeterministicGroupExtremeRoute(rows, communities, cloneDeterministicQuestionContext(ctx)); ok {
		return route, true
	}
	if route, ok := tryDeterministicGroupSummaryRoute(rows, communities, cloneDeterministicQuestionContext(ctx)); ok {
		return route, true
	}
	if route, ok := tryDeterministicFieldLookupRoute(rows, communities, cloneDeterministicQuestionContext(ctx)); ok {
		return route, true
	}
	if route, ok := tryDeterministicExistenceRoute(rows, communities, cloneDeterministicQuestionContext(ctx)); ok {
		return route, true
	}
	if route, ok := tryDeterministicCountRoute(rows, communities, cloneDeterministicQuestionContext(ctx)); ok {
		return route, true
	}

	return deterministicChatRoute{}, false
}

func buildDeterministicQuestionContext(rows []cachedStructuredChatRow, question string) deterministicQuestionContext {
	normalizedQuestion := normalizeChatSearchValue(question)
	consumed := make(map[string]struct{})
	filters := extractDeterministicFilters(rows, normalizedQuestion, consumed)

	return deterministicQuestionContext{
		Question:           question,
		NormalizedQuestion: normalizedQuestion,
		Profile:            buildChatQuestionProfile(question),
		Filters:            filters,
		ConsumedTokens:     consumed,
	}
}

func cloneDeterministicQuestionContext(ctx deterministicQuestionContext) deterministicQuestionContext {
	cloned := ctx
	if len(ctx.ConsumedTokens) == 0 {
		return cloned
	}
	cloned.ConsumedTokens = make(map[string]struct{}, len(ctx.ConsumedTokens))
	for key := range ctx.ConsumedTokens {
		cloned.ConsumedTokens[key] = struct{}{}
	}
	return cloned
}

func extractDeterministicFilters(rows []cachedStructuredChatRow, normalizedQuestion string, consumed map[string]struct{}) deterministicRowFilter {
	filters := deterministicRowFilter{}

	if community, ok := uniqueStructuredFieldValueMatch(rows, normalizedQuestion, func(row cachedStructuredChatRow) (string, string) {
		return row.CanonicalCommunity, structuredFieldDisplay(row.DefaultBundle.Community, row.CanonicalCommunity)
	}); ok {
		filters.CommunityNormalized = community.Normalized
		filters.CommunityDisplay = community.Display
		consumeNormalizedPhrase(consumed, community.Normalized)
	}

	if school, ok := uniqueStructuredFieldValueMatch(rows, normalizedQuestion, func(row cachedStructuredChatRow) (string, string) {
		return row.CanonicalSchool, structuredFieldDisplay(row.DefaultBundle.School, row.CanonicalSchool)
	}); ok {
		filters.SchoolNormalized = school.Normalized
		filters.SchoolDisplay = school.Display
		consumeNormalizedPhrase(consumed, school.Normalized)
	}

	if strings.Contains(normalizedQuestion, "death details") {
		filters.RequireDeathDetails = boolPtr(true)
		consumeNormalizedPhrase(consumed, "death details")
	}
	if strings.Contains(normalizedQuestion, "additional information") || strings.Contains(normalizedQuestion, "additional info") {
		filters.RequireAddInfo = boolPtr(true)
		consumeNormalizedPhrase(consumed, "additional information")
		consumeNormalizedPhrase(consumed, "additional info")
	}
	if strings.Contains(normalizedQuestion, "notes") || strings.Contains(normalizedQuestion, "note") {
		filters.RequireNotes = boolPtr(true)
		consumeNormalizedPhrase(consumed, "notes")
		consumeNormalizedPhrase(consumed, "note")
	}
	if strings.Contains(normalizedQuestion, "photos") || strings.Contains(normalizedQuestion, "photo") {
		filters.RequirePhotos = boolPtr(true)
		consumeNormalizedPhrase(consumed, "photos")
		consumeNormalizedPhrase(consumed, "photo")
	}

	switch {
	case strings.Contains(normalizedQuestion, "unknown deceased") || strings.Contains(normalizedQuestion, "deceased unknown"):
		status := "unknown"
		filters.DeceasedStatus = &status
		consumeNormalizedPhrase(consumed, "unknown deceased")
		consumeNormalizedPhrase(consumed, "deceased unknown")
	case strings.Contains(normalizedQuestion, "not deceased") || strings.Contains(normalizedQuestion, "living") || strings.Contains(normalizedQuestion, "alive"):
		status := "no"
		filters.DeceasedStatus = &status
		consumeNormalizedPhrase(consumed, "not deceased")
		consumeNormalizedPhrase(consumed, "living")
		consumeNormalizedPhrase(consumed, "alive")
	case strings.Contains(normalizedQuestion, "deceased") || strings.Contains(normalizedQuestion, "died") || strings.Contains(normalizedQuestion, "dead") || strings.Contains(normalizedQuestion, "passed away"):
		status := "yes"
		filters.DeceasedStatus = &status
		consumeNormalizedPhrase(consumed, "deceased")
		consumeNormalizedPhrase(consumed, "died")
		consumeNormalizedPhrase(consumed, "dead")
		consumeNormalizedPhrase(consumed, "passed away")
	}

	return filters
}

func tryDeterministicFieldLookupRoute(rows []cachedStructuredChatRow, communities []string, ctx deterministicQuestionContext) (deterministicChatRoute, bool) {
	field, consumed, ok := detectDeterministicLookupField(ctx.NormalizedQuestion)
	if !ok {
		return deterministicChatRoute{}, false
	}

	consumeTokens(ctx.ConsumedTokens, consumed...)

	candidateIndexes := applyDeterministicFilters(rows, communities, ctx.Filters)
	if len(candidateIndexes) == 0 {
		return deterministicChatRoute{}, false
	}

	selection := selectStructuredChatRowsFromIndexes(rows, candidateIndexes, ctx.Question)
	if len(selection.Indexes) != 1 {
		return deterministicChatRoute{}, false
	}

	row := rows[selection.Indexes[0]]
	answer, ok := buildDeterministicFieldLookupAnswer(row, field)
	if !ok {
		return deterministicChatRoute{}, false
	}

	matchedRowID := row.SourceRowID
	return deterministicChatRoute{
		QueryType:     "field_lookup",
		RetrievalMode: "deterministic_field_lookup",
		Answer:        answer,
		MatchedRowID:  &matchedRowID,
		RowsSelected:  1,
	}, true
}

func tryDeterministicExistenceRoute(rows []cachedStructuredChatRow, communities []string, ctx deterministicQuestionContext) (deterministicChatRoute, bool) {
	if !looksLikeDeterministicExistenceQuestion(ctx.NormalizedQuestion) {
		return deterministicChatRoute{}, false
	}
	if hasUnsupportedDeterministicTokens(ctx.Profile.Tokens, ctx.ConsumedTokens) {
		return deterministicChatRoute{}, false
	}

	selectedIndexes := applyDeterministicFilters(rows, communities, ctx.Filters)
	count := len(selectedIndexes)
	answer := "No, I didn't find any matching records."
	if count > 0 {
		answer = fmt.Sprintf("Yes, I found %d matching record%s.", count, pluralSuffix(count))
	}

	return deterministicChatRoute{
		QueryType:     "existence",
		RetrievalMode: "deterministic_existence",
		Answer:        answer,
		RowsSelected:  count,
	}, true
}

func tryDeterministicCountRoute(rows []cachedStructuredChatRow, communities []string, ctx deterministicQuestionContext) (deterministicChatRoute, bool) {
	if !looksLikeDeterministicCountQuestion(ctx.NormalizedQuestion) {
		return deterministicChatRoute{}, false
	}
	if hasUnsupportedDeterministicTokens(ctx.Profile.Tokens, ctx.ConsumedTokens) {
		return deterministicChatRoute{}, false
	}

	selectedIndexes := applyDeterministicFilters(rows, communities, ctx.Filters)
	count := len(selectedIndexes)
	answer := formatDeterministicCountAnswer(count, ctx.Filters, len(communities) > 0)

	return deterministicChatRoute{
		QueryType:     "count",
		RetrievalMode: "deterministic_count",
		Answer:        answer,
		RowsSelected:  count,
	}, true
}

func tryDeterministicGroupExtremeRoute(rows []cachedStructuredChatRow, communities []string, ctx deterministicQuestionContext) (deterministicChatRoute, bool) {
	extreme := deterministicGroupExtreme(ctx.NormalizedQuestion)
	if extreme == "" {
		return deterministicChatRoute{}, false
	}

	field, consumed, ok := detectDeterministicGroupField(ctx.NormalizedQuestion)
	if !ok {
		return deterministicChatRoute{}, false
	}
	consumeTokens(ctx.ConsumedTokens, consumed...)
	consumeNormalizedPhrase(ctx.ConsumedTokens, extreme)

	if hasUnsupportedDeterministicTokens(ctx.Profile.Tokens, ctx.ConsumedTokens) {
		return deterministicChatRoute{}, false
	}

	selectedIndexes := applyDeterministicFilters(rows, communities, ctx.Filters)
	grouped := buildDeterministicGroupedCounts(rows, selectedIndexes, field)
	if len(grouped) == 0 {
		return deterministicChatRoute{}, false
	}

	sort.Slice(grouped, func(i, j int) bool {
		if grouped[i].Count != grouped[j].Count {
			if extreme == "lowest" {
				return grouped[i].Count < grouped[j].Count
			}
			return grouped[i].Count > grouped[j].Count
		}
		return strings.ToLower(grouped[i].Display) < strings.ToLower(grouped[j].Display)
	})

	targetCount := grouped[0].Count
	tied := make([]string, 0, len(grouped))
	for _, item := range grouped {
		if item.Count != targetCount {
			break
		}
		tied = append(tied, item.Display)
	}

	scopePrefix := deterministicScopePrefix(ctx.Filters, len(communities) > 0)
	groupLabel := deterministicGroupLabel(field)
	answer := ""
	if len(tied) == 1 {
		answer = fmt.Sprintf("%s%s has the %s %s, with %d record%s.", scopePrefix, tied[0], extreme, groupLabel, targetCount, pluralSuffix(targetCount))
	} else {
		answer = fmt.Sprintf("%s%s tie for the %s %s, with %d record%s each.", scopePrefix, joinNaturalStrings(tied), extreme, groupLabel, targetCount, pluralSuffix(targetCount))
	}

	return deterministicChatRoute{
		QueryType:     "group_extreme",
		RetrievalMode: "deterministic_group_extreme",
		Answer:        answer,
		RowsSelected:  len(selectedIndexes),
	}, true
}

func tryDeterministicGroupSummaryRoute(rows []cachedStructuredChatRow, communities []string, ctx deterministicQuestionContext) (deterministicChatRoute, bool) {
	if !looksLikeDeterministicGroupSummaryQuestion(ctx.NormalizedQuestion) {
		return deterministicChatRoute{}, false
	}

	field, consumed, ok := detectDeterministicGroupField(ctx.NormalizedQuestion)
	if !ok {
		return deterministicChatRoute{}, false
	}
	consumeTokens(ctx.ConsumedTokens, consumed...)

	if hasUnsupportedDeterministicTokens(ctx.Profile.Tokens, ctx.ConsumedTokens) {
		return deterministicChatRoute{}, false
	}

	selectedIndexes := applyDeterministicFilters(rows, communities, ctx.Filters)
	grouped := buildDeterministicGroupedCounts(rows, selectedIndexes, field)
	if len(grouped) == 0 || len(grouped) > 8 {
		return deterministicChatRoute{}, false
	}

	sort.Slice(grouped, func(i, j int) bool {
		if grouped[i].Count != grouped[j].Count {
			return grouped[i].Count > grouped[j].Count
		}
		return strings.ToLower(grouped[i].Display) < strings.ToLower(grouped[j].Display)
	})

	parts := make([]string, 0, len(grouped))
	for _, item := range grouped {
		parts = append(parts, fmt.Sprintf("%s (%d)", item.Display, item.Count))
	}

	answer := fmt.Sprintf("By %s, the matching records break down as %s.", deterministicGroupDimension(field), joinNaturalStrings(parts))
	return deterministicChatRoute{
		QueryType:     "group_summary",
		RetrievalMode: "deterministic_group_summary",
		Answer:        answer,
		RowsSelected:  len(selectedIndexes),
	}, true
}

func applyDeterministicFilters(rows []cachedStructuredChatRow, communities []string, filters deterministicRowFilter) []int {
	indexes := filterStructuredRowsByCommunity(rows, communities)
	out := make([]int, 0, len(indexes))
	for _, index := range indexes {
		row := rows[index]
		if filters.CommunityNormalized != "" && row.CanonicalCommunity != filters.CommunityNormalized {
			continue
		}
		if filters.SchoolNormalized != "" && row.CanonicalSchool != filters.SchoolNormalized {
			continue
		}
		if filters.DeceasedStatus != nil && normalizeChatSearchValue(row.DefaultBundle.DeceasedStatus) != normalizeChatSearchValue(*filters.DeceasedStatus) {
			continue
		}
		if filters.RequireNotes != nil && row.DefaultBundle.HasNotes != *filters.RequireNotes {
			continue
		}
		if filters.RequireAddInfo != nil && row.DefaultBundle.HasAdditionalInformation != *filters.RequireAddInfo {
			continue
		}
		if filters.RequireDeathDetails != nil && row.DefaultBundle.HasDeathDetails != *filters.RequireDeathDetails {
			continue
		}
		if filters.RequirePhotos != nil && row.DefaultBundle.HasPhotos != *filters.RequirePhotos {
			continue
		}
		out = append(out, index)
	}
	return out
}

func buildDeterministicGroupedCounts(rows []cachedStructuredChatRow, indexes []int, field string) []deterministicGroupedCount {
	if len(indexes) == 0 {
		return nil
	}

	counts := map[string]deterministicGroupedCount{}
	for _, index := range indexes {
		normalized, display := deterministicGroupValue(rows[index], field)
		if normalized == "" || strings.TrimSpace(display) == "" {
			continue
		}
		item := counts[normalized]
		item.Normalized = normalized
		item.Display = display
		item.Count++
		counts[normalized] = item
	}

	out := make([]deterministicGroupedCount, 0, len(counts))
	for _, item := range counts {
		out = append(out, item)
	}
	return out
}

func deterministicGroupValue(row cachedStructuredChatRow, field string) (string, string) {
	switch field {
	case "community":
		return row.CanonicalCommunity, structuredFieldDisplay(row.DefaultBundle.Community, row.CanonicalCommunity)
	case "school":
		return row.CanonicalSchool, structuredFieldDisplay(row.DefaultBundle.School, row.CanonicalSchool)
	default:
		return "", ""
	}
}

func detectDeterministicLookupField(normalizedQuestion string) (string, []string, bool) {
	matchedFields := make(map[string][]string)

	if strings.Contains(normalizedQuestion, "where") && strings.Contains(normalizedQuestion, " from") {
		matchedFields["community"] = append(matchedFields["community"], "from")
	}

	for _, candidate := range deterministicLookupFieldPhrases {
		for _, phrase := range candidate.Phrases {
			normalizedPhrase := normalizeChatSearchValue(phrase)
			if normalizedPhrase == "" || !strings.Contains(normalizedQuestion, normalizedPhrase) {
				continue
			}
			matchedFields[candidate.Field] = append(matchedFields[candidate.Field], strings.Fields(normalizedPhrase)...)
		}
	}

	if len(matchedFields) != 1 {
		return "", nil, false
	}

	for field, tokens := range matchedFields {
		return field, uniqueChatTokens(tokens), true
	}

	return "", nil, false
}

func detectDeterministicGroupField(normalizedQuestion string) (string, []string, bool) {
	matchedFields := make(map[string][]string)
	for _, candidate := range deterministicGroupFieldPhrases {
		for _, phrase := range candidate.Phrases {
			normalizedPhrase := normalizeChatSearchValue(phrase)
			if normalizedPhrase == "" || !strings.Contains(normalizedQuestion, normalizedPhrase) {
				continue
			}
			matchedFields[candidate.Field] = append(matchedFields[candidate.Field], strings.Fields(normalizedPhrase)...)
		}
	}

	if len(matchedFields) != 1 {
		return "", nil, false
	}

	for field, tokens := range matchedFields {
		return field, uniqueChatTokens(tokens), true
	}

	return "", nil, false
}

func buildDeterministicFieldLookupAnswer(row cachedStructuredChatRow, field string) (string, bool) {
	name := strings.TrimSpace(firstNonEmptyString(row.DefaultBundle.Name, row.CanonicalName))
	if name == "" {
		name = "This record"
	}

	switch field {
	case "student_number":
		if strings.TrimSpace(row.DefaultBundle.StudentNumber) == "" {
			return fmt.Sprintf("%s is in the data, but a student number isn't listed.", name), true
		}
		return fmt.Sprintf("%s's student number is %s.", name, row.DefaultBundle.StudentNumber), true
	case "community":
		if strings.TrimSpace(row.DefaultBundle.Community) == "" {
			return fmt.Sprintf("%s is in the data, but a community isn't listed.", name), true
		}
		return fmt.Sprintf("%s is from %s.", name, row.DefaultBundle.Community), true
	case "school":
		if strings.TrimSpace(row.DefaultBundle.School) == "" {
			return fmt.Sprintf("%s is in the data, but a school isn't listed.", name), true
		}
		return fmt.Sprintf("%s's school is %s.", name, row.DefaultBundle.School), true
	case "date_of_birth":
		if strings.TrimSpace(row.DefaultBundle.DateOfBirth) == "" {
			return fmt.Sprintf("%s is in the data, but a date of birth isn't listed.", name), true
		}
		return fmt.Sprintf("%s's date of birth is %s.", name, row.DefaultBundle.DateOfBirth), true
	case "admitted":
		if strings.TrimSpace(row.DefaultBundle.Admitted) == "" {
			return fmt.Sprintf("%s is in the data, but an admission date isn't listed.", name), true
		}
		return fmt.Sprintf("%s was admitted on %s.", name, row.DefaultBundle.Admitted), true
	case "discharged":
		if strings.TrimSpace(row.DefaultBundle.Discharged) == "" {
			return fmt.Sprintf("%s is in the data, but a discharge date isn't listed.", name), true
		}
		return fmt.Sprintf("%s was discharged on %s.", name, row.DefaultBundle.Discharged), true
	case "parents_names":
		if strings.TrimSpace(row.DefaultBundle.ParentsNames) == "" {
			return fmt.Sprintf("%s is in the data, but parents' names aren't listed.", name), true
		}
		return fmt.Sprintf("%s's parents are listed as %s.", name, row.DefaultBundle.ParentsNames), true
	case "mapping_location":
		if strings.TrimSpace(row.DefaultBundle.MappingLocation) == "" {
			return fmt.Sprintf("%s is in the data, but a mapping location isn't listed.", name), true
		}
		return fmt.Sprintf("%s's mapping location is %s.", name, row.DefaultBundle.MappingLocation), true
	case "deceased_status":
		switch normalizeChatSearchValue(row.DefaultBundle.DeceasedStatus) {
		case "yes":
			return fmt.Sprintf("%s is marked as deceased.", name), true
		case "no":
			return fmt.Sprintf("%s is marked as not deceased.", name), true
		case "unknown":
			return fmt.Sprintf("%s's deceased status is marked as unknown.", name), true
		default:
			return fmt.Sprintf("%s is in the data, but deceased status isn't listed.", name), true
		}
	default:
		return "", false
	}
}

func uniqueStructuredFieldValueMatch(rows []cachedStructuredChatRow, normalizedQuestion string, getter func(cachedStructuredChatRow) (string, string)) (deterministicFieldValueMatch, bool) {
	matches := make([]deterministicFieldValueMatch, 0, 2)
	seen := make(map[string]struct{})

	candidates := make([]deterministicFieldValueMatch, 0, len(rows))
	for _, row := range rows {
		normalizedValue, displayValue := getter(row)
		normalizedValue = strings.TrimSpace(normalizedValue)
		displayValue = strings.TrimSpace(displayValue)
		if normalizedValue == "" {
			continue
		}
		if _, exists := seen[normalizedValue]; exists {
			continue
		}
		seen[normalizedValue] = struct{}{}
		candidates = append(candidates, deterministicFieldValueMatch{
			Normalized: normalizedValue,
			Display:    firstNonEmptyString(displayValue, normalizedValue),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if len(candidates[i].Normalized) != len(candidates[j].Normalized) {
			return len(candidates[i].Normalized) > len(candidates[j].Normalized)
		}
		return candidates[i].Normalized < candidates[j].Normalized
	})

	for _, candidate := range candidates {
		if !containsStructuredTokenSequence(normalizedQuestion, candidate.Normalized) {
			continue
		}
		overlapped := false
		for _, existing := range matches {
			if containsStructuredTokenSequence(existing.Normalized, candidate.Normalized) {
				overlapped = true
				break
			}
		}
		if overlapped {
			continue
		}
		matches = append(matches, candidate)
	}

	if len(matches) != 1 {
		return deterministicFieldValueMatch{}, false
	}
	return matches[0], true
}

func looksLikeDeterministicCountQuestion(normalizedQuestion string) bool {
	return strings.Contains(normalizedQuestion, "how many") ||
		strings.Contains(normalizedQuestion, "count ") ||
		strings.HasPrefix(normalizedQuestion, "count") ||
		strings.Contains(normalizedQuestion, "number of")
}

func looksLikeDeterministicExistenceQuestion(normalizedQuestion string) bool {
	return strings.HasPrefix(normalizedQuestion, "are there any ") ||
		strings.HasPrefix(normalizedQuestion, "is there any ") ||
		strings.HasPrefix(normalizedQuestion, "did any ") ||
		strings.HasPrefix(normalizedQuestion, "do any ") ||
		strings.HasPrefix(normalizedQuestion, "does any ")
}

func looksLikeDeterministicGroupSummaryQuestion(normalizedQuestion string) bool {
	signals := []string{
		"by community", "per community", "each community",
		"by communities", "per communities", "each communities",
		"by school", "per school", "each school",
		"by schools", "per schools", "each schools",
	}
	for _, signal := range signals {
		if strings.Contains(normalizedQuestion, signal) {
			return true
		}
	}
	return false
}

func deterministicGroupExtreme(normalizedQuestion string) string {
	switch {
	case strings.Contains(normalizedQuestion, "highest"), strings.Contains(normalizedQuestion, "most"), strings.Contains(normalizedQuestion, "largest"):
		return "highest"
	case strings.Contains(normalizedQuestion, "lowest"), strings.Contains(normalizedQuestion, "least"), strings.Contains(normalizedQuestion, "fewest"), strings.Contains(normalizedQuestion, "smallest"):
		return "lowest"
	default:
		return ""
	}
}

func hasUnsupportedDeterministicTokens(tokens []string, consumed map[string]struct{}) bool {
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if _, ok := consumed[token]; ok {
			continue
		}
		if _, ok := deterministicRouteNoiseTokens[token]; ok {
			continue
		}
		return true
	}
	return false
}

func formatDeterministicCountAnswer(count int, filters deterministicRowFilter, hasCommunityFilter bool) string {
	scope := deterministicScopeDescription(filters, hasCommunityFilter)
	if scope == "" {
		return fmt.Sprintf("I found %d record%s in the data.", count, pluralSuffix(count))
	}
	return fmt.Sprintf("I found %d matching record%s %s.", count, pluralSuffix(count), scope)
}

func deterministicScopeDescription(filters deterministicRowFilter, hasCommunityFilter bool) string {
	parts := make([]string, 0, 6)
	if filters.CommunityDisplay != "" {
		parts = append(parts, "for "+filters.CommunityDisplay)
	} else if hasCommunityFilter {
		parts = append(parts, "within the selected community filter")
	}
	if filters.SchoolDisplay != "" {
		parts = append(parts, "for "+filters.SchoolDisplay)
	}
	if filters.DeceasedStatus != nil {
		switch normalizeChatSearchValue(*filters.DeceasedStatus) {
		case "yes":
			parts = append(parts, "marked as deceased")
		case "no":
			parts = append(parts, "marked as not deceased")
		case "unknown":
			parts = append(parts, "with unknown deceased status")
		}
	}
	if filters.RequireNotes != nil && *filters.RequireNotes {
		parts = append(parts, "with notes")
	}
	if filters.RequireAddInfo != nil && *filters.RequireAddInfo {
		parts = append(parts, "with additional information")
	}
	if filters.RequireDeathDetails != nil && *filters.RequireDeathDetails {
		parts = append(parts, "with death details")
	}
	if filters.RequirePhotos != nil && *filters.RequirePhotos {
		parts = append(parts, "with photos")
	}
	return strings.Join(parts, " ")
}

func deterministicScopePrefix(filters deterministicRowFilter, hasCommunityFilter bool) string {
	scope := deterministicScopeDescription(filters, hasCommunityFilter)
	if scope == "" {
		return ""
	}
	return "Among the matching records, "
}

func deterministicGroupLabel(field string) string {
	switch field {
	case "community":
		return "community count"
	case "school":
		return "school count"
	default:
		return "group count"
	}
}

func deterministicGroupDimension(field string) string {
	switch field {
	case "community":
		return "community"
	case "school":
		return "school"
	default:
		return "group"
	}
}

func structuredFieldDisplay(display string, normalized string) string {
	display = strings.TrimSpace(display)
	if display != "" {
		return display
	}
	return strings.TrimSpace(normalized)
}

func joinNaturalStrings(values []string) string {
	switch len(values) {
	case 0:
		return ""
	case 1:
		return values[0]
	case 2:
		return values[0] + " and " + values[1]
	default:
		return strings.Join(values[:len(values)-1], ", ") + ", and " + values[len(values)-1]
	}
}

func consumeNormalizedPhrase(consumed map[string]struct{}, phrase string) {
	consumeTokens(consumed, strings.Fields(normalizeChatSearchValue(phrase))...)
}

func consumeTokens(consumed map[string]struct{}, tokens ...string) {
	for _, token := range tokens {
		token = normalizeChatSearchValue(token)
		if token == "" {
			continue
		}
		for _, part := range strings.Fields(token) {
			consumed[part] = struct{}{}
		}
	}
}

func pluralSuffix(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

func boolPtr(v bool) *bool {
	return &v
}
