package chat

import (
	"sort"
	"strings"
	"unicode"
)

type chatQuestionProfile struct {
	NormalizedQuestion  string
	Tokens              []string
	KeyPhrase           string
	LooksLikeEntity     bool
	WantsAggregate      bool
	WantsNarrative      bool
	WantsDeathFocus     bool
	WantsDeathNarrative bool
}

type structuredRowSelection struct {
	Indexes          []int
	IncludeNarrative bool
	Mode             string
}

type structuredRowMatch struct {
	Index        int
	Score        int
	NarrativeHit bool
}

var chatQuestionStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "any": {}, "are": {}, "as": {}, "at": {}, "be": {}, "been": {}, "by": {},
	"can": {}, "could": {}, "data": {}, "details": {}, "did": {}, "do": {}, "does": {}, "for": {}, "from": {},
	"had": {}, "has": {}, "have": {}, "how": {}, "in": {}, "is": {}, "it": {}, "its": {}, "list": {}, "me": {},
	"more": {}, "of": {}, "on": {}, "or": {}, "our": {}, "please": {}, "records": {}, "show": {}, "tell": {},
	"than": {}, "that": {}, "the": {}, "their": {}, "them": {}, "there": {}, "these": {}, "this": {}, "those": {},
	"to": {}, "us": {}, "was": {}, "were": {}, "what": {}, "when": {}, "where": {}, "which": {}, "who": {},
	"why": {}, "with": {}, "would": {}, "children": {}, "child": {}, "student": {}, "students": {},
}

func selectStructuredChatRows(rows []cachedStructuredChatRow, question string, communities []string) structuredRowSelection {
	filteredIndexes := filterStructuredRowsByCommunity(rows, communities)
	return selectStructuredChatRowsFromIndexes(rows, filteredIndexes, question)
}

func selectStructuredChatRowsFromIndexes(rows []cachedStructuredChatRow, filteredIndexes []int, question string) structuredRowSelection {
	if len(filteredIndexes) == 0 {
		return structuredRowSelection{Indexes: []int{}, Mode: "structured_no_matches"}
	}

	profile := buildChatQuestionProfile(question)
	filteredIndexes = prefilterStructuredChatRows(rows, filteredIndexes, profile)
	if profile.WantsAggregate && !profile.LooksLikeEntity {
		return structuredRowSelection{
			Indexes:          selectStructuredAggregateRows(rows, filteredIndexes, question),
			IncludeNarrative: profile.WantsNarrative,
			Mode:             "aggregate_dataset",
		}
	}
	if len(profile.Tokens) == 0 {
		return structuredRowSelection{
			Indexes:          filteredIndexes,
			IncludeNarrative: profile.WantsNarrative,
			Mode:             "compact_dataset",
		}
	}

	matches := make([]structuredRowMatch, 0, len(filteredIndexes))
	for _, index := range filteredIndexes {
		match, ok := scoreStructuredChatRow(rows[index], profile)
		if !ok {
			continue
		}
		match.Index = index
		matches = append(matches, match)
	}

	if len(matches) == 0 {
		return structuredRowSelection{
			Indexes:          filteredIndexes,
			IncludeNarrative: profile.WantsNarrative,
			Mode:             "compact_dataset",
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score != matches[j].Score {
			return matches[i].Score > matches[j].Score
		}
		return rows[matches[i].Index].SourceRowID < rows[matches[j].Index].SourceRowID
	})

	selectedMatches := matches
	mode := "keyword_rows"
	if profile.LooksLikeEntity {
		topScore := matches[0].Score
		narrowed := make([]structuredRowMatch, 0, len(matches))
		for _, match := range matches {
			if match.Score != topScore {
				break
			}
			narrowed = append(narrowed, match)
		}
		if len(narrowed) > 0 && len(narrowed) < len(matches) {
			selectedMatches = narrowed
		}
		mode = "entity_rows"
	}

	indexes := make([]int, 0, len(selectedMatches))
	includeNarrative := profile.WantsNarrative
	for _, match := range selectedMatches {
		indexes = append(indexes, match.Index)
		if match.NarrativeHit {
			includeNarrative = true
		}
	}

	return structuredRowSelection{
		Indexes:          indexes,
		IncludeNarrative: includeNarrative,
		Mode:             mode,
	}
}

func filterStructuredRowsByCommunity(rows []cachedStructuredChatRow, communities []string) []int {
	indexes := make([]int, 0, len(rows))
	if len(communities) == 0 {
		for idx := range rows {
			indexes = append(indexes, idx)
		}
		return indexes
	}

	allowed := make(map[string]struct{}, len(communities))
	for _, community := range communities {
		normalized := normalizeChatSearchValue(community)
		if normalized == "" {
			continue
		}
		allowed[normalized] = struct{}{}
	}

	for idx, row := range rows {
		if _, ok := allowed[row.CanonicalCommunity]; ok {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

func buildChatQuestionProfile(question string) chatQuestionProfile {
	normalizedQuestion := normalizeChatSearchValue(question)
	parts := strings.Fields(normalizedQuestion)
	tokens := make([]string, 0, len(parts)*2)
	for _, part := range parts {
		if len(part) <= 1 {
			continue
		}
		if _, stopWord := chatQuestionStopWords[part]; stopWord {
			continue
		}
		tokens = append(tokens, chatQueryTokenVariants(part)...)
	}

	wantsDeathFocus := questionMentionsAny(
		normalizedQuestion,
		"death", "deaths", "deceased", "dead", "die", "died", "passed away",
		"drown", "drowned", "drowning", "grave", "obit",
	)
	wantsDeathNarrative := wantsDeathFocus && questionMentionsAny(
		normalizedQuestion,
		"when",
		"what year",
		"which year",
		"in what year",
		"in what years",
		"years",
		"how did",
		"what happened",
		"cause of death",
		"death details",
		"drown",
		"drowned",
		"drowning",
	)
	wantsAggregate := looksLikeDeterministicCountQuestion(normalizedQuestion) ||
		looksLikeDeterministicGroupSummaryQuestion(normalizedQuestion) ||
		deterministicGroupExtreme(normalizedQuestion) != "" ||
		(wantsDeathFocus && questionMentionsAny(
			normalizedQuestion,
			"what year",
			"which year",
			"in what year",
			"in what years",
			"years",
		))

	profile := chatQuestionProfile{
		NormalizedQuestion:  normalizedQuestion,
		Tokens:              uniqueChatTokens(tokens),
		LooksLikeEntity:     looksLikeEntityLookup(normalizedQuestion),
		WantsAggregate:      wantsAggregate,
		WantsNarrative:      wantsNarrativeBundle(normalizedQuestion) || wantsDeathNarrative,
		WantsDeathFocus:     wantsDeathFocus,
		WantsDeathNarrative: wantsDeathNarrative,
	}
	if len(profile.Tokens) > 0 && len(profile.Tokens) <= 4 {
		profile.KeyPhrase = strings.Join(profile.Tokens, " ")
	}
	return profile
}

func chatQueryTokenVariants(token string) []string {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}

	variants := []string{token}
	switch token {
	case "died":
		variants = append(variants, "die", "death")
	case "dies":
		variants = append(variants, "die")
	case "dead":
		variants = append(variants, "death", "deceased")
	case "deaths":
		variants = append(variants, "death")
	case "drowning", "drowned":
		variants = append(variants, "drown")
	}

	switch {
	case strings.HasSuffix(token, "ies") && len(token) > 4:
		variants = append(variants, token[:len(token)-3]+"y")
	case strings.HasSuffix(token, "s") && len(token) > 3:
		variants = append(variants, strings.TrimSuffix(token, "s"))
	}

	switch {
	case strings.HasSuffix(token, "ied") && len(token) > 4 && token != "died":
		variants = append(variants, token[:len(token)-3]+"y")
	case strings.HasSuffix(token, "ed") && len(token) > 3 && token != "died":
		variants = append(variants, strings.TrimSuffix(token, "ed"))
	}

	if strings.HasSuffix(token, "ing") && len(token) > 4 {
		variants = append(variants, strings.TrimSuffix(token, "ing"))
	}

	return uniqueChatTokens(variants)
}

func prefilterStructuredChatRows(rows []cachedStructuredChatRow, filteredIndexes []int, profile chatQuestionProfile) []int {
	if !profile.WantsDeathFocus || len(filteredIndexes) == 0 {
		return filteredIndexes
	}

	narrowed := make([]int, 0, len(filteredIndexes))
	for _, index := range filteredIndexes {
		row := rows[index]
		if row.IsDeathRecord || normalizeChatSearchValue(row.DefaultBundle.DeceasedStatus) == "yes" || row.DefaultBundle.HasDeathDetails {
			narrowed = append(narrowed, index)
		}
	}
	if len(narrowed) == 0 {
		return filteredIndexes
	}
	return narrowed
}

func selectStructuredAggregateRows(rows []cachedStructuredChatRow, filteredIndexes []int, question string) []int {
	if len(filteredIndexes) == 0 {
		return []int{}
	}

	ctx := buildDeterministicQuestionContext(rows, question)
	indexes := applyDeterministicFiltersToIndexes(rows, filteredIndexes, ctx.Filters)
	if len(indexes) == 0 {
		return filteredIndexes
	}
	return indexes
}

func scoreStructuredChatRow(row cachedStructuredChatRow, profile chatQuestionProfile) (structuredRowMatch, bool) {
	score := 0
	narrativeHit := false

	if profile.KeyPhrase != "" {
		switch {
		case containsStructuredTokenSequence(row.CanonicalName, profile.KeyPhrase):
			score += 16
		case containsStructuredTokenSequence(row.NameAliasText, profile.KeyPhrase):
			score += 14
		case containsStructuredTokenSequence(row.IdentifierText, profile.KeyPhrase):
			score += 14
		case containsStructuredTokenSequence(row.CanonicalCommunity, profile.KeyPhrase):
			score += 10
		case containsStructuredTokenSequence(row.CanonicalSchool, profile.KeyPhrase):
			score += 10
		case containsStructuredTokenSequence(row.CoreSearchText, profile.KeyPhrase):
			score += 8
		case containsStructuredTokenSequence(row.SearchText, profile.KeyPhrase):
			score += 4
			narrativeHit = row.HasNarrative
		}
	}

	for _, token := range profile.Tokens {
		switch {
		case containsStructuredToken(row.CanonicalName, token):
			score += 8
		case containsStructuredToken(row.NameAliasText, token):
			score += 7
		case containsStructuredToken(row.IdentifierText, token):
			score += 7
		case containsStructuredToken(row.CanonicalCommunity, token):
			score += 5
		case containsStructuredToken(row.CanonicalSchool, token):
			score += 5
		case containsStructuredToken(row.CoreSearchText, token):
			score += 3
		case containsStructuredToken(row.SearchText, token):
			score += 1
			narrativeHit = row.HasNarrative
		}
	}

	if score <= 0 {
		return structuredRowMatch{}, false
	}

	return structuredRowMatch{
		Score:        score,
		NarrativeHit: narrativeHit,
	}, true
}

func looksLikeEntityLookup(normalizedQuestion string) bool {
	if remainder, ok := trimEntityPrefix(normalizedQuestion, "who is "); ok {
		return !startsWithBroadFilterWord(remainder)
	}
	if remainder, ok := trimEntityPrefix(normalizedQuestion, "who was "); ok {
		return !startsWithBroadFilterWord(remainder)
	}
	entityPrefixes := []string{
		"tell me about ",
		"what do you know about ",
		"describe ",
		"give me details about ",
	}
	for _, prefix := range entityPrefixes {
		if _, ok := trimEntityPrefix(normalizedQuestion, prefix); ok {
			return true
		}
	}
	return false
}

func trimEntityPrefix(normalizedQuestion string, prefix string) (string, bool) {
	if !strings.HasPrefix(normalizedQuestion, prefix) {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(normalizedQuestion, prefix)), true
}

func startsWithBroadFilterWord(value string) bool {
	for _, prefix := range []string{"from ", "in ", "at ", "with ", "for "} {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func wantsNarrativeBundle(normalizedQuestion string) bool {
	narrativeSignals := []string{
		"tell me about",
		"what happened",
		"details",
		"detail",
		"note",
		"notes",
		"story",
		"background",
		"additional information",
		"death details",
		"why",
		"how did",
	}
	for _, signal := range narrativeSignals {
		if strings.Contains(normalizedQuestion, signal) {
			return true
		}
	}
	return false
}

func normalizeChatSearchValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}

	var builder strings.Builder
	lastSpace := false
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			builder.WriteRune(r)
			lastSpace = false
			continue
		}
		if lastSpace {
			continue
		}
		builder.WriteByte(' ')
		lastSpace = true
	}

	return strings.TrimSpace(builder.String())
}

func containsStructuredToken(haystack string, token string) bool {
	haystack = strings.TrimSpace(haystack)
	token = strings.TrimSpace(token)
	if haystack == "" || token == "" {
		return false
	}
	return strings.Contains(" "+haystack+" ", " "+token+" ")
}

func containsStructuredTokenSequence(haystack string, sequence string) bool {
	haystack = strings.TrimSpace(haystack)
	sequence = strings.TrimSpace(sequence)
	if haystack == "" || sequence == "" {
		return false
	}
	return strings.Contains(" "+haystack+" ", " "+sequence+" ")
}

func uniqueChatTokens(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
