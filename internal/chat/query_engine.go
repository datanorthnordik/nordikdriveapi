package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"mime/multipart"
	"sort"
	"strconv"
	"strings"

	f "nordik-drive-api/internal/file"
)

type chatFieldKind string

const (
	chatFieldKindText      chatFieldKind = "text"
	chatFieldKindName      chatFieldKind = "name"
	chatFieldKindDate      chatFieldKind = "date"
	chatFieldKindNumber    chatFieldKind = "number"
	chatFieldKindBoolean   chatFieldKind = "boolean"
	chatFieldKindCommunity chatFieldKind = "community"
	chatFieldKindLocation  chatFieldKind = "location"
)

type chatSchemaField struct {
	ID       string
	Label    string
	Kind     chatFieldKind
	Aliases  []string
	Examples []string
}

type chatDatasetSchema struct {
	Fields            []chatSchemaField
	fieldByID         map[string]*chatSchemaField
	fieldByLookup     map[string]*chatSchemaField
	nameFieldIDs      []string
	communityFieldIDs []string
	dateFieldIDs      []string
}

type rawChatRow struct {
	RowID   int
	RowJSON string
	Values  map[string]string
}

type chatPlannerOutput struct {
	TranscribedQuestion   string              `json:"transcribed_question,omitempty"`
	Intent                string              `json:"intent"`
	SubjectText           string              `json:"subject_text,omitempty"`
	UseSessionFocus       bool                `json:"use_session_focus,omitempty"`
	TargetFieldID         string              `json:"target_field_id,omitempty"`
	GroupByFieldID        string              `json:"group_by_field_id,omitempty"`
	GroupByGranularity    string              `json:"group_by_granularity,omitempty"`
	Filters               []chatPlannerFilter `json:"filters,omitempty"`
	SearchTerms           []string            `json:"search_terms,omitempty"`
	SortDirection         string              `json:"sort_direction,omitempty"`
	Limit                 int                 `json:"limit,omitempty"`
	ClarificationQuestion string              `json:"clarification_question,omitempty"`
}

type chatPlannerFilter struct {
	FieldID string `json:"field_id"`
	Op      string `json:"op"`
	Value   string `json:"value,omitempty"`
	Value2  string `json:"value2,omitempty"`
}

type chatSubjectMatch struct {
	Row         *cachedChatRow
	Score       float64
	MatchedName string
}

type chatVerifiedResult struct {
	Status                string           `json:"status"`
	Intent                string           `json:"intent,omitempty"`
	Question              string           `json:"question,omitempty"`
	TargetFieldID         string           `json:"target_field_id,omitempty"`
	TargetFieldLabel      string           `json:"target_field_label,omitempty"`
	FocusRowIDs           []int            `json:"-"`
	MatchedRowID          *int             `json:"-"`
	ClarificationQuestion string           `json:"clarification_question,omitempty"`
	Notes                 []string         `json:"notes,omitempty"`
	Rows                  []map[string]any `json:"rows,omitempty"`
	Value                 any              `json:"value,omitempty"`
}

const chatPlannerInstruction = `
You convert the user's latest question into a strict JSON plan for a deterministic data engine.

CRITICAL ACCURACY RULES (95%+ accuracy depends on these):
1. Be conservative: If ANY part of the question is unclear, ASK CLARIFICATION instead of guessing.
2. Never guess about implicit meaning - what's not explicitly stated should be clarified.
3. Vague terms need clarification: "recent" (when?), "young" (what age?), "many" (how many?), etc.
4. Always prefer exact field matches over vague searches.
5. If subject name is unclear or might have typos, ask before filtering.
6. For comparative questions (who had most/least), ask which specific aspect to compare.
7. Never extrapolate: if data doesn't directly answer, ask clarification not guessing.

INTENT SELECTION RULES:
- "first, earliest, oldest, youngest, latest, last, most, least, highest, lowest" → intent "extreme"
- "how many, count, total, number of, how much" → intent "count_rows"
- "which year/month/date had the most/highest/lowest deaths/admissions" → intent "group_count_extreme"
- "in what years did [event] occur" → intent "group_count_extreme" with target_field_id = date field, group by year
- "when did [event] happen" → intent "extreme" to find earliest/latest date
- "what is X of person Y, tell me X about person Y" where X is specific field → intent "field_lookup"
- "tell me about person, describe person, information about person, who is person" → intent "describe_subject"
- "list of X values, what X values are there, all X" → intent "list_values"
- Any unclear questions → intent "clarify" with specific question

SEARCH TERMS EXTRACTION (for multi-column search):
- Extract terms that describe WHAT you're searching for across all columns
- Examples:
  - "Did any children die from drowning?" → search_terms: ["drowning"]
  - "What diseases caused deaths?" → search_terms: ["disease", "illness"]
  - "Find tuberculosis records" → search_terms: ["tuberculosis"]
  - "Any poisoning or toxic cases?" → search_terms: ["poison", "toxic"]
  - "Accidents and injuries" → search_terms: ["accident", "injury"]
- Only include search_terms if searching across multiple columns is needed
- Don't include search_terms for specific person names (use subject_text instead)
- For compound terms, break into components: "drowning accident" → ["drowning", "accident"]

FILTERS vs SEARCH_TERMS (CRITICAL - this causes most failures):
- Use filters for: specific field values including LOCATION/SCHOOL NAMES
  - Examples: "Shingwauk", "Walpole Island", "Toronto", school names
  - Extract location/institution names from phrases like: "at Shingwauk", "in Toronto", "at the school"
  - Look for field values that match known locations, schools, or communities
- Use search_terms for: keywords that could appear anywhere (causes of death, conditions, descriptions)
  - Examples: "drowning", "disease", "tuberculosis", "malnutrition"
  - Only for things that might appear in text fields, not for place names
- Use subject_text for: specific person name matching

LOCATION/SCHOOL EXTRACTION (High priority):
- When question mentions a specific place (Shingwauk, Walpole Island, reserve name, etc.):
  - Find a filter from SCHEMA fields that might contain locations (kind=location, kind=community, or field about school)
  - Create a filter: {field_id: "location_field_id", op: "eq", value: "Shingwauk"}
  - Do NOT include it in search_terms
- Examples:
  - "In what years did the highest number of deaths occur at Shingwauk?"
    → intent: "group_count_extreme", filters: [{field_id: "school", op: "eq", value: "Shingwauk"}]
  - "Did any children die from drowning at Shingwauk Indian Residential School?"
    → intent: "count_rows", search_terms: ["drowning"], filters: [{field_id: "school", op: "eq", value: "Shingwauk"}]
  - "What community had the most deaths?" 
    → intent: "group_count_extreme" (no specific community filter, group BY community)

SESSION FOCUS:
- use_session_focus=true ONLY when user says: "he, she, they, that person, same person, same one, him, her, them" AND session has previous focus
- Otherwise use_session_focus=false

FIELD ID RULES:
- Use ONLY field ids from SCHEMA section - never invent
- When unsure which field user means, ask clarification not guess
- For ambiguous field names, ask which specific field

SAFETY RULES:
- Do NOT invent or guess field values
- Do NOT make assumptions about date formats without clarity
- Do NOT guess person names - use exact matching or ask
- Do NOT assume relationships between fields
- If a search term is too vague (one letter, too common), ask clarification
- If asking for "top X" but data might have ties, note that uncertainty
- Never guess about implicit data - if something isn't stated, ask not assume

ANTI-HALLUCINATION RULES (CRITICAL for 95%+ accuracy):
- Only use field IDs that exist in SCHEMA - typos here destroy accuracy
- Only use filter operations that make sense for that field kind
- If a person's name could be spelled multiple ways, ask for the exact spelling
- If a date is vague (like "March 1950"), ask for exact date or clarify it's approximate
- Never create filters for fields you're unsure about - ask clarification instead
- For comparative questions, only ask if you can identify the exact comparison fields
- If subject_text is unclear or could match multiple people, ask for clarification
- Search terms should be meaningful keywords, not random words from the question

Allowed intents:
- "describe_subject"
- "field_lookup"
- "count_rows"
- "list_values"
- "extreme"
- "group_count_extreme"
- "clarify"

Allowed filter ops:
- "eq"
- "contains"
- "before"
- "after"
- "between"
- "yes"
- "no"

Return ONLY one JSON object with this shape:
{
  "transcribed_question": "filled only if audio was provided and you had to infer speech",
  "intent": "describe_subject",
  "subject_text": "optional person name or subject phrase from the user",
  "use_session_focus": false,
  "target_field_id": "optional field id from schema",
  "group_by_field_id": "optional field id from schema",
  "group_by_granularity": "optional year, month, or day",
  "search_terms": ["optional", "keywords", "to find anywhere in data"],
  "filters": [
    {"field_id": "optional field id", "op": "eq", "value": "value", "value2": "optional second value for between"}
  ],
  "sort_direction": "asc or desc for intent extreme",
  "limit": 5,
  "clarification_question": "filled only when intent is clarify"
}
`

const chatVerifiedAnswerInstruction = `
You are a helpful assistant answering a community data question for a non-technical user.

Style requirements:
- Answer like a human: natural, warm, and conversational.
- Prefer short paragraphs over bullet points.
- Do NOT use bullet points unless the user explicitly asks for a list.
- Do NOT sound robotic or overly formal.
- Do NOT mention JSON, planner, query, row ids, columns, file name, file version, or any technical details.
- Maintain an empathetic and respectful tone, especially when discussing deaths or sensitive topics.

Hard accuracy rules (these prevent 95% of errors):
- Use ONLY facts from the VERIFIED RESULT. Do NOT add any external knowledge or guesses.
- CRITICAL: Every claim you make must be traceable to actual data in the rows provided.
- If data is incomplete or ambiguous, SAY SO explicitly. Example: "The records show X, but details about Y are not available."
- Count and verify: For count questions, manually count the rows provided and state that count.
- When describing people or records, mention each one individually with specific details from the data.
- If a value is missing, empty, or unclear in the data, do NOT guess - say "the records don't specify" or "information is not available."
- If a date is approximate, partial, or a range, describe it exactly as it appears - don't assume precision.
- For pattern questions ("did more die from X or Y?"), only answer if data directly shows the comparison.
- Never say things like "usually", "typically", "often", "tends to" based on limited data - be precise about what you see.
- If row count seems wrong for the question, flag it: "The search found only X records, which seems unexpectedly low/high for this question."
- Contradictions: If rows contain conflicting information, mention that conflict and include all versions.
- Names and identities: Always verify name matches exactly. If there are multiple similar names, mention all matches.
- Field values: If a field shows multiple values for one person, include all of them.

Conservative answer patterns:
- "Based on the records provided, [specific fact]." (traceable to data)
- "The data shows X records with [criteria]." (transparent counting)
- "I could not find [what user asked for] in the available records." (honest about limits)
- "The records include [names], but don't provide information about [what's missing]." (transparent)
- "This data is limited to [specific scope], so I cannot determine [broader question]." (scope awareness)

Output rules:
- Write only the answer text.
- Start with the direct answer in the first sentence when possible.
- Provide comprehensive information from ALL rows given to you.
`

func (cs *ChatService) ChatForUser(userID int64, question string, audioFile *multipart.FileHeader, filename string, communities []string) (*ChatResult, error) {
	if cs.DB == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	if cs.Client == nil {
		return nil, fmt.Errorf("genai client not initialized")
	}

	var file f.File
	if err := cs.DB.Select("id, filename, version, columns_order").Where("filename = ?", filename).Order("version DESC").First(&file).Error; err != nil {
		return nil, fmt.Errorf("file not found")
	}

	filteredCommunities := normalizeCommunities(communities)
	dataset, err := cs.getOrLoadChatDataset(file)
	if err != nil {
		return nil, err
	}
	rows := dataset.rowsForCommunities(filteredCommunities)
	session := cs.loadSession(userID, file.ID, filename, file.Version, filteredCommunities)

	audioBytes, audioMime, err := loadOptionalAudio(audioFile)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	plan, resolvedQuestion, _, planErr := cs.planChatRequest(ctx, question, audioBytes, audioMime, dataset, session)
	if planErr != nil {
		return nil, planErr
	}

	verified := executeChatPlan(plan, resolvedQuestion, dataset, rows, session)
	answer, err := cs.answerFromVerifiedResult(ctx, resolvedQuestion, dataset, session, verified)
	if err != nil {
		return nil, err
	}

	if verified.Status == "needs_clarification" {
		session.PendingPrompt = verified.ClarificationQuestion
	} else {
		session.PendingPrompt = ""
	}
	session.registerTurn(resolvedQuestion, answer, verified.TargetFieldID, focusIDsForSession(verified))
	cs.saveSession(userID, session)

	return &ChatResult{
		Answer:       answer,
		MatchedRowID: verified.MatchedRowID,
	}, nil
}

func loadOptionalAudio(audioFile *multipart.FileHeader) ([]byte, string, error) {
	if audioFile == nil {
		return nil, "", nil
	}
	fh, err := openMultipartFileHook(audioFile)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open audio file: %w", err)
	}
	defer fh.Close()

	audioBytes, err := readAllHook(fh)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read audio file: %w", err)
	}

	audioMime := ""
	if audioFile.Header != nil {
		audioMime = audioFile.Header.Get("Content-Type")
	}
	return audioBytes, audioMime, nil
}

func (cs *ChatService) planChatRequest(
	ctx context.Context,
	question string,
	audioBytes []byte,
	audioMime string,
	dataset *chatDatasetCacheEntry,
	session *chatSessionState,
) (chatPlannerOutput, string, string, error) {
	question = strings.TrimSpace(question)
	useSessionContext := shouldUseSessionContext(question, session)
	if question == "" && len(audioBytes) == 0 {
		return chatPlannerOutput{
			Intent:                "clarify",
			ClarificationQuestion: "What would you like to know about the data?",
		}, question, "", nil
	}

	prompt := buildChatPlannerPrompt(question, dataset, session, useSessionContext)
	raw, usedModel, err := cs.generateFromPrompt(ctx, prompt, audioBytes, audioMime)
	if err != nil {
		if len(audioBytes) == 0 {
			if fallback, ok := heuristicChatPlan(question, dataset, session); ok {
				return fallback, question, "", nil
			}
		}
		return chatPlannerOutput{}, question, usedModel, fmt.Errorf("generation error (%s): %w", usedModel, err)
	}

	plan, ok := parseChatPlannerOutput(raw, &dataset.schema)
	if !ok {
		if fallback, ok := heuristicChatPlan(question, dataset, session); ok {
			return fallback, question, usedModel, nil
		}
		return chatPlannerOutput{}, question, usedModel, fmt.Errorf("generation error (%s): failed to parse planner output", usedModel)
	}
	if heuristic, ok := heuristicChatPlan(question, dataset, session); ok {
		plan = mergePlannerWithHeuristic(plan, heuristic)
	}
	if !useSessionContext {
		plan.UseSessionFocus = false
	}

	resolvedQuestion := strings.TrimSpace(plan.TranscribedQuestion)
	if resolvedQuestion == "" {
		resolvedQuestion = question
	}
	if resolvedQuestion == "" {
		resolvedQuestion = strings.TrimSpace(plan.SubjectText)
	}
	return plan, resolvedQuestion, usedModel, nil
}

func buildChatPlannerPrompt(question string, dataset *chatDatasetCacheEntry, session *chatSessionState, useSessionContext bool) string {
	sessionSummary := "No active session context for this question."
	if useSessionContext {
		sessionSummary = session.summaryForPrompt(dataset)
	}
	return fmt.Sprintf(
		"%s\n\nSESSION CONTEXT:\n%s\n\nSCHEMA:\n%s\n\nLATEST USER QUESTION:\n%s",
		strings.TrimSpace(chatPlannerInstruction),
		sessionSummary,
		dataset.schema.describeForPlanner(),
		strings.TrimSpace(question),
	)
}

func parseChatPlannerOutput(raw string, schema *chatDatasetSchema) (chatPlannerOutput, bool) {
	jsonText := extractJSONObjectCandidate(raw)
	if jsonText == "" {
		return chatPlannerOutput{}, false
	}

	var plan chatPlannerOutput
	if err := json.Unmarshal([]byte(jsonText), &plan); err != nil {
		return chatPlannerOutput{}, false
	}
	plan.Intent = strings.TrimSpace(strings.ToLower(plan.Intent))
	switch plan.Intent {
	case "describe_subject", "field_lookup", "count_rows", "list_values", "extreme", "group_count_extreme", "clarify":
	default:
		return chatPlannerOutput{}, false
	}

	if field, ok := schema.resolveField(plan.TargetFieldID); ok {
		plan.TargetFieldID = field.ID
	} else if plan.TargetFieldID != "" {
		plan.TargetFieldID = ""
	}
	if field, ok := schema.resolveField(plan.GroupByFieldID); ok {
		plan.GroupByFieldID = field.ID
	} else if plan.GroupByFieldID != "" {
		plan.GroupByFieldID = ""
	}

	cleanFilters := make([]chatPlannerFilter, 0, len(plan.Filters))
	for _, filter := range plan.Filters {
		field, ok := schema.resolveField(filter.FieldID)
		if !ok {
			continue
		}
		filter.FieldID = field.ID
		filter.Op = strings.TrimSpace(strings.ToLower(filter.Op))
		switch filter.Op {
		case "eq", "contains", "before", "after", "between", "yes", "no":
		default:
			continue
		}
		cleanFilters = append(cleanFilters, filter)
	}
	plan.Filters = cleanFilters

	// Normalize search terms
	cleanSearchTerms := make([]string, 0, len(plan.SearchTerms))
	for _, term := range plan.SearchTerms {
		term = strings.TrimSpace(term)
		if term != "" {
			cleanSearchTerms = append(cleanSearchTerms, term)
		}
	}
	plan.SearchTerms = cleanSearchTerms

	plan.SortDirection = strings.TrimSpace(strings.ToLower(plan.SortDirection))
	if plan.SortDirection != "asc" && plan.SortDirection != "desc" {
		plan.SortDirection = ""
	}
	plan.GroupByGranularity = strings.TrimSpace(strings.ToLower(plan.GroupByGranularity))
	switch plan.GroupByGranularity {
	case "", "year", "month", "day":
	default:
		plan.GroupByGranularity = ""
	}
	if plan.Limit <= 0 || plan.Limit > 10 {
		plan.Limit = 5
	}
	if plan.Intent == "clarify" && strings.TrimSpace(plan.ClarificationQuestion) == "" {
		return chatPlannerOutput{}, false
	}
	return plan, true
}

func heuristicChatPlan(question string, dataset *chatDatasetCacheEntry, session *chatSessionState) (chatPlannerOutput, bool) {
	question = strings.TrimSpace(question)
	if question == "" {
		return chatPlannerOutput{
			Intent:                "clarify",
			ClarificationQuestion: "What would you like to know about the data?",
		}, true
	}

	qnorm := normalizeSearchText(question)
	plan := chatPlannerOutput{Limit: 5}
	useSessionContext := shouldUseSessionContext(question, session)

	if useSessionContext && hasSessionPronoun(" "+qnorm+" ") && len(session.FocusRowIDs) > 0 {
		plan.UseSessionFocus = true
	}

	if granularity, ok := detectGroupedCountGranularity(qnorm); ok {
		plan.Intent = "group_count_extreme"
		plan.GroupByGranularity = granularity
		if field := dataset.schema.bestDateFieldForQuestion(question); field != nil {
			plan.GroupByFieldID = field.ID
			plan.TargetFieldID = field.ID
		} else if field := dataset.schema.defaultExtremeField(qnorm); field != nil {
			plan.GroupByFieldID = field.ID
			plan.TargetFieldID = field.ID
		}
		plan.Filters = inferQuestionFilters(question, dataset, nil)
		return plan, true
	}

	if field := dataset.schema.bestFieldForQuestion(question); field != nil {
		plan.TargetFieldID = field.ID
	}

	switch {
	case isExistenceCountQuestion(qnorm):
		plan.Intent = "count_rows"
	case containsAny(qnorm, "how many", "count", "number of"):
		plan.Intent = "count_rows"
	case containsAny(qnorm, "earliest", "first ", " first", "oldest"):
		plan.Intent = "extreme"
		plan.SortDirection = "asc"
	case containsAny(qnorm, "latest", "last ", " last", "youngest"):
		plan.Intent = "extreme"
		plan.SortDirection = "desc"
	case containsAny(qnorm, "list ", "show all", "which ", "who "):
		if plan.TargetFieldID != "" {
			plan.Intent = "list_values"
		}
	case plan.TargetFieldID != "":
		plan.Intent = "field_lookup"
	}
	if plan.Intent == "" {
		plan.Intent = "describe_subject"
	}

	if !plan.UseSessionFocus {
		if match := dataset.bestSubjectFromQuestion(question); strings.TrimSpace(match) != "" {
			plan.SubjectText = match
		}
	}

	if plan.Intent == "extreme" && plan.TargetFieldID == "" {
		if field := dataset.schema.defaultExtremeField(qnorm); field != nil {
			plan.TargetFieldID = field.ID
		}
	}
	if plan.Intent == "field_lookup" && plan.TargetFieldID == "" && session.LastFieldID != "" {
		plan.TargetFieldID = session.LastFieldID
	}
	plan.Filters = inferQuestionFilters(question, dataset, plan.Filters)
	if len(plan.SearchTerms) == 0 {
		if plan.Intent == "count_rows" {
			plan.SearchTerms = inferSearchTerms(question, dataset, plan.Filters)
		}
	}
	if !useSessionContext {
		plan.UseSessionFocus = false
	}

	return plan, true
}

func mergePlannerWithHeuristic(primary, heuristic chatPlannerOutput) chatPlannerOutput {
	if heuristic.Intent == "group_count_extreme" {
		primary.Intent = heuristic.Intent
		if primary.GroupByFieldID == "" {
			primary.GroupByFieldID = heuristic.GroupByFieldID
		}
		if primary.GroupByGranularity == "" {
			primary.GroupByGranularity = heuristic.GroupByGranularity
		}
		if primary.TargetFieldID == "" {
			primary.TargetFieldID = heuristic.TargetFieldID
		}
	}

	if primary.Intent == "clarify" && heuristic.Intent != "" && heuristic.Intent != "clarify" && heuristic.Intent != "describe_subject" {
		primary = heuristic
	} else {
		if primary.TargetFieldID == "" {
			primary.TargetFieldID = heuristic.TargetFieldID
		}
		if primary.GroupByFieldID == "" {
			primary.GroupByFieldID = heuristic.GroupByFieldID
		}
		if primary.GroupByGranularity == "" {
			primary.GroupByGranularity = heuristic.GroupByGranularity
		}
		if primary.SubjectText == "" {
			primary.SubjectText = heuristic.SubjectText
		}
		if !primary.UseSessionFocus {
			primary.UseSessionFocus = heuristic.UseSessionFocus
		}
		if primary.SortDirection == "" {
			primary.SortDirection = heuristic.SortDirection
		}
		primary.Filters = mergePlannerFilters(primary.Filters, heuristic.Filters)
		primary.SearchTerms = mergeSearchTerms(primary.SearchTerms, heuristic.SearchTerms)
	}

	if primary.Limit <= 0 {
		primary.Limit = heuristic.Limit
	}
	if primary.Limit <= 0 {
		primary.Limit = 5
	}
	return primary
}

func mergeSearchTerms(primary, secondary []string) []string {
	if len(secondary) == 0 {
		return primary
	}
	merged := make([]string, 0, len(primary)+len(secondary))
	seen := map[string]struct{}{}
	add := func(term string) {
		term = strings.TrimSpace(term)
		key := normalizeSearchText(term)
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		merged = append(merged, term)
	}
	for _, term := range primary {
		add(term)
	}
	for _, term := range secondary {
		add(term)
	}
	return merged
}

func mergePlannerFilters(primary, secondary []chatPlannerFilter) []chatPlannerFilter {
	if len(secondary) == 0 {
		return primary
	}
	merged := make([]chatPlannerFilter, 0, len(primary)+len(secondary))
	seen := map[string]struct{}{}
	add := func(filter chatPlannerFilter) {
		key := strings.Join([]string{
			filter.FieldID,
			strings.ToLower(strings.TrimSpace(filter.Op)),
			normalizeSearchText(filter.Value),
			normalizeSearchText(filter.Value2),
		}, "\x1f")
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		merged = append(merged, filter)
	}
	for _, filter := range primary {
		add(filter)
	}
	for _, filter := range secondary {
		add(filter)
	}
	return merged
}

func (cs *ChatService) answerFromVerifiedResult(
	ctx context.Context,
	question string,
	dataset *chatDatasetCacheEntry,
	session *chatSessionState,
	verified chatVerifiedResult,
) (string, error) {
	if answer, ok := renderDeterministicAnswer(question, dataset, verified); ok {
		return answer, nil
	}
	prompt, fallback := buildVerifiedAnswerPrompt(question, dataset, session, verified)
	answer, usedModel, err := cs.generateFromPrompt(ctx, prompt, nil, "")
	if err != nil {
		if fallback != "" {
			return fallback, nil
		}
		return "", fmt.Errorf("generation error (%s): %w", usedModel, err)
	}
	answer = strings.TrimSpace(answer)
	if structured, ok := parseStructuredChatResponse(answer); ok {
		answer = strings.TrimSpace(structured.Answer)
	}
	if answer == "" {
		if fallback != "" {
			return fallback, nil
		}
		return "", fmt.Errorf("generation error (%s): empty final answer", usedModel)
	}
	return answer, nil
}

func buildVerifiedAnswerPrompt(question string, dataset *chatDatasetCacheEntry, session *chatSessionState, verified chatVerifiedResult) (string, string) {
	resultJSON, _ := json.MarshalIndent(verified, "", "  ")
	prompt := fmt.Sprintf(
		"%s\n\nUSER QUESTION:\n%s\n\nVERIFIED RESULT (only source of truth):\n%s",
		strings.TrimSpace(chatVerifiedAnswerInstruction),
		strings.TrimSpace(question),
		string(resultJSON),
	)
	return prompt, renderVerifiedResultFallback(verified)
}

func renderDeterministicAnswer(question string, dataset *chatDatasetCacheEntry, verified chatVerifiedResult) (string, bool) {
	switch verified.Status {
	case "not_found":
		if isExistenceCountQuestion(normalizeSearchText(question)) || verified.Intent == "count_rows" || verified.Intent == "group_count_extreme" {
			return "No. I couldn't find any matching records in the available data.", true
		}
		return "", false
	case "cannot_determine_exactly":
		if len(verified.Notes) > 0 {
			return "I can't determine that exactly from the available data. " + strings.Join(verified.Notes, " "), true
		}
		return "I can't determine that exactly from the available data.", true
	case "needs_clarification":
		if verified.ClarificationQuestion != "" {
			return verified.ClarificationQuestion, true
		}
		return "Could you clarify what you mean?", true
	}

	switch verified.Intent {
	case "count_rows":
		count, ok := verified.Value.(int)
		if !ok {
			return "", false
		}
		if isExistenceCountQuestion(normalizeSearchText(question)) {
			if count == 0 {
				return "No. I couldn't find any matching records in the available data.", true
			}
			return fmt.Sprintf("Yes. I found %d matching record%s in the available data.", count, pluralSuffix(count)), true
		}
		return fmt.Sprintf("I found %d matching record%s in the available data.", count, pluralSuffix(count)), true
	case "group_count_extreme":
		values, maxCount, ok := extractGroupCountSummary(verified)
		if !ok {
			return "", false
		}
		noun := groupedEventNoun(question)
		countPhrase := groupedCountPhrase(noun, maxCount)
		if len(values) == 1 {
			return fmt.Sprintf("%s had the highest number of %s, with %s.", values[0], noun, countPhrase), true
		}
		return fmt.Sprintf("%s were tied for the highest number of %s, with %s each.", joinWithAnd(values), noun, countPhrase), true
	}
	return "", false
}

func extractGroupCountSummary(verified chatVerifiedResult) ([]string, int, bool) {
	valueMap, ok := verified.Value.(map[string]any)
	if !ok {
		return nil, 0, false
	}
	maxCount, ok := valueMap["max_count"].(int)
	if !ok {
		return nil, 0, false
	}
	rawValues, ok := valueMap["winning_values"].([]string)
	if ok && len(rawValues) > 0 {
		return rawValues, maxCount, true
	}
	if rows := verified.Rows; len(rows) > 0 {
		values := make([]string, 0, len(rows))
		for _, row := range rows {
			if v, ok := row["value"].(string); ok && strings.TrimSpace(v) != "" {
				values = append(values, v)
			}
		}
		if len(values) > 0 {
			return values, maxCount, true
		}
	}
	return nil, 0, false
}

func groupedEventNoun(question string) string {
	qnorm := normalizeSearchText(question)
	switch {
	case containsAny(qnorm, "death", "deaths", "died"):
		return "deaths"
	case containsAny(qnorm, "birth", "births", "born"):
		return "births"
	case containsAny(qnorm, "admission", "admissions", "admitted", "admit"):
		return "admissions"
	case containsAny(qnorm, "discharge", "discharges", "discharged"):
		return "discharges"
	default:
		return "matching records"
	}
}

func groupedCountPhrase(noun string, count int) string {
	switch noun {
	case "deaths":
		return fmt.Sprintf("%d recorded death%s", count, pluralSuffix(count))
	case "births":
		return fmt.Sprintf("%d recorded birth%s", count, pluralSuffix(count))
	case "admissions":
		return fmt.Sprintf("%d recorded admission%s", count, pluralSuffix(count))
	case "discharges":
		return fmt.Sprintf("%d recorded discharge%s", count, pluralSuffix(count))
	default:
		return fmt.Sprintf("%d matching record%s", count, pluralSuffix(count))
	}
}

func pluralSuffix(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

func joinWithAnd(values []string) string {
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

func renderVerifiedResultFallback(verified chatVerifiedResult) string {
	switch verified.Status {
	case "needs_clarification":
		if verified.ClarificationQuestion != "" {
			return verified.ClarificationQuestion
		}
		return "Could you clarify what you mean?"
	case "not_found":
		return "I couldn't find that in the data."
	case "cannot_determine_exactly":
		if len(verified.Notes) > 0 {
			return "I can't determine that exactly from the data. " + strings.Join(verified.Notes, " ")
		}
		return "I can't determine that exactly from the data."
	case "ok":
		if str, ok := verified.Value.(string); ok && strings.TrimSpace(str) != "" {
			return strings.TrimSpace(str)
		}
		if i, ok := verified.Value.(int); ok {
			return strconv.Itoa(i)
		}
		if m, ok := verified.Value.(map[string]any); ok {
			values, _ := m["winning_values"].([]string)
			maxCount, _ := m["max_count"].(int)
			if len(values) > 0 && maxCount > 0 {
				if len(values) == 1 {
					return fmt.Sprintf("%s had the highest count, with %d matching records.", values[0], maxCount)
				}
				return fmt.Sprintf("%s had the joint highest count, with %d matching records each.", strings.Join(values, ", "), maxCount)
			}
		}
		if len(verified.Rows) > 0 {
			names := make([]string, 0, len(verified.Rows))
			for _, row := range verified.Rows {
				if name, ok := row["name"].(string); ok && strings.TrimSpace(name) != "" {
					names = append(names, name)
				}
			}
			if len(names) > 0 {
				return strings.Join(names, ", ")
			}
		}
	}
	return "I couldn't find a clear answer in the data."
}

func executeChatPlan(
	plan chatPlannerOutput,
	question string,
	dataset *chatDatasetCacheEntry,
	rows []cachedChatRow,
	session *chatSessionState,
) chatVerifiedResult {
	verified := chatVerifiedResult{
		Status:   "ok",
		Intent:   plan.Intent,
		Question: question,
	}

	if len(rows) == 0 {
		verified.Status = "not_found"
		verified.Notes = []string{"No rows match the current community filter."}
		return verified
	}

	if plan.Intent == "clarify" {
		verified.Status = "needs_clarification"
		verified.ClarificationQuestion = strings.TrimSpace(plan.ClarificationQuestion)
		return verified
	}

	candidates := rows
	isFollowUp := plan.UseSessionFocus && len(session.FocusRowIDs) > 0
	if isFollowUp {
		focused := dataset.rowsByIDs(session.FocusRowIDs, rows)
		if len(focused) > 0 {
			candidates = focused
		}
	}

	if strings.TrimSpace(plan.SubjectText) != "" {
		matches := matchSubjectRows(candidates, plan.SubjectText)
		if len(matches) == 0 {
			verified.Status = "not_found"
			verified.Notes = []string{"No matching person was found."}
			return verified
		}

		// Conservative: if top match score is very low, ask for clarification
		topScore := matches[0].Score

		// If score is very low (< 0.85), ask for confirmation
		if topScore < 0.85 {
			closeMatches := make([]chatSubjectMatch, 0)
			for _, m := range matches {
				// Include scores within 0.1 of top
				if m.Score >= topScore-0.1 {
					closeMatches = append(closeMatches, m)
				}
			}

			if len(closeMatches) > 1 {
				// Multiple potential matches - ask for clarification
				matchNames := make([]string, 0, len(closeMatches))
				for _, m := range closeMatches {
					matchNames = append(matchNames, m.Row.primaryName())
				}
				verified.Status = "needs_clarification"
				verified.ClarificationQuestion = fmt.Sprintf("Did you mean one of these: %s?", strings.Join(matchNames, ", "))
				return verified
			}
		}

		// Use all close matches (for describe_subject, might be ambiguous but still answer)
		candidates = selectSubjectRows(matches)
	}

	// Apply search terms across all columns (multi-column search)
	for _, searchTerm := range plan.SearchTerms {
		searchTerm = strings.TrimSpace(searchTerm)
		if searchTerm == "" {
			continue
		}
		searchFiltered := applyMultiColumnFilter(candidates, dataset, searchTerm)

		// If search eliminates all candidates, that's suspicious - be conservative
		if len(searchFiltered) == 0 && len(candidates) > 0 {
			// Search returned no results - mark as not found rather than continuing
			verified.Status = "not_found"
			verified.Notes = []string{fmt.Sprintf("No records found containing '%s' in available data.", searchTerm)}
			return verified
		}
		candidates = searchFiltered
	}

	for _, filter := range plan.Filters {
		prevCount := len(candidates)

		// Handle special multi-column search
		if filter.FieldID == "__multi_column__" && filter.Op == "contains_any_column" {
			filtered := applyMultiColumnFilter(candidates, dataset, filter.Value)
			if len(filtered) == 0 && prevCount > 0 {
				verified.Status = "not_found"
				verified.Notes = []string{fmt.Sprintf("No records match the filter '%s'.", filter.Value)}
				return verified
			}
			candidates = filtered
			continue
		}

		field, ok := dataset.schema.resolveField(filter.FieldID)
		if !ok {
			continue
		}

		filtered := applyFilter(candidates, field, filter)
		if len(filtered) == 0 && prevCount > 0 {
			// Filter eliminated all results - this might be a typo or wrong filter value
			verified.Status = "not_found"
			verified.Notes = []string{fmt.Sprintf("No records match the filter on '%s' with value '%s'.", field.Label, filter.Value)}
			return verified
		}
		candidates = filtered
	}

	if len(candidates) == 0 {
		verified.Status = "not_found"
		verified.TargetFieldID = plan.TargetFieldID
		if field, ok := dataset.schema.resolveField(plan.TargetFieldID); ok {
			verified.TargetFieldLabel = field.Label
		}
		return verified
	}

	if field, ok := dataset.schema.resolveField(plan.TargetFieldID); ok {
		verified.TargetFieldID = field.ID
		verified.TargetFieldLabel = field.Label
	}

	switch plan.Intent {
	case "describe_subject":
		return buildDescribeVerifiedResult(dataset, verified, candidates, isFollowUp)
	case "field_lookup":
		return buildFieldLookupVerifiedResult(dataset, verified, candidates, isFollowUp)
	case "count_rows":
		verified.Value = len(candidates)
		verified.FocusRowIDs = rowIDsFromRows(candidates)
		// Always pass all rows with all fields for accurate counting context
		verified.Rows = sampleRowsForPrompt(dataset, candidates, "", len(candidates), true)
		return verified
	case "list_values":
		return buildListValuesVerifiedResult(dataset, verified, candidates, isFollowUp)
	case "extreme":
		return buildExtremeVerifiedResult(dataset, verified, candidates, plan.SortDirection, isFollowUp)
	case "group_count_extreme":
		return buildGroupCountExtremeVerifiedResult(dataset, verified, candidates, plan, isFollowUp)
	default:
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The question could not be mapped safely.")
		return verified
	}
}

func focusIDsForSession(verified chatVerifiedResult) []int {
	if verified.Status != "ok" {
		return nil
	}
	switch verified.Intent {
	case "describe_subject", "field_lookup", "extreme":
		if len(verified.FocusRowIDs) > 0 && len(verified.FocusRowIDs) <= 4 {
			return cloneIntSlice(verified.FocusRowIDs)
		}
	}
	return nil
}

func buildDescribeVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow, isFollowUp bool) chatVerifiedResult {
	verified.FocusRowIDs = rowIDsFromRows(rows)
	// Always pass all rows with all fields for complete context
	verified.Rows = sampleRowsForPrompt(dataset, rows, "", len(rows), true)
	if len(rows) == 1 {
		rowID := rows[0].RowID
		verified.MatchedRowID = &rowID
	} else if len(rows) > 1 {
		// Multiple results - add note that these are all matches
		verified.Notes = append(verified.Notes, fmt.Sprintf("Found %d matching records. All are shown below.", len(rows)))
	}

	// Add note about any empty fields for transparency
	emptyFieldCount := 0
	for _, row := range rows {
		for _, field := range dataset.schema.Fields {
			value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
			if value == "" {
				emptyFieldCount++
			}
		}
	}
	if emptyFieldCount > 0 && len(rows) > 0 {
		avgEmpty := emptyFieldCount / (len(rows) * len(dataset.schema.Fields))
		if avgEmpty > 20 {
			verified.Notes = append(verified.Notes, "Note: Some fields in these records are empty or not available.")
		}
	}

	return verified
}

func buildFieldLookupVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow, isFollowUp bool) chatVerifiedResult {
	if verified.TargetFieldID == "" {
		verified.Status = "needs_clarification"
		verified.ClarificationQuestion = "Which detail would you like to know?"
		return verified
	}

	verified.FocusRowIDs = rowIDsFromRows(rows)
	field, ok := dataset.schema.resolveField(verified.TargetFieldID)
	if !ok {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The requested field could not be resolved.")
		return verified
	}

	// Always pass all rows with all fields for complete context
	verified.Rows = sampleRowsForPrompt(dataset, rows, field.ID, len(rows), true)
	if len(rows) == 1 {
		rowID := rows[0].RowID
		verified.MatchedRowID = &rowID
		value := strings.TrimSpace(rows[0].valueByField(field.ID, &dataset.schema))
		if value != "" {
			verified.Value = value
		}
		return verified
	}

	values := map[string]struct{}{}
	allSame := true
	firstValue := ""
	for _, row := range rows {
		v := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
		if v == "" {
			continue
		}
		values[v] = struct{}{}
		if firstValue == "" {
			firstValue = v
			continue
		}
		if v != firstValue {
			allSame = false
		}
	}
	if allSame && len(values) == 1 {
		verified.Value = firstValue
	}
	return verified
}

func buildListValuesVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow, isFollowUp bool) chatVerifiedResult {
	fieldID := verified.TargetFieldID
	if fieldID == "" {
		fieldID = dataset.schema.primaryListFieldID()
	}
	field, ok := dataset.schema.resolveField(fieldID)
	if !ok {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The list field could not be resolved.")
		return verified
	}
	verified.TargetFieldID = field.ID
	verified.TargetFieldLabel = field.Label

	uniq := map[string]struct{}{}
	values := make([]string, 0)
	emptyCount := 0
	for _, row := range rows {
		value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
		if value == "" {
			emptyCount++
			continue
		}
		if _, ok := uniq[value]; ok {
			continue
		}
		uniq[value] = struct{}{}
		values = append(values, value)
	}
	sort.Strings(values)
	verified.Value = values

	// Add note if many records have empty values
	if emptyCount > len(rows)/2 {
		verified.Notes = append(verified.Notes, fmt.Sprintf("Note: %d out of %d records have no value for %s.", emptyCount, len(rows), field.Label))
	}

	// Always pass all rows with all fields for complete context
	verified.Rows = sampleRowsForPrompt(dataset, rows, field.ID, len(rows), true)
	verified.FocusRowIDs = rowIDsFromRows(rows)
	return verified
}

func buildExtremeVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow, direction string, isFollowUp bool) chatVerifiedResult {
	if verified.TargetFieldID == "" {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The comparison field could not be resolved.")
		return verified
	}
	field, ok := dataset.schema.resolveField(verified.TargetFieldID)
	if !ok {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The comparison field could not be resolved.")
		return verified
	}

	switch field.Kind {
	case chatFieldKindDate:
		winner, tied, note := determineExtremeDate(rows, field.ID)
		if note != "" {
			verified.Status = "cannot_determine_exactly"
			verified.Notes = append(verified.Notes, note)
			return verified
		}
		if winner == nil && len(tied) == 0 {
			verified.Status = "not_found"
			return verified
		}
		if direction == "desc" {
			if winner, tied, note = determineLatestDate(rows, field.ID); note != "" {
				verified.Status = "cannot_determine_exactly"
				verified.Notes = append(verified.Notes, note)
				return verified
			}
		}
		if winner != nil {
			rows = []cachedChatRow{*winner}
			rowID := winner.RowID
			verified.MatchedRowID = &rowID
		} else {
			rows = tied
		}
	case chatFieldKindNumber:
		winnerRows := determineExtremeNumber(rows, field.ID, direction)
		if len(winnerRows) == 0 {
			verified.Status = "not_found"
			return verified
		}
		rows = winnerRows
		if len(rows) == 1 {
			rowID := rows[0].RowID
			verified.MatchedRowID = &rowID
		}
	default:
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "That kind of comparison is not supported exactly yet.")
		return verified
	}

	verified.FocusRowIDs = rowIDsFromRows(rows)
	// Always pass all rows with all fields for complete context
	verified.Rows = sampleRowsForPrompt(dataset, rows, field.ID, len(rows), true)
	if len(rows) == 1 {
		verified.Value = strings.TrimSpace(rows[0].valueByField(field.ID, &dataset.schema))
	}
	return verified
}

func buildGroupCountExtremeVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow, plan chatPlannerOutput, isFollowUp bool) chatVerifiedResult {
	groupFieldID := plan.GroupByFieldID
	if groupFieldID == "" {
		groupFieldID = verified.TargetFieldID
	}
	if groupFieldID == "" {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The grouped comparison field could not be resolved.")
		return verified
	}

	groupField, ok := dataset.schema.resolveField(groupFieldID)
	if !ok {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The grouped comparison field could not be resolved.")
		return verified
	}
	verified.TargetFieldID = groupField.ID
	verified.TargetFieldLabel = groupField.Label

	counts := map[string]int{}
	displayValues := map[string]string{}
	uncertainRows := 0
	usableRows := 0
	for _, row := range rows {
		key, display, exact, present := extractGroupCountKey(row, groupField, plan.GroupByGranularity, &dataset.schema)
		switch {
		case exact:
			counts[key]++
			if displayValues[key] == "" {
				displayValues[key] = display
			}
			usableRows++
		case present:
			uncertainRows++
		}
	}

	if len(counts) == 0 {
		if uncertainRows > 0 {
			verified.Status = "cannot_determine_exactly"
			verified.Notes = append(verified.Notes, "The available values do not identify one exact group for every matching row.")
			return verified
		}
		verified.Status = "not_found"
		return verified
	}

	type bucketCount struct {
		Key     string
		Display string
		Count   int
	}
	buckets := make([]bucketCount, 0, len(counts))
	for key, count := range counts {
		buckets = append(buckets, bucketCount{
			Key:     key,
			Display: firstNonEmpty(displayValues[key], key),
			Count:   count,
		})
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Count == buckets[j].Count {
			return buckets[i].Display < buckets[j].Display
		}
		return buckets[i].Count > buckets[j].Count
	})

	maxCount := buckets[0].Count
	winners := make([]bucketCount, 0, len(buckets))
	for _, bucket := range buckets {
		if bucket.Count != maxCount {
			break
		}
		winners = append(winners, bucket)
	}

	if uncertainRows > 0 {
		if len(winners) != 1 {
			verified.Status = "cannot_determine_exactly"
			verified.Notes = append(verified.Notes, "Some matching rows do not identify one exact group, so the exact top result could change.")
			return verified
		}
		if len(buckets) == 1 || maxCount > buckets[1].Count+uncertainRows {
			verified.Notes = append(verified.Notes, fmt.Sprintf("%d matching rows were not counted because they do not identify one exact %s.", uncertainRows, groupLabelForNotes(plan.GroupByGranularity)))
		} else {
			verified.Status = "cannot_determine_exactly"
			verified.Notes = append(verified.Notes, "Some matching rows do not identify one exact group, so the exact top result could change.")
			return verified
		}
	}

	winnerRows := make([]map[string]any, 0, len(winners))
	winnerValues := make([]string, 0, len(winners))
	for _, winner := range winners {
		winnerRows = append(winnerRows, map[string]any{
			"value": winner.Display,
			"count": winner.Count,
		})
		winnerValues = append(winnerValues, winner.Display)
	}

	verified.Rows = winnerRows
	verified.Value = map[string]any{
		"group_by":       firstNonEmpty(plan.GroupByGranularity, "value"),
		"group_field":    groupField.Label,
		"winning_values": winnerValues,
		"max_count":      maxCount,
		"counted_rows":   usableRows,
	}
	return verified
}

func extractGroupCountKey(row cachedChatRow, field *chatSchemaField, granularity string, schema *chatDatasetSchema) (key string, display string, exact bool, present bool) {
	raw := strings.TrimSpace(row.valueByField(field.ID, schema))
	if raw == "" {
		return "", "", false, false
	}
	if field.Kind != chatFieldKindDate {
		key = normalizeSearchText(raw)
		if key == "" {
			return "", "", false, false
		}
		return key, raw, true, true
	}

	tv, ok := row.temporalByField(field.ID)
	if !ok {
		return "", "", false, true
	}
	return tv.groupBucket(granularity)
}

func groupLabelForNotes(granularity string) string {
	switch granularity {
	case "day":
		return "day"
	case "month":
		return "month"
	default:
		return "year"
	}
}

func determineExtremeDate(rows []cachedChatRow, fieldID string) (*cachedChatRow, []cachedChatRow, string) {
	type candidate struct {
		row cachedChatRow
		tv  temporalValue
	}
	candidates := make([]candidate, 0, len(rows))
	for _, row := range rows {
		tv, ok := row.temporalByField(fieldID)
		if !ok || !tv.hasExactBounds() || !tv.isDeterministic() {
			continue
		}
		candidates = append(candidates, candidate{row: row, tv: tv})
	}
	if len(candidates) == 0 {
		return nil, nil, ""
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].tv.Lower.compare(*candidates[j].tv.Lower) < 0
	})
	best := candidates[0]
	tied := []cachedChatRow{best.row}
	for _, candidate := range candidates[1:] {
		if candidate.tv.Lower.compare(*best.tv.Lower) == 0 {
			tied = append(tied, candidate.row)
			continue
		}
		if candidate.tv.Lower.compare(*best.tv.Upper) < 0 {
			return nil, nil, "The date values overlap, so the exact earliest result is unclear."
		}
		break
	}
	if len(tied) == 1 {
		return &tied[0], nil, ""
	}
	return nil, tied, ""
}

func determineLatestDate(rows []cachedChatRow, fieldID string) (*cachedChatRow, []cachedChatRow, string) {
	type candidate struct {
		row cachedChatRow
		tv  temporalValue
	}
	candidates := make([]candidate, 0, len(rows))
	for _, row := range rows {
		tv, ok := row.temporalByField(fieldID)
		if !ok || !tv.hasExactBounds() || !tv.isDeterministic() {
			continue
		}
		candidates = append(candidates, candidate{row: row, tv: tv})
	}
	if len(candidates) == 0 {
		return nil, nil, ""
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].tv.Upper.compare(*candidates[j].tv.Upper) > 0
	})
	best := candidates[0]
	tied := []cachedChatRow{best.row}
	for _, candidate := range candidates[1:] {
		if candidate.tv.Upper.compare(*best.tv.Upper) == 0 {
			tied = append(tied, candidate.row)
			continue
		}
		if candidate.tv.Upper.compare(*best.tv.Lower) > 0 {
			return nil, nil, "The date values overlap, so the exact latest result is unclear."
		}
		break
	}
	if len(tied) == 1 {
		return &tied[0], nil, ""
	}
	return nil, tied, ""
}

func determineExtremeNumber(rows []cachedChatRow, fieldID, direction string) []cachedChatRow {
	best := 0.0
	found := false
	winners := make([]cachedChatRow, 0)
	for _, row := range rows {
		value, ok := parseNumericValue(row.valueByField(fieldID, nil))
		if !ok {
			continue
		}
		if !found {
			best = value
			found = true
			winners = []cachedChatRow{row}
			continue
		}
		switch {
		case direction == "desc" && value > best:
			best = value
			winners = []cachedChatRow{row}
		case direction != "desc" && value < best:
			best = value
			winners = []cachedChatRow{row}
		case value == best:
			winners = append(winners, row)
		}
	}
	return winners
}

func applyFilter(rows []cachedChatRow, field *chatSchemaField, filter chatPlannerFilter) []cachedChatRow {
	filtered := make([]cachedChatRow, 0, len(rows))
	for _, row := range rows {
		if rowMatchesFilter(row, field, filter) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// applyMultiColumnFilter searches for a keyword across all columns in all rows
func applyMultiColumnFilter(rows []cachedChatRow, dataset *chatDatasetCacheEntry, keyword string) []cachedChatRow {
	if keyword == "" {
		return rows
	}

	keywordNorm := normalizeSearchText(keyword)
	if keywordNorm == "" {
		return rows
	}

	filtered := make([]cachedChatRow, 0, len(rows))

	for _, row := range rows {
		found := false
		for _, field := range dataset.schema.Fields {
			value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
			if value == "" {
				continue
			}
			valueNorm := normalizeSearchText(value)
			if valueMatchesSearchTerm(valueNorm, keywordNorm) {
				found = true
				break
			}
		}

		if found {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

func valueMatchesSearchTerm(valueNorm, keywordNorm string) bool {
	if valueNorm == "" || keywordNorm == "" {
		return false
	}
	if strings.Contains(valueNorm, keywordNorm) || strings.Contains(keywordNorm, valueNorm) {
		return true
	}

	valueTokens := strings.Fields(valueNorm)
	keywordTokens := strings.Fields(keywordNorm)
	if len(keywordTokens) > 1 {
		for _, keywordToken := range keywordTokens {
			matched := false
			for _, valueToken := range valueTokens {
				if tokenMatchesSearchTerm(valueToken, keywordToken) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		return true
	}

	for _, valueToken := range valueTokens {
		if tokenMatchesSearchTerm(valueToken, keywordNorm) {
			return true
		}
	}
	return false
}

func tokenMatchesSearchTerm(valueToken, keywordToken string) bool {
	valueToken = strings.TrimSpace(valueToken)
	keywordToken = strings.TrimSpace(keywordToken)
	if valueToken == "" || keywordToken == "" {
		return false
	}
	if valueToken == keywordToken || strings.Contains(valueToken, keywordToken) || strings.Contains(keywordToken, valueToken) {
		return true
	}

	valueRoot := searchTokenRoot(valueToken)
	keywordRoot := searchTokenRoot(keywordToken)
	if valueRoot != "" && keywordRoot != "" && (valueRoot == keywordRoot || strings.Contains(valueRoot, keywordRoot) || strings.Contains(keywordRoot, valueRoot)) {
		return true
	}

	maxLen := maxInt(len(valueToken), len(keywordToken))
	if maxLen >= 5 && levenshteinDistance(valueToken, keywordToken) <= 1 {
		return true
	}
	if maxLen >= 7 && valueRoot != "" && keywordRoot != "" && levenshteinDistance(valueRoot, keywordRoot) <= 1 {
		return true
	}
	return false
}

func searchTokenRoot(token string) string {
	token = strings.TrimSpace(token)
	if len(token) <= 4 {
		return token
	}
	suffixes := []string{"ingly", "edly", "ation", "ition", "ments", "ment", "ings", "ness", "tion", "sion", "edly", "edly", "ing", "ers", "ies", "ied", "est", "ous", "ive", "ful", "ed", "es", "s"}
	root := token
	for _, suffix := range suffixes {
		if len(root) > len(suffix)+2 && strings.HasSuffix(root, suffix) {
			root = strings.TrimSuffix(root, suffix)
			break
		}
	}
	if strings.HasSuffix(root, "i") && len(root) > 3 {
		root = strings.TrimSuffix(root, "i") + "y"
	}
	return root
}

func rowMatchesFilter(row cachedChatRow, field *chatSchemaField, filter chatPlannerFilter) bool {
	raw := row.valueByField(field.ID, nil)
	switch field.Kind {
	case chatFieldKindBoolean:
		switch filter.Op {
		case "yes":
			return isTruthy(raw)
		case "no":
			return isFalsy(raw)
		}
	case chatFieldKindNumber:
		value, ok := parseNumericValue(raw)
		if !ok {
			return false
		}
		target, ok := parseNumericValue(filter.Value)
		if !ok {
			return false
		}
		switch filter.Op {
		case "eq":
			return value == target
		case "before":
			return value < target
		case "after":
			return value > target
		case "between":
			target2, ok := parseNumericValue(filter.Value2)
			return ok && value >= math.Min(target, target2) && value <= math.Max(target, target2)
		case "contains":
			return strings.Contains(normalizeSearchText(raw), normalizeSearchText(filter.Value))
		}
	case chatFieldKindDate:
		tv, ok := row.temporalByField(field.ID)
		if !ok {
			return false
		}
		switch filter.Op {
		case "eq":
			target := parseTemporalValue(filter.Value)
			return target.hasExactBounds() && tv.definiteWithinRange(*target.Lower, *target.Upper)
		case "before":
			target := parseTemporalValue(filter.Value)
			return target.hasExactBounds() && tv.definiteBefore(*target.Lower)
		case "after":
			target := parseTemporalValue(filter.Value)
			return target.hasExactBounds() && tv.definiteAfter(*target.Upper)
		case "between":
			left := parseTemporalValue(filter.Value)
			right := parseTemporalValue(filter.Value2)
			return left.hasExactBounds() && right.hasExactBounds() && tv.definiteWithinRange(*left.Lower, *right.Upper)
		case "contains":
			return strings.Contains(normalizeSearchText(raw), normalizeSearchText(filter.Value))
		}
	default:
		switch filter.Op {
		case "eq":
			return normalizeSearchText(raw) == normalizeSearchText(filter.Value)
		case "contains":
			return strings.Contains(normalizeSearchText(raw), normalizeSearchText(filter.Value))
		case "yes":
			return isTruthy(raw)
		case "no":
			return isFalsy(raw)
		}
	}
	return false
}

func matchSubjectRows(rows []cachedChatRow, subject string) []chatSubjectMatch {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return nil
	}

	subjectNorm := normalizeSearchText(subject)
	if subjectNorm == "" {
		return nil
	}

	matches := make([]chatSubjectMatch, 0)
	for i := range rows {
		bestScore := 0.0
		bestName := ""
		for _, candidateName := range rows[i].Names {
			score := scoreNameMatch(subjectNorm, normalizeSearchText(candidateName))
			if score > bestScore {
				bestScore = score
				bestName = candidateName
			}
		}
		// Lowered threshold from 0.72 to 0.65 to catch spelling variations, abbreviations, and name aliases
		// The ambiguity check below will ask for clarification if there are multiple close matches
		if bestScore >= 0.65 {
			rowCopy := rows[i]
			matches = append(matches, chatSubjectMatch{
				Row:         &rowCopy,
				Score:       bestScore,
				MatchedName: bestName,
			})
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score == matches[j].Score {
			return matches[i].Row.primaryName() < matches[j].Row.primaryName()
		}
		return matches[i].Score > matches[j].Score
	})
	return matches
}

func selectSubjectRows(matches []chatSubjectMatch) []cachedChatRow {
	if len(matches) == 0 {
		return nil
	}
	top := matches[0].Score
	selected := make([]cachedChatRow, 0)
	for _, match := range matches {
		if match.Score+0.03 < top {
			break
		}
		selected = append(selected, *match.Row)
		if len(selected) >= 4 {
			break
		}
	}
	return selected
}

func sampleRowsForPrompt(dataset *chatDatasetCacheEntry, rows []cachedChatRow, targetFieldID string, limit int, includeAll bool) []map[string]any {
	if limit <= 0 || limit > len(rows) {
		limit = len(rows)
	}
	out := make([]map[string]any, 0, limit)
	for idx := 0; idx < limit; idx++ {
		out = append(out, dataset.rowPromptData(rows[idx], targetFieldID, includeAll))
	}
	return out
}

func rowIDsFromRows(rows []cachedChatRow) []int {
	out := make([]int, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.RowID)
	}
	return out
}

func (cs *ChatService) getOrLoadChatDataset(file f.File) (*chatDatasetCacheEntry, error) {
	cacheKey := chatDatasetCacheKey(file.ID, file.Version)
	if cached, ok := cs.datasetCache.Load(cacheKey); ok {
		if entry, ok := cached.(*chatDatasetCacheEntry); ok {
			return entry, nil
		}
	}

	var rawRowsDB []f.FileData
	if err := cs.DB.Select("id, row_data").
		Where("file_id = ? AND version = ?", file.ID, file.Version).
		Order("id ASC").
		Find(&rawRowsDB).Error; err != nil {
		return nil, fmt.Errorf("file data not found: %w", err)
	}

	rawRows := make([]rawChatRow, 0, len(rawRowsDB))
	for _, rawRow := range rawRowsDB {
		rowMap, err := rowJSONToStrings(rawRow.RowData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal file data: invalid row json")
		}
		rawRows = append(rawRows, rawChatRow{
			RowID:   int(rawRow.ID),
			RowJSON: string(rawRow.RowData),
			Values:  rowMap,
		})
	}

	columns := extractOrderedColumns(file.ColumnsOrder, rawRows)
	schema := buildChatSchema(columns, rawRows)
	rows := make([]cachedChatRow, 0, len(rawRows))
	rowByID := make(map[int]*cachedChatRow, len(rawRows))
	for _, rawRow := range rawRows {
		row := buildCachedChatRow(rawRow, &schema)
		rows = append(rows, row)
		rowByID[row.RowID] = &rows[len(rows)-1]
	}

	entry := &chatDatasetCacheEntry{
		rows:    rows,
		rowByID: rowByID,
		schema:  schema,
	}
	actual, _ := cs.datasetCache.LoadOrStore(cacheKey, entry)
	if cached, ok := actual.(*chatDatasetCacheEntry); ok {
		return cached, nil
	}
	return entry, nil
}

func rowJSONToStrings(raw []byte) (map[string]string, error) {
	var anyMap map[string]any
	if err := json.Unmarshal(raw, &anyMap); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(anyMap))
	for key, value := range anyMap {
		out[key] = stringifyRowValue(value)
	}
	return out, nil
}

func stringifyRowValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case float64:
		if math.Trunc(v) == v {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

func extractOrderedColumns(columnsJSON []byte, rows []rawChatRow) []string {
	var columns []string
	if len(strings.TrimSpace(string(columnsJSON))) > 0 {
		_ = json.Unmarshal(columnsJSON, &columns)
	}
	if len(columns) > 0 {
		return columns
	}
	seen := map[string]struct{}{}
	for _, row := range rows {
		keys := make([]string, 0, len(row.Values))
		for key := range row.Values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			columns = append(columns, key)
		}
	}
	return columns
}

func buildChatSchema(columns []string, rows []rawChatRow) chatDatasetSchema {
	schema := chatDatasetSchema{
		Fields:        make([]chatSchemaField, 0, len(columns)),
		fieldByID:     map[string]*chatSchemaField{},
		fieldByLookup: map[string]*chatSchemaField{},
	}

	usedIDs := map[string]int{}
	for _, column := range columns {
		fieldID := uniqueFieldID(column, usedIDs)
		kind := inferFieldKind(column, rows)
		field := chatSchemaField{
			ID:       fieldID,
			Label:    column,
			Kind:     kind,
			Aliases:  inferFieldAliases(column, kind),
			Examples: collectFieldExamples(column, rows, kind),
		}
		schema.Fields = append(schema.Fields, field)
		if kind == chatFieldKindName {
			schema.nameFieldIDs = append(schema.nameFieldIDs, fieldID)
		}
		if kind == chatFieldKindCommunity {
			schema.communityFieldIDs = append(schema.communityFieldIDs, fieldID)
		}
		if kind == chatFieldKindDate {
			schema.dateFieldIDs = append(schema.dateFieldIDs, fieldID)
		}
	}

	for idx := range schema.Fields {
		field := &schema.Fields[idx]
		schema.fieldByID[field.ID] = field
		schema.fieldByLookup[normalizeSearchText(field.ID)] = field
		schema.fieldByLookup[normalizeSearchText(field.Label)] = field
		for _, alias := range field.Aliases {
			schema.fieldByLookup[normalizeSearchText(alias)] = field
		}
	}
	return schema
}

func uniqueFieldID(label string, used map[string]int) string {
	base := slugify(label)
	if base == "" {
		base = "field"
	}
	used[base]++
	if used[base] == 1 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, used[base])
}

func inferFieldKind(label string, rows []rawChatRow) chatFieldKind {
	lower := strings.ToLower(strings.TrimSpace(label))
	switch {
	case strings.Contains(lower, "deceased"):
		return chatFieldKindBoolean
	case strings.Contains(lower, "community"), strings.Contains(lower, "reserve"), strings.Contains(lower, "first nation"), strings.Contains(lower, "home"):
		return chatFieldKindCommunity
	case strings.Contains(lower, "name") && !strings.Contains(lower, "parent") && !strings.Contains(lower, "sibling"):
		return chatFieldKindName
	case strings.Contains(lower, "date"), strings.Contains(lower, "birth"), strings.Contains(lower, "death"), strings.Contains(lower, "burial"), strings.Contains(lower, "admit"), strings.Contains(lower, "discharge"):
		return chatFieldKindDate
	case strings.Contains(lower, "age"), strings.Contains(lower, "number"), lower == "no.", lower == "no":
		return chatFieldKindNumber
	case strings.Contains(lower, "place"), strings.Contains(lower, "location"), strings.Contains(lower, "school"):
		return chatFieldKindLocation
	}

	sampleCount := 0
	dateCount := 0
	numberCount := 0
	boolCount := 0
	for _, row := range rows {
		value := strings.TrimSpace(row.Values[label])
		if value == "" {
			continue
		}
		sampleCount++
		parsedDate := parseTemporalValue(value)
		if parsedDate.Kind != temporalKindMalformed && parsedDate.Kind != temporalKindUnknown {
			dateCount++
		}
		if _, ok := parseNumericValue(value); ok {
			numberCount++
		}
		if isTruthy(value) || isFalsy(value) {
			boolCount++
		}
		if sampleCount >= 20 {
			break
		}
	}

	switch {
	case boolCount >= 4:
		return chatFieldKindBoolean
	case dateCount >= 4:
		return chatFieldKindDate
	case numberCount >= 4:
		return chatFieldKindNumber
	default:
		return chatFieldKindText
	}
}

func collectFieldExamples(label string, rows []rawChatRow, kind chatFieldKind) []string {
	counts := map[string]int{}
	for _, row := range rows {
		value := strings.TrimSpace(row.Values[label])
		if value == "" {
			continue
		}
		counts[value]++
	}
	if len(counts) == 0 {
		return nil
	}
	type pair struct {
		value string
		count int
	}
	items := make([]pair, 0, len(counts))
	for value, count := range counts {
		items = append(items, pair{value: value, count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].value < items[j].value
		}
		return items[i].count > items[j].count
	})

	limit := 5
	if kind == chatFieldKindCommunity {
		limit = 8
	}
	if len(items) < limit {
		limit = len(items)
	}
	examples := make([]string, 0, limit)
	for _, item := range items[:limit] {
		examples = append(examples, item.value)
	}
	return examples
}

func inferFieldAliases(label string, kind chatFieldKind) []string {
	lower := strings.ToLower(strings.TrimSpace(label))
	aliases := []string{label, lower}
	add := func(values ...string) {
		aliases = append(aliases, values...)
	}

	switch {
	case strings.Contains(lower, "date of death"):
		add("death date", "date died", "when died", "died", "death", "deaths")
	case strings.Contains(lower, "death details"):
		add("death details", "details of death", "how died")
	case strings.Contains(lower, "date of birth"):
		add("birth date", "when born", "born", "birth", "births")
	case strings.Contains(lower, "date first admitted"), strings.Contains(lower, "admitted"):
		add("admitted", "admission date", "when admitted")
	case strings.Contains(lower, "date of discharge"), strings.Contains(lower, "discharged"):
		add("discharge date", "when discharged", "left school")
	case strings.Contains(lower, "place of burial"), strings.Contains(lower, "burial"):
		add("burial place", "where buried", "buried")
	case strings.Contains(lower, "place of death"):
		add("where died", "death place", "place died")
	case strings.Contains(lower, "location of death"):
		add("death location", "location died")
	case strings.Contains(lower, "cause of death"):
		add("cause", "how died", "death cause")
	case strings.Contains(lower, "community"), strings.Contains(lower, "reserve"), strings.Contains(lower, "first nation"), strings.Contains(lower, "home"):
		add("community", "reserve", "first nation", "home")
	case strings.Contains(lower, "school"):
		add("school", "institution")
	case strings.Contains(lower, "notes"), strings.Contains(lower, "comments"), strings.Contains(lower, "information"):
		add("notes", "comments", "details")
	case strings.Contains(lower, "age"):
		add("age", "age at death")
	case strings.Contains(lower, "deceased"):
		add("deceased", "dead", "died")
	}

	switch kind {
	case chatFieldKindName:
		add("name", "person", "student")
	case chatFieldKindDate:
		add("date")
	}

	uniq := map[string]struct{}{}
	out := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		key := normalizeSearchText(alias)
		if key == "" {
			continue
		}
		if _, ok := uniq[key]; ok {
			continue
		}
		uniq[key] = struct{}{}
		out = append(out, alias)
	}
	return out
}

func buildCachedChatRow(rawRow rawChatRow, schema *chatDatasetSchema) cachedChatRow {
	row := cachedChatRow{
		RowID:      rawRow.RowID,
		RowJSON:    rawRow.RowJSON,
		Values:     rawRow.Values,
		Normalized: map[string]string{},
		Temporal:   map[string]temporalValue{},
	}

	for _, field := range schema.Fields {
		value := strings.TrimSpace(rawRow.Values[field.Label])
		row.Normalized[field.ID] = normalizeSearchText(value)
		if field.Kind == chatFieldKindDate && value != "" {
			row.Temporal[field.ID] = parseTemporalValue(value)
		}
	}

	row.Names = collectRowNames(row, schema)
	for _, fieldID := range schema.communityFieldIDs {
		if value := strings.TrimSpace(row.valueByField(fieldID, schema)); value != "" {
			row.Community = value
			break
		}
	}
	return row
}

func collectRowNames(row cachedChatRow, schema *chatDatasetSchema) []string {
	names := []string{}
	firstName := ""
	lastName := ""
	for _, fieldID := range schema.nameFieldIDs {
		field := schema.fieldByID[fieldID]
		if field == nil {
			continue
		}
		value := strings.TrimSpace(row.Values[field.Label])
		if value == "" {
			continue
		}
		lower := strings.ToLower(field.Label)
		switch {
		case strings.Contains(lower, "first"):
			firstName = value
		case strings.Contains(lower, "last"):
			lastName = value
		default:
			names = append(names, value)
		}
	}
	if firstName != "" || lastName != "" {
		full := strings.TrimSpace(strings.Join([]string{firstName, lastName}, " "))
		if full != "" {
			names = append([]string{full}, names...)
		}
	}
	uniq := map[string]struct{}{}
	out := make([]string, 0, len(names))
	for _, name := range names {
		key := normalizeSearchText(name)
		if key == "" {
			continue
		}
		if _, ok := uniq[key]; ok {
			continue
		}
		uniq[key] = struct{}{}
		out = append(out, strings.TrimSpace(name))
	}
	return out
}

func (schema *chatDatasetSchema) resolveField(ref string) (*chatSchemaField, bool) {
	ref = normalizeSearchText(ref)
	if ref == "" {
		return nil, false
	}
	field, ok := schema.fieldByLookup[ref]
	return field, ok
}

func (schema *chatDatasetSchema) bestFieldForQuestion(question string) *chatSchemaField {
	qnorm := normalizeSearchText(question)
	bestScore := 0
	var best *chatSchemaField
	for idx := range schema.Fields {
		field := &schema.Fields[idx]
		score := 0
		for _, alias := range field.Aliases {
			aliasNorm := normalizeSearchText(alias)
			if aliasNorm == "" {
				continue
			}
			if strings.Contains(qnorm, aliasNorm) && len(aliasNorm) > score {
				score = len(aliasNorm)
			}
		}
		if score > bestScore {
			bestScore = score
			best = field
		}
	}
	return best
}

func (schema *chatDatasetSchema) bestDateFieldForQuestion(question string) *chatSchemaField {
	qnorm := normalizeSearchText(question)
	bestScore := 0
	var best *chatSchemaField
	for _, fieldID := range schema.dateFieldIDs {
		field := schema.fieldByID[fieldID]
		if field == nil {
			continue
		}

		score := 0
		for _, alias := range field.Aliases {
			aliasNorm := normalizeSearchText(alias)
			if aliasNorm == "" {
				continue
			}
			if strings.Contains(qnorm, aliasNorm) {
				score = maxInt(score, len(aliasNorm)+5)
			}
		}

		blob := fieldSearchBlob(field)
		switch {
		case containsAny(qnorm, "death", "deaths", "died") && containsAny(blob, "death", "died"):
			score += 50
		case containsAny(qnorm, "birth", "births", "born") && containsAny(blob, "birth", "born"):
			score += 50
		case containsAny(qnorm, "burial", "buried") && containsAny(blob, "burial", "buried"):
			score += 40
		case containsAny(qnorm, "admission", "admissions", "admitted", "admit") && containsAny(blob, "admission", "admitted", "admit"):
			score += 40
		case containsAny(qnorm, "discharge", "discharges", "discharged") && containsAny(blob, "discharge", "discharged"):
			score += 40
		}
		if containsAny(qnorm, "year", "years", "month", "months", "day", "days", "date", "dates") {
			score += 2
		}
		if score > bestScore {
			bestScore = score
			best = field
		}
	}
	if best != nil {
		return best
	}
	if fallback := schema.defaultExtremeField(qnorm); fallback != nil && fallback.Kind == chatFieldKindDate {
		return fallback
	}
	return nil
}

func fieldSearchBlob(field *chatSchemaField) string {
	if field == nil {
		return ""
	}
	parts := []string{field.ID, field.Label}
	parts = append(parts, field.Aliases...)
	return normalizeSearchText(strings.Join(parts, " "))
}

func (schema *chatDatasetSchema) defaultExtremeField(questionNorm string) *chatSchemaField {
	if containsAny(questionNorm, "oldest", "youngest") {
		for idx := range schema.Fields {
			field := &schema.Fields[idx]
			if field.Kind == chatFieldKindNumber && strings.Contains(strings.ToLower(field.Label), "age") {
				return field
			}
		}
	}
	for _, fieldID := range schema.dateFieldIDs {
		field := schema.fieldByID[fieldID]
		if field == nil {
			continue
		}
		label := strings.ToLower(field.Label)
		if strings.Contains(label, "death") || strings.Contains(label, "birth") {
			return field
		}
	}
	if len(schema.dateFieldIDs) > 0 {
		return schema.fieldByID[schema.dateFieldIDs[0]]
	}
	return nil
}

func (schema *chatDatasetSchema) primaryListFieldID() string {
	if len(schema.nameFieldIDs) > 0 {
		return schema.nameFieldIDs[0]
	}
	if len(schema.communityFieldIDs) > 0 {
		return schema.communityFieldIDs[0]
	}
	if len(schema.Fields) > 0 {
		return schema.Fields[0].ID
	}
	return ""
}

func inferQuestionFilters(question string, dataset *chatDatasetCacheEntry, existing []chatPlannerFilter) []chatPlannerFilter {
	qnorm := normalizeSearchText(question)
	if qnorm == "" {
		return existing
	}

	filters := make([]chatPlannerFilter, 0, len(existing)+1)
	filters = append(filters, existing...)
	seenFields := map[string]struct{}{}
	for _, filter := range existing {
		seenFields[filter.FieldID] = struct{}{}
	}

	type inferredFilterCandidate struct {
		field *chatSchemaField
		value string
		score float64
	}
	best := inferredFilterCandidate{}

	for idx := range dataset.schema.Fields {
		field := &dataset.schema.Fields[idx]
		if _, ok := seenFields[field.ID]; ok {
			continue
		}
		if !fieldSupportsValueInference(field) {
			continue
		}
		if value, score, ok := dataset.bestFilterValueForQuestion(field, qnorm); ok {
			score += filterFieldPreference(field, qnorm)
			if score > best.score {
				best = inferredFilterCandidate{field: field, value: value, score: score}
			}
		}
	}
	if best.field != nil {
		filters = append(filters, chatPlannerFilter{
			FieldID: best.field.ID,
			Op:      "eq",
			Value:   best.value,
		})
	}

	return filters
}

func fieldSupportsValueInference(field *chatSchemaField) bool {
	if field == nil {
		return false
	}
	switch field.Kind {
	case chatFieldKindCommunity, chatFieldKindLocation:
		return true
	case chatFieldKindText:
		blob := fieldSearchBlob(field)
		return containsAny(blob, "school", "institution", "community", "reserve", "location", "place")
	default:
		return false
	}
}

func filterFieldPreference(field *chatSchemaField, questionNorm string) float64 {
	if field == nil {
		return 0
	}
	blob := fieldSearchBlob(field)
	score := 0.0
	switch field.Kind {
	case chatFieldKindLocation:
		score += 0.10
	case chatFieldKindCommunity:
		score += 0.05
	}
	if containsAny(questionNorm, "school", "residential school", "institution") && containsAny(blob, "school", "institution", "residential") {
		score += 0.35
	}
	if containsAny(questionNorm, "community", "reserve", "home") && containsAny(blob, "community", "reserve", "home") {
		score += 0.25
	}
	if containsAny(questionNorm, "at ", " in ") && containsAny(blob, "school", "location", "place", "institution") {
		score += 0.10
	}
	return score
}

func inferSearchTerms(question string, dataset *chatDatasetCacheEntry, filters []chatPlannerFilter) []string {
	qnorm := normalizeSearchText(question)
	if qnorm == "" {
		return nil
	}

	excluded := map[string]struct{}{}
	for _, filter := range filters {
		field, ok := dataset.schema.resolveField(filter.FieldID)
		if !ok {
			continue
		}
		addNormalizedPhraseTokens(excluded, field.Label)
		addNormalizedPhraseTokens(excluded, filter.Value)
		addNormalizedPhraseTokens(excluded, filter.Value2)
	}
	for _, field := range dataset.schema.Fields {
		addNormalizedPhraseTokens(excluded, field.Label)
		for _, alias := range field.Aliases {
			addNormalizedPhraseTokens(excluded, alias)
		}
	}

	stopWords := map[string]struct{}{
		"a": {}, "about": {}, "admissions": {}, "admitted": {}, "after": {}, "all": {}, "an": {}, "and": {}, "any": {}, "are": {}, "at": {},
		"before": {}, "birth": {}, "births": {}, "by": {}, "child": {}, "children": {}, "community": {}, "count": {}, "date": {}, "dates": {},
		"death": {}, "deaths": {}, "describe": {}, "did": {}, "do": {}, "does": {}, "during": {}, "earliest": {}, "find": {}, "first": {},
		"for": {}, "from": {}, "had": {}, "has": {}, "have": {}, "highest": {}, "how": {}, "in": {}, "indian": {}, "information": {},
		"institution": {}, "is": {}, "it": {}, "its": {}, "last": {}, "latest": {}, "least": {}, "list": {}, "many": {}, "month": {}, "months": {},
		"most": {}, "much": {}, "number": {}, "occur": {}, "occurred": {}, "of": {}, "on": {}, "or": {}, "records": {}, "residential": {},
		"same": {}, "school": {}, "show": {}, "student": {}, "students": {}, "tell": {}, "that": {}, "the": {}, "their": {}, "there": {},
		"these": {}, "they": {}, "this": {}, "those": {}, "total": {}, "was": {}, "were": {}, "what": {}, "when": {}, "where": {}, "which": {},
		"who": {}, "with": {}, "year": {}, "years": {},
	}

	terms := make([]string, 0, 3)
	seen := map[string]struct{}{}
	for _, token := range strings.Fields(qnorm) {
		if len(token) < 4 {
			continue
		}
		if _, ok := stopWords[token]; ok {
			continue
		}
		if _, ok := excluded[token]; ok {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		terms = append(terms, token)
		if len(terms) >= 3 {
			break
		}
	}
	return terms
}

func addNormalizedPhraseTokens(dest map[string]struct{}, value string) {
	for _, token := range strings.Fields(normalizeSearchText(value)) {
		if token == "" {
			continue
		}
		dest[token] = struct{}{}
	}
}

func (schema *chatDatasetSchema) describeForPlanner() string {
	lines := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		line := fmt.Sprintf("- id=%s | label=%s | kind=%s", field.ID, field.Label, field.Kind)
		if len(field.Aliases) > 0 {
			line += " | aliases=" + strings.Join(field.Aliases[:minInt(len(field.Aliases), 5)], ", ")
		}
		if len(field.Examples) > 0 {
			line += " | examples=" + strings.Join(field.Examples[:minInt(len(field.Examples), 5)], "; ")
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (dataset *chatDatasetCacheEntry) rowsForCommunities(communities []string) []cachedChatRow {
	if len(communities) == 0 {
		return dataset.rows
	}
	allowed := map[string]struct{}{}
	for _, community := range communities {
		allowed[normalizeSearchText(community)] = struct{}{}
	}
	out := make([]cachedChatRow, 0, len(dataset.rows))
	for _, row := range dataset.rows {
		if _, ok := allowed[normalizeSearchText(row.Community)]; ok {
			out = append(out, row)
		}
	}
	return out
}

func (dataset *chatDatasetCacheEntry) bestFilterValueForQuestion(field *chatSchemaField, questionNorm string) (string, float64, bool) {
	if dataset == nil || field == nil || questionNorm == "" {
		return "", 0, false
	}

	bestRaw := ""
	bestScore := 0.0
	seen := map[string]struct{}{}

	// First try field examples
	for _, example := range field.Examples {
		raw := strings.TrimSpace(example)
		norm := normalizeSearchText(raw)
		if len(norm) < 3 {
			continue
		}
		seen[norm] = struct{}{}

		score := scoreLocationMatch(questionNorm, norm)
		if score > bestScore {
			bestScore = score
			bestRaw = raw
		}
	}

	// Then try dataset values
	for _, row := range dataset.rows {
		raw := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
		norm := strings.TrimSpace(row.Normalized[field.ID])
		if len(norm) < 3 || len(norm) > 80 {
			continue
		}
		if _, ok := seen[norm]; ok {
			continue
		}
		seen[norm] = struct{}{}

		score := scoreLocationMatch(questionNorm, norm)
		if score > bestScore {
			bestScore = score
			bestRaw = raw
		}
	}

	if bestScore > 0.5 {
		return bestRaw, bestScore, true
	}
	return "", 0, false
}

// scoreLocationMatch checks if a normalized location appears in the question
// It handles multi-word location names by checking component words
func scoreLocationMatch(questionNorm string, locationNorm string) float64 {
	if strings.Contains(questionNorm, locationNorm) {
		// Exact substring match - highest score
		return 1.0
	}

	// Check if any significant word from location matches in question
	locationWords := strings.Fields(locationNorm)
	if len(locationWords) == 0 {
		return 0.0
	}

	matchedWords := 0
	for _, word := range locationWords {
		if len(word) >= 4 && strings.Contains(questionNorm, word) {
			matchedWords++
		}
	}

	// Score based on fraction of words matched
	// For "Shingwauk Indian Residential School", if "shingwauk" matches, that's 1/4 = 0.25
	// But that might be enough if it's the most specific word
	if matchedWords > 0 {
		score := float64(matchedWords) / float64(len(locationWords))
		// Boost score for first/primary word (usually the name)
		if strings.Contains(questionNorm, locationWords[0]) {
			score += 0.2
		}
		return score
	}

	return 0.0
}

func (dataset *chatDatasetCacheEntry) rowsByIDs(ids []int, within []cachedChatRow) []cachedChatRow {
	if len(ids) == 0 {
		return nil
	}
	allowed := map[int]struct{}{}
	for _, id := range ids {
		allowed[id] = struct{}{}
	}
	out := make([]cachedChatRow, 0, len(ids))
	for _, row := range within {
		if _, ok := allowed[row.RowID]; ok {
			out = append(out, row)
		}
	}
	return out
}

func (dataset *chatDatasetCacheEntry) rowPromptData(row cachedChatRow, targetFieldID string, includeAll bool) map[string]any {
	out := map[string]any{
		"row_id": row.RowID,
	}
	if name := row.primaryName(); name != "" {
		out["name"] = name
	}

	if targetFieldID != "" {
		if field, ok := dataset.schema.resolveField(targetFieldID); ok {
			raw := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
			out["target_field"] = field.Label
			out["target_value"] = raw
			if tv, ok := row.temporalByField(field.ID); ok {
				out["target_value_meta"] = renderTemporalMetadata(tv)
			}
		}
	}

	if includeAll {
		fields := map[string]any{}
		for _, field := range dataset.schema.Fields {
			value := strings.TrimSpace(row.Values[field.Label])
			if value == "" {
				continue
			}
			fields[field.Label] = value
			if tv, ok := row.temporalByField(field.ID); ok {
				fields[field.Label+"__meta"] = renderTemporalMetadata(tv)
			}
		}
		out["fields"] = fields
	}
	return out
}

func (dataset *chatDatasetCacheEntry) bestSubjectFromQuestion(question string) string {
	qnorm := normalizeSearchText(question)
	best := ""
	for _, row := range dataset.rows {
		for _, name := range row.Names {
			nameNorm := normalizeSearchText(name)
			if nameNorm == "" || len(nameNorm) < 4 {
				continue
			}
			if strings.Contains(qnorm, nameNorm) && len(nameNorm) > len(best) {
				best = name
			}
		}
	}
	return best
}

func (row cachedChatRow) valueByField(fieldID string, schema *chatDatasetSchema) string {
	if schema != nil {
		if field, ok := schema.resolveField(fieldID); ok {
			return strings.TrimSpace(row.Values[field.Label])
		}
	}
	for label := range row.Values {
		if slugify(label) == fieldID || normalizeSearchText(label) == normalizeSearchText(fieldID) {
			return strings.TrimSpace(row.Values[label])
		}
	}
	return ""
}

func (row cachedChatRow) temporalByField(fieldID string) (temporalValue, bool) {
	tv, ok := row.Temporal[fieldID]
	return tv, ok
}

func (row cachedChatRow) primaryName() string {
	if len(row.Names) == 0 {
		return ""
	}
	return strings.TrimSpace(row.Names[0])
}

func normalizeSearchText(v string) string {
	var b strings.Builder
	lastSpace := true
	for _, r := range strings.ToLower(strings.TrimSpace(v)) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastSpace = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastSpace = false
		default:
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func slugify(v string) string {
	return strings.ReplaceAll(normalizeSearchText(v), " ", "_")
}

func containsAny(value string, patterns ...string) bool {
	for _, pattern := range patterns {
		if strings.Contains(value, pattern) {
			return true
		}
	}
	return false
}

func detectGroupedCountGranularity(questionNorm string) (string, bool) {
	if questionNorm == "" {
		return "", false
	}
	if containsAny(questionNorm, "earliest", "latest", "first", "last", "oldest", "youngest") {
		return "", false
	}

	granularity := ""
	switch {
	case containsAny(questionNorm, "year", "years"):
		granularity = "year"
	case containsAny(questionNorm, "month", "months"):
		granularity = "month"
	case containsAny(questionNorm, "day", "days", "date", "dates"):
		granularity = "day"
	default:
		return "", false
	}

	if !containsAny(questionNorm, "most", "highest number", "largest number", "greatest number", "maximum", "max") {
		return "", false
	}

	if !containsAny(questionNorm, "death", "deaths", "died", "birth", "births", "born", "burial", "buried", "admission", "admissions", "admitted", "discharge", "discharges", "occur", "occurred") {
		return "", false
	}

	return granularity, true
}

func shouldUseSessionContext(question string, session *chatSessionState) bool {
	if session == nil {
		return false
	}
	qnorm := normalizeSearchText(question)
	if qnorm == "" {
		return false
	}
	padded := " " + qnorm + " "
	if hasSessionPronoun(padded) {
		return len(session.FocusRowIDs) > 0
	}
	if containsAny(qnorm, "what about", "how about", "same person", "same one", "same child", "same student", "that person", "that one", "this person", "this one") {
		return len(session.FocusRowIDs) > 0
	}
	if session.PendingPrompt != "" && isLikelyClarificationReply(qnorm) {
		return true
	}
	return false
}

func isLikelyClarificationReply(questionNorm string) bool {
	if questionNorm == "" {
		return false
	}
	if containsAny(questionNorm, "did any", "were there any", "are there any", "is there any", "how many", "count", "number of", "in what year", "in what years", "which year", "which years", "tell me", "list ", "show ", "find ", "search ", "who ", "what ", "when ", "where ") {
		return false
	}
	return len(strings.Fields(questionNorm)) <= 8
}

func isExistenceCountQuestion(questionNorm string) bool {
	return containsAny(questionNorm,
		"did any",
		"were there any",
		"are there any",
		"is there any",
		"was there any",
		"have any",
	)
}

func hasSessionPronoun(questionNorm string) bool {
	return containsAny(questionNorm, " he ", " she ", " they ", " them ", " him ", " her ", " his ", " their ", " same person", " same one", " that person", " that one", " this one", " this person")
}

func scoreNameMatch(query, candidate string) float64 {
	if query == "" || candidate == "" {
		return 0
	}
	if query == candidate {
		return 1
	}
	if strings.Contains(candidate, query) || strings.Contains(query, candidate) {
		return 0.96
	}

	// Check for known name variations (Albert/Bert, Elizabeth/Betsy, etc.)
	if isNameVariation(query, candidate) {
		return 0.92
	}

	// Check if phonetically similar (handles common spelling variations)
	if soundsLike(query, candidate) {
		return 0.88
	}

	// Split by spaces and check token matches
	queryTokens := strings.Fields(query)
	candidateTokens := strings.Fields(candidate)
	overlap := 0
	for _, qt := range queryTokens {
		for _, ct := range candidateTokens {
			if qt == ct {
				overlap++
				break
			}
		}
	}
	tokenScore := 0.0
	if len(queryTokens) > 0 {
		tokenScore = float64(overlap) / float64(len(queryTokens))
	}

	// Levenshtein distance for fuzzy matching
	dist := levenshteinDistance(query, candidate)
	maxLen := maxInt(len(query), len(candidate))
	if maxLen == 0 {
		return tokenScore
	}
	editScore := 1 - (float64(dist) / float64(maxLen))

	// For short names, be less strict about edit distance
	if maxLen <= 8 {
		// Names like "Leo" vs "Loe" should match at high score
		if dist <= 2 {
			editScore = math.Max(editScore, 0.85)
		}
	}

	return math.Max(tokenScore, editScore)
}

// isNameVariation checks for common name aliases/variations
func isNameVariation(name1, name2 string) bool {
	n1 := strings.ToLower(strings.TrimSpace(name1))
	n2 := strings.ToLower(strings.TrimSpace(name2))

	variations := map[string][]string{
		// Common name variations
		"albert":      {"bert", "al"},
		"bert":        {"albert"},
		"elizabeth":   {"betsy", "beth", "liz", "betty", "libby"},
		"betsy":       {"elizabeth", "beth", "betty"},
		"betty":       {"elizabeth", "betsy", "beth"},
		"beth":        {"elizabeth", "betsy", "betty"},
		"william":     {"bill", "will", "liam", "wm"},
		"bill":        {"william", "will"},
		"will":        {"william", "bill"},
		"margaret":    {"maggie", "meg", "marge", "margot"},
		"maggie":      {"margaret", "meg", "marge"},
		"meg":         {"margaret", "maggie", "marge"},
		"john":        {"johnny", "jon", "jack"},
		"johnny":      {"john", "jon"},
		"jon":         {"john", "johnny"},
		"jack":        {"john", "johnny"},
		"robert":      {"bob", "rob", "robin"},
		"bob":         {"robert", "rob", "robin"},
		"rob":         {"robert", "bob"},
		"robin":       {"robert", "rob", "bob"},
		"james":       {"jim", "jimmy", "jamie", "jim"},
		"jim":         {"james", "jimmy", "jamie"},
		"jimmy":       {"james", "jim", "jamie"},
		"jamie":       {"james", "jim", "jimmy"},
		"joseph":      {"joe", "jo", "joey"},
		"joe":         {"joseph", "joey"},
		"joey":        {"joseph", "joe"},
		"thomas":      {"tom", "tommy", "tom"},
		"tom":         {"thomas", "tommy"},
		"tommy":       {"thomas", "tom"},
		"benjamin":    {"ben", "benji", "benny"},
		"ben":         {"benjamin", "benji"},
		"benji":       {"benjamin", "ben", "benny"},
		"christopher": {"chris", "kit", "kip"},
		"chris":       {"christopher", "kit"},
		"charles":     {"charlie", "chuck", "chas"},
		"charlie":     {"charles", "chuck", "chas"},
		"chuck":       {"charles", "charlie"},
		"edward":      {"ed", "eddie", "ted"},
		"ed":          {"edward", "eddie"},
		"eddie":       {"edward", "ed"},
		"ted":         {"edward", "ed"},
		"henry":       {"hank", "harry"},
		"hank":        {"henry", "harry"},
		"harry":       {"henry", "hank", "harrison"},
		"richard":     {"rich", "rick", "dick"},
		"rick":        {"richard", "rich"},
		"rich":        {"richard", "rick"},
		"dick":        {"richard", "rick", "rich"},
		"lawrence":    {"larry", "laurence", "lars"},
		"larry":       {"lawrence", "laurence"},
		"samuel":      {"sam", "sammie", "sammy"},
		"sam":         {"samuel", "sammie", "sammy"},
		"sammy":       {"samuel", "sam"},
		"sampson":     {"sam", "sammy"},
		"david":       {"dave", "davy"},
		"dave":        {"david", "davy"},
		"davy":        {"david", "dave"},
		"daniel":      {"dan", "danny"},
		"dan":         {"daniel", "danny"},
		"danny":       {"daniel", "dan"},
		"andrew":      {"andy", "andre", "andres"},
		"andy":        {"andrew", "andre"},
		"andre":       {"andrew", "andy"},
		"michael":     {"mike", "mick", "michael"},
		"mike":        {"michael", "mick"},
		"mick":        {"michael", "mike"},
		"paul":        {"paulo", "pablo", "pol"},
		"stephen":     {"steve", "stephen", "steven"},
		"steve":       {"stephen", "steven"},
		"steven":      {"stephen", "steve"},
		"patrick":     {"pat", "patty", "patrick"},
		"pat":         {"patrick", "patty"},
		"patty":       {"patrick", "pat"},
		"anthony":     {"tony", "ant"},
		"tony":        {"anthony", "ant"},
		"ant":         {"anthony", "tony"},
		"nicholas":    {"nick", "nicky", "col"},
		"nick":        {"nicholas", "nicky"},
		"nicky":       {"nicholas", "nick"},
		"george":      {"geo", "georgia"},
		"elijah":      {"eli", "lijah"},
		"eli":         {"elijah", "lijah"},
		"jeremiah":    {"jerry", "jer"},
		"jerry":       {"jeremiah", "jer"},
		"isaiah":      {"isa", "zay"},
		"isa":         {"isaiah", "zay"},
	}

	if alts, ok := variations[n1]; ok {
		for _, alt := range alts {
			if alt == n2 {
				return true
			}
		}
	}
	if alts, ok := variations[n2]; ok {
		for _, alt := range alts {
			if alt == n1 {
				return true
			}
		}
	}
	return false
}

// soundsLike checks for phonetic similarity (handles common spelling variations)
func soundsLike(name1, name2 string) bool {
	n1 := strings.ToLower(strings.TrimSpace(name1))
	n2 := strings.ToLower(strings.TrimSpace(name2))

	// Common phonetic substitutions
	phonetic1 := phoneticallyNormalize(n1)
	phonetic2 := phoneticallyNormalize(n2)

	if phonetic1 == phonetic2 && phonetic1 != "" {
		return true
	}

	// Check for common letter swaps (Penance vs Pinnance)
	if levenshteinDistance(n1, n2) <= 2 && len(n1) >= 4 {
		return true
	}

	return false
}

// phoneticallyNormalize applies common phonetic transformations
func phoneticallyNormalize(s string) string {
	s = strings.ToLower(s)
	// Vowel normalization (a, e, i, o, u all similar)
	s = strings.NewReplacer(
		"a", "a",
		"e", "a",
		"i", "a",
		"o", "a",
		"u", "a",
	).Replace(s)
	// Common consonant pairs
	s = strings.NewReplacer(
		"c", "k",
		"j", "g",
		"ph", "f",
		"qu", "kw",
		"x", "ks",
		"z", "s",
	).Replace(s)
	return s
}

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	}
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		current := make([]int, len(b)+1)
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			current[j] = minInt(
				current[j-1]+1,
				minInt(prev[j]+1, prev[j-1]+cost),
			)
		}
		prev = current
	}
	return prev[len(b)]
}

func parseNumericValue(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	raw = strings.ReplaceAll(raw, ",", "")
	v, err := strconv.ParseFloat(raw, 64)
	return v, err == nil
}

func isTruthy(raw string) bool {
	switch normalizeSearchText(raw) {
	case "yes", "y", "true", "1":
		return true
	default:
		return false
	}
}

func isFalsy(raw string) bool {
	switch normalizeSearchText(raw) {
	case "no", "n", "false", "0":
		return true
	default:
		return false
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
