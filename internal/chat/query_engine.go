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
	"time"

	dc "nordik-drive-api/internal/dataconfig"
	f "nordik-drive-api/internal/file"

	"gorm.io/datatypes"
)

type chatFieldRole string

const (
	chatFieldRoleText      chatFieldRole = "text"
	chatFieldRoleName      chatFieldRole = "name"
	chatFieldRoleDate      chatFieldRole = "date"
	chatFieldRoleNumber    chatFieldRole = "number"
	chatFieldRoleBoolean   chatFieldRole = "boolean"
	chatFieldRoleCommunity chatFieldRole = "community"
	chatFieldRoleSchool    chatFieldRole = "school"
	chatFieldRoleLocation  chatFieldRole = "location"
)

type chatSchemaField struct {
	ID         string
	Label      string
	Role       chatFieldRole
	Aliases    []string
	Examples   []string
	Searchable bool
	Groupable  bool
	ExactOnly  bool
}

type chatDatasetSchema struct {
	Fields            []chatSchemaField
	fieldByID         map[string]*chatSchemaField
	fieldByLookup     map[string]*chatSchemaField
	nameFieldIDs      []string
	communityFieldIDs []string
	schoolFieldIDs    []string
	locationFieldIDs  []string
	dateFieldIDs      []string
	eventFieldIDs     map[string][]string
}

type chatDatasetCacheEntry struct {
	FileID   uint
	Filename string
	Version  int
	ConfigAt time.Time
	rows     []cachedChatRow
	rowByID  map[int]*cachedChatRow
	schema   chatDatasetSchema
}

type cachedChatRow struct {
	RowID        int
	RowJSON      string
	Values       map[string]string
	Normalized   map[string]string
	Tokens       map[string][]string
	SearchTokens []string
	Names        []string
	Communities  []string
	Schools      []string
	Locations    []string
	Temporal     map[string]temporalValue
}

type chatPlannerOutput struct {
	TranscribedQuestion   string              `json:"transcribed_question,omitempty"`
	Intent                string              `json:"intent"`
	SubjectText           string              `json:"subject_text,omitempty"`
	UseSessionFocus       bool                `json:"use_session_focus,omitempty"`
	TargetFieldID         string              `json:"target_field_id,omitempty"`
	GroupByFieldID        string              `json:"group_by_field_id,omitempty"`
	GroupByGranularity    string              `json:"group_by_granularity,omitempty"`
	IncludeMatchedNames   bool                `json:"include_matched_names,omitempty"`
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
	Status                       string          `json:"status"`
	Intent                       string          `json:"intent,omitempty"`
	Question                     string          `json:"question,omitempty"`
	TargetFieldID                string          `json:"target_field_id,omitempty"`
	TargetFieldLabel             string          `json:"target_field_label,omitempty"`
	FocusRowIDs                  []int           `json:"-"`
	MatchedRowID                 *int            `json:"-"`
	ClarificationQuestion        string          `json:"clarification_question,omitempty"`
	ClarificationCandidateRowIDs []int           `json:"-"`
	Notes                        []string        `json:"notes,omitempty"`
	SourceRows                   []cachedChatRow `json:"-"`
	Value                        any             `json:"value,omitempty"`
}

type chatRowRecord struct {
	RowID              uint           `gorm:"column:row_id"`
	RowData            datatypes.JSON `gorm:"column:row_data"`
	RowUpdatedAt       time.Time      `gorm:"column:row_updated_at"`
	RowDataNormalized  datatypes.JSON `gorm:"column:row_data_normalized"`
	SearchText         string         `gorm:"column:search_text"`
	CanonicalName      string         `gorm:"column:canonical_name"`
	CanonicalCommunity string         `gorm:"column:canonical_community"`
	CanonicalSchool    string         `gorm:"column:canonical_school"`
	Status             string         `gorm:"column:status"`
	SourceUpdatedAt    *time.Time     `gorm:"column:source_updated_at"`
}

type chatNormalizedPayload struct {
	Fields       map[string]chatNormalizedField `json:"fields"`
	Names        []string                       `json:"names,omitempty"`
	Communities  []string                       `json:"communities,omitempty"`
	Schools      []string                       `json:"schools,omitempty"`
	Locations    []string                       `json:"locations,omitempty"`
	SearchTokens []string                       `json:"search_tokens,omitempty"`
}

type chatNormalizedField struct {
	Raw        string              `json:"raw"`
	Normalized string              `json:"normalized"`
	Tokens     []string            `json:"tokens,omitempty"`
	Role       string              `json:"role"`
	DateHint   *chatNormalizedDate `json:"date_hint,omitempty"`
}

type chatNormalizedDate struct {
	Raw         string `json:"raw"`
	Kind        string `json:"kind"`
	LowerYear   *int   `json:"lower_year,omitempty"`
	UpperYear   *int   `json:"upper_year,omitempty"`
	Approximate bool   `json:"approximate,omitempty"`
}

type chatConfigLookup struct {
	byKey map[string]*chatConfigFieldMeta
}

type chatConfigFieldMeta struct {
	Role       chatFieldRole
	Aliases    []string
	Searchable *bool
	Groupable  *bool
	ExactOnly  *bool
}

const chatPlannerInstruction = `
You convert the user's latest question into a strict JSON plan for a deterministic data engine.

Rules:
- Use ONLY field ids from SCHEMA.
- If the user asks about a specific person, put that in subject_text.
- Use use_session_focus=true only for clear follow-up wording like he, she, they, him, her, that person, same one.
- Use count_rows for yes/no or how many questions.
- Use group_count_extreme for most/least/highest/lowest by year/community/school/location.
- Use field_lookup for one specific detail about one person.
- Use describe_subject for general information about one person.
- Use list_values when the user wants names or a list.
- Use extreme for earliest/latest/first/last questions.
- If anything important is unclear, return intent clarify with one short question.
- Never invent field ids or filter values.

Return ONLY one JSON object with this shape:
{
  "transcribed_question": "filled only if audio was provided and you had to infer speech",
  "intent": "count_rows",
  "subject_text": "optional person name or subject phrase",
  "use_session_focus": false,
  "target_field_id": "optional schema field id",
  "group_by_field_id": "optional schema field id",
  "group_by_granularity": "optional year, month, or day",
  "search_terms": ["optional", "keywords"],
  "filters": [
    {"field_id": "optional schema field id", "op": "eq", "value": "value", "value2": "optional"}
  ],
  "sort_direction": "asc or desc",
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

Hard accuracy rules:
- Use ONLY facts from the VERIFIED RESULT. Do NOT add outside knowledge or guesses.
- Every claim must be traceable to the VERIFIED RESULT.
- If status is "needs_clarification", ask exactly the clarification question and nothing more.
- If status is "not_found", clearly say the records do not show a matching answer.
- If status is "cannot_determine_exactly", clearly say the answer cannot be determined exactly and use the notes if provided.
- If a value is missing or empty, say the records do not specify it.
- If dates are approximate, partial, or ranges, describe them exactly as provided.
- If multiple answers exist, include all of them in the first sentence.

Output rules:
- Write only the answer text.
- Start with the direct answer in the first sentence when possible.
`

func (cs *ChatService) ChatForUser(userID int64, question string, audioFile *multipart.FileHeader, filename string, communities []string) (*ChatResult, error) {
	if cs.DB == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	var file f.File
	if err := cs.DB.Select("id, filename, version, columns_order").
		Where("filename = ?", filename).
		Order("version DESC").
		First(&file).Error; err != nil {
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

	if verified, handled := cs.resolvePendingCandidateClarification(question, dataset, rows, session); handled {
		answer, err := cs.answerFromVerifiedResult(ctx, question, dataset, verified)
		if err != nil {
			answer = renderVerifiedAnswer(question, dataset, verified)
		}
		updateSessionAfterResult(session, question, answer, chatPlannerOutput{}, verified)
		cs.saveSession(userID, session)
		return &ChatResult{Answer: answer, MatchedRowID: verified.MatchedRowID}, nil
	}

	plan, resolvedQuestion, err := cs.planChatRequest(ctx, question, audioBytes, audioMime, dataset, session)
	if err != nil {
		return nil, err
	}

	if plan.Intent == "clarify" && session.Pending != nil && isLikelyClarificationReply(normalizeSearchText(question)) && session.Pending.Attempts >= chatClarificationBudget {
		verified := chatVerifiedResult{
			Status:   "cannot_determine_exactly",
			Intent:   plan.Intent,
			Question: resolvedQuestion,
			Notes:    []string{"I still can't determine one exact interpretation from the available data."},
		}
		answer := renderVerifiedAnswer(question, dataset, verified)
		updateSessionAfterResult(session, question, answer, plan, verified)
		cs.saveSession(userID, session)
		return &ChatResult{Answer: answer}, nil
	}

	verified := executeChatPlan(plan, resolvedQuestion, dataset, rows, session)
	answer, err := cs.answerFromVerifiedResult(ctx, resolvedQuestion, dataset, verified)
	if err != nil {
		answer = renderVerifiedAnswer(resolvedQuestion, dataset, verified)
	}

	updateSessionAfterResult(session, resolvedQuestion, answer, plan, verified)
	cs.saveSession(userID, session)

	return &ChatResult{
		Answer:       answer,
		MatchedRowID: verified.MatchedRowID,
	}, nil
}

func updateSessionAfterResult(session *chatSessionState, question, answer string, plan chatPlannerOutput, verified chatVerifiedResult) {
	if session == nil {
		return
	}
	if verified.Status == "needs_clarification" {
		attempts := 1
		originalQuestion := question
		if session.Pending != nil && isLikelyClarificationReply(normalizeSearchText(question)) {
			attempts = session.Pending.Attempts + 1
			if strings.TrimSpace(session.Pending.OriginalQuestion) != "" {
				originalQuestion = session.Pending.OriginalQuestion
			}
		}
		session.Pending = &chatPendingClarification{
			Prompt:           verified.ClarificationQuestion,
			Attempts:         attempts,
			OriginalQuestion: originalQuestion,
			CandidateRowIDs:  cloneIntSlice(verified.ClarificationCandidateRowIDs),
			Plan:             plan,
		}
	} else {
		session.Pending = nil
	}
	session.registerTurn(question, answer, verified.TargetFieldID, focusIDsForSession(verified))
}

func (cs *ChatService) resolvePendingCandidateClarification(
	question string,
	dataset *chatDatasetCacheEntry,
	rows []cachedChatRow,
	session *chatSessionState,
) (chatVerifiedResult, bool) {
	if session == nil || session.Pending == nil || len(session.Pending.CandidateRowIDs) == 0 {
		return chatVerifiedResult{}, false
	}
	if !isLikelyClarificationReply(normalizeSearchText(question)) {
		return chatVerifiedResult{}, false
	}

	candidates := dataset.rowsByIDs(session.Pending.CandidateRowIDs, rows)
	if len(candidates) == 0 {
		return chatVerifiedResult{}, false
	}

	refined := refineClarificationRows(candidates, dataset, question)
	if len(refined) == 1 {
		plan := session.Pending.Plan
		plan.SubjectText = ""
		plan.UseSessionFocus = false
		verified := executeChatPlan(plan, strings.TrimSpace(session.Pending.OriginalQuestion+" "+question), dataset, refined, session)
		return verified, true
	}

	if len(refined) > 1 && session.Pending.Attempts < chatClarificationBudget {
		return chatVerifiedResult{
			Status:                       "needs_clarification",
			Intent:                       session.Pending.Plan.Intent,
			Question:                     question,
			ClarificationQuestion:        buildRowClarificationQuestion(refined, dataset),
			ClarificationCandidateRowIDs: rowIDsFromRows(refined),
		}, true
	}

	return chatVerifiedResult{
		Status:   "cannot_determine_exactly",
		Intent:   session.Pending.Plan.Intent,
		Question: question,
		Notes:    []string{"I still can't narrow that down to one exact record."},
	}, true
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
) (chatPlannerOutput, string, error) {
	question = strings.TrimSpace(question)
	if session != nil && session.Pending != nil && len(session.Pending.CandidateRowIDs) == 0 && isLikelyClarificationReply(normalizeSearchText(question)) {
		question = strings.TrimSpace(session.Pending.OriginalQuestion + " " + question)
	}

	if question == "" && len(audioBytes) == 0 {
		return chatPlannerOutput{
			Intent:                "clarify",
			ClarificationQuestion: "What would you like to know about the data?",
			Limit:                 5,
		}, question, nil
	}

	if len(audioBytes) == 0 {
		if deterministic, ok := deterministicChatPlan(question, dataset, session); ok {
			return deterministic, question, nil
		}
	}

	if cs.Client == nil {
		return chatPlannerOutput{
			Intent:                "clarify",
			ClarificationQuestion: "Could you rephrase that a little more specifically?",
			Limit:                 5,
		}, question, nil
	}

	prompt := buildChatPlannerPrompt(question, dataset, session)
	raw, usedModel, err := cs.generateFromPrompt(ctx, prompt, audioBytes, audioMime)
	if err != nil {
		return chatPlannerOutput{}, question, fmt.Errorf("generation error (%s): %w", usedModel, err)
	}

	plan, ok := parseChatPlannerOutput(raw, &dataset.schema)
	if !ok {
		return chatPlannerOutput{
			Intent:                "clarify",
			ClarificationQuestion: "Could you rephrase that a little more specifically?",
			Limit:                 5,
		}, question, nil
	}

	resolvedQuestion := strings.TrimSpace(plan.TranscribedQuestion)
	if resolvedQuestion == "" {
		resolvedQuestion = question
	}
	if !shouldUseSessionContext(resolvedQuestion, session) {
		plan.UseSessionFocus = false
	}
	if deterministic, ok := deterministicChatPlan(resolvedQuestion, dataset, session); ok {
		plan = mergePlannerWithDeterministic(plan, deterministic)
	}
	return plan, resolvedQuestion, nil
}

func buildChatPlannerPrompt(question string, dataset *chatDatasetCacheEntry, session *chatSessionState) string {
	sessionSummary := "No active session context for this question."
	if shouldUseSessionContext(question, session) {
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
		case "eq", "contains", "before", "after", "between", "yes", "no", "is_empty", "is_not_empty":
			cleanFilters = append(cleanFilters, filter)
		}
	}
	plan.Filters = cleanFilters

	searchTerms := make([]string, 0, len(plan.SearchTerms))
	for _, term := range plan.SearchTerms {
		term = strings.TrimSpace(term)
		if term != "" {
			searchTerms = append(searchTerms, term)
		}
	}
	plan.SearchTerms = searchTerms
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

func mergePlannerWithDeterministic(planned, deterministic chatPlannerOutput) chatPlannerOutput {
	if planned.Intent == "clarify" && deterministic.Intent != "" {
		return deterministic
	}
	if deterministic.Intent == "" {
		return planned
	}
	if planned.Intent == "" {
		return deterministic
	}
	if planned.TargetFieldID == "" {
		planned.TargetFieldID = deterministic.TargetFieldID
	}
	if planned.GroupByFieldID == "" {
		planned.GroupByFieldID = deterministic.GroupByFieldID
	}
	if planned.GroupByGranularity == "" {
		planned.GroupByGranularity = deterministic.GroupByGranularity
	}
	if planned.SortDirection == "" {
		planned.SortDirection = deterministic.SortDirection
	}
	if len(planned.Filters) == 0 {
		planned.Filters = deterministic.Filters
	}
	if len(planned.SearchTerms) == 0 {
		planned.SearchTerms = deterministic.SearchTerms
	}
	if strings.TrimSpace(planned.SubjectText) == "" {
		planned.SubjectText = deterministic.SubjectText
	}
	if !planned.UseSessionFocus {
		planned.UseSessionFocus = deterministic.UseSessionFocus
	}
	planned.IncludeMatchedNames = planned.IncludeMatchedNames || deterministic.IncludeMatchedNames
	if planned.Limit <= 0 {
		planned.Limit = deterministic.Limit
	}
	return planned
}

func (cs *ChatService) answerFromVerifiedResult(
	ctx context.Context,
	question string,
	dataset *chatDatasetCacheEntry,
	verified chatVerifiedResult,
) (string, error) {
	if cs.Client == nil {
		return "", fmt.Errorf("genai client not initialized")
	}
	payload, err := json.MarshalIndent(buildVerifiedAnswerPayload(dataset, verified), "", "  ")
	if err != nil {
		return "", err
	}

	prompt := fmt.Sprintf(
		"%s\n\n%s\n\nUser question:\n%s\n\nVERIFIED RESULT (only source of truth):\n%s",
		strings.TrimSpace(chatStyleInstruction),
		strings.TrimSpace(chatVerifiedAnswerInstruction),
		strings.TrimSpace(question),
		string(payload),
	)

	answer, _, err := cs.generateFromPrompt(ctx, prompt, nil, "")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(answer), nil
}

func buildVerifiedAnswerPayload(dataset *chatDatasetCacheEntry, verified chatVerifiedResult) map[string]any {
	payload := map[string]any{
		"status":             verified.Status,
		"intent":             verified.Intent,
		"question":           verified.Question,
		"target_field_id":    verified.TargetFieldID,
		"target_field_label": verified.TargetFieldLabel,
		"value":              verified.Value,
		"notes":              verified.Notes,
	}
	if verified.ClarificationQuestion != "" {
		payload["clarification_question"] = verified.ClarificationQuestion
	}
	if len(verified.SourceRows) > 0 {
		rows := make([]map[string]any, 0, minInt(len(verified.SourceRows), 8))
		limit := minInt(len(verified.SourceRows), 8)
		includeAllFields := limit <= 4
		for idx := 0; idx < limit; idx++ {
			rows = append(rows, dataset.rowPromptData(verified.SourceRows[idx], verified.TargetFieldID, includeAllFields))
		}
		payload["rows"] = rows
	}
	return payload
}

func deterministicChatPlan(question string, dataset *chatDatasetCacheEntry, session *chatSessionState) (chatPlannerOutput, bool) {
	question = strings.TrimSpace(question)
	if question == "" {
		return chatPlannerOutput{}, false
	}

	qnorm := normalizeSearchText(question)
	if qnorm == "" {
		return chatPlannerOutput{}, false
	}

	useSessionContext := shouldUseSessionContext(question, session)
	plan := chatPlannerOutput{
		Limit:               5,
		UseSessionFocus:     useSessionContext && len(session.FocusRowIDs) > 0 && hasSessionPronoun(" "+qnorm+" "),
		IncludeMatchedNames: questionRequestsMatchedNames(qnorm),
	}

	if grouped, ok := deterministicGroupedPlan(question, dataset, plan); ok {
		return grouped, true
	}
	if counted, ok := deterministicCountPlan(question, dataset, plan); ok {
		return counted, true
	}
	if listed, ok := deterministicListPlan(question, dataset, plan); ok {
		return listed, true
	}
	if fieldLookup, ok := deterministicFieldLookupPlan(question, dataset, session, plan); ok {
		return fieldLookup, true
	}
	if described, ok := deterministicDescribePlan(question, dataset, session, plan); ok {
		return described, true
	}
	if extreme, ok := deterministicExtremePlan(question, dataset, plan); ok {
		return extreme, true
	}

	return chatPlannerOutput{}, false
}

func deterministicGroupedPlan(question string, dataset *chatDatasetCacheEntry, base chatPlannerOutput) (chatPlannerOutput, bool) {
	qnorm := normalizeSearchText(question)
	granularity, direction, groupedByDate := detectGroupedCountSpec(qnorm)
	hasComparison := containsAny(qnorm, "most", "highest", "least", "lowest", "fewest", "smallest number", "largest number", "greatest number", "maximum", "minimum", "max", "min")
	if !groupedByDate && !hasComparison {
		return chatPlannerOutput{}, false
	}

	plan := base
	plan.Intent = "group_count_extreme"
	plan.SortDirection = direction
	plan.Filters = inferQuestionFilters(question, dataset, nil)
	plan.SearchTerms = inferSearchTerms(question, dataset, plan.Filters)

	if groupedByDate {
		if field := dataset.schema.bestDateFieldForQuestion(question); field != nil {
			plan.TargetFieldID = field.ID
			plan.GroupByFieldID = field.ID
			plan.GroupByGranularity = granularity
			return plan, true
		}
		return chatPlannerOutput{}, false
	}

	switch {
	case containsAny(qnorm, "community", "communities", "reserve", "reserves", "first nation", "first nations", "home", "homes"):
		if field := dataset.schema.bestCommunityFieldForQuestion(question); field != nil {
			plan.TargetFieldID = field.ID
			plan.GroupByFieldID = field.ID
			return plan, true
		}
	case containsAny(qnorm, "school", "schools", "institution", "institutions"):
		if field := dataset.schema.bestSchoolFieldForQuestion(question); field != nil {
			plan.TargetFieldID = field.ID
			plan.GroupByFieldID = field.ID
			return plan, true
		}
	}

	return chatPlannerOutput{}, false
}

func deterministicCountPlan(question string, dataset *chatDatasetCacheEntry, base chatPlannerOutput) (chatPlannerOutput, bool) {
	qnorm := normalizeSearchText(question)
	if !isExistenceCountQuestion(qnorm) && !containsAny(qnorm, "how many", "count", "number of", "total") {
		return chatPlannerOutput{}, false
	}

	plan := base
	plan.Intent = "count_rows"
	plan.Filters = inferQuestionFilters(question, dataset, nil)
	plan.SearchTerms = inferSearchTerms(question, dataset, plan.Filters)
	return plan, true
}

func deterministicListPlan(question string, dataset *chatDatasetCacheEntry, base chatPlannerOutput) (chatPlannerOutput, bool) {
	qnorm := normalizeSearchText(question)
	if !containsAny(qnorm, "list", "show me", "show all", "what are the names", "what were the names", "which children", "who were the children", "who are the children") {
		return chatPlannerOutput{}, false
	}

	plan := base
	plan.Intent = "list_values"
	plan.Filters = inferQuestionFilters(question, dataset, nil)
	plan.SearchTerms = inferSearchTerms(question, dataset, plan.Filters)
	if containsAny(qnorm, "names", "children", "students", "who ") && len(dataset.schema.nameFieldIDs) > 0 {
		plan.TargetFieldID = dataset.schema.nameFieldIDs[0]
	}
	return plan, true
}

func deterministicFieldLookupPlan(question string, dataset *chatDatasetCacheEntry, session *chatSessionState, base chatPlannerOutput) (chatPlannerOutput, bool) {
	qnorm := normalizeSearchText(question)
	field := dataset.schema.bestFieldForQuestion(question)
	if field == nil {
		return chatPlannerOutput{}, false
	}

	if base.UseSessionFocus && len(session.FocusRowIDs) > 0 {
		return chatPlannerOutput{
			Intent:              "field_lookup",
			TargetFieldID:       field.ID,
			UseSessionFocus:     true,
			IncludeMatchedNames: base.IncludeMatchedNames,
			Limit:               5,
		}, true
	}

	if containsAny(qnorm, "what is", "what was", "when did", "where did", "how did", "which", "who was", "who is") {
		subject := dataset.bestSubjectFromQuestion(question)
		if subject == "" {
			subject = extractSubjectCandidate(question)
		}
		if subject != "" {
			plan := base
			plan.Intent = "field_lookup"
			plan.TargetFieldID = field.ID
			plan.SubjectText = subject
			plan.Filters = inferQuestionFilters(question, dataset, nil)
			return plan, true
		}
	}

	return chatPlannerOutput{}, false
}

func deterministicDescribePlan(question string, dataset *chatDatasetCacheEntry, session *chatSessionState, base chatPlannerOutput) (chatPlannerOutput, bool) {
	qnorm := normalizeSearchText(question)
	if !(containsAny(qnorm, "tell me about", "describe", "information about", "who is", "who was") || (base.UseSessionFocus && len(session.FocusRowIDs) > 0)) {
		return chatPlannerOutput{}, false
	}

	subject := ""
	if !base.UseSessionFocus {
		subject = dataset.bestSubjectFromQuestion(question)
		if subject == "" {
			subject = extractSubjectCandidate(question)
		}
	}

	plan := base
	plan.Intent = "describe_subject"
	plan.SubjectText = subject
	plan.Filters = inferQuestionFilters(question, dataset, nil)
	return plan, true
}

func deterministicExtremePlan(question string, dataset *chatDatasetCacheEntry, base chatPlannerOutput) (chatPlannerOutput, bool) {
	qnorm := normalizeSearchText(question)
	if !containsAny(qnorm, "earliest", "latest", "first", "last", "oldest", "youngest") {
		return chatPlannerOutput{}, false
	}

	field := dataset.schema.bestDateFieldForQuestion(question)
	if field == nil {
		field = dataset.schema.defaultExtremeField(qnorm)
	}
	if field == nil {
		return chatPlannerOutput{}, false
	}

	direction := "asc"
	if containsAny(qnorm, "latest", "last", "youngest") {
		direction = "desc"
	}

	plan := base
	plan.Intent = "extreme"
	plan.TargetFieldID = field.ID
	plan.SortDirection = direction
	plan.Filters = inferQuestionFilters(question, dataset, nil)
	plan.SearchTerms = inferSearchTerms(question, dataset, plan.Filters)
	return plan, true
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
	if plan.UseSessionFocus && len(session.FocusRowIDs) > 0 {
		if focused := dataset.rowsByIDs(session.FocusRowIDs, rows); len(focused) > 0 {
			candidates = focused
		}
	}

	if eventScope := detectQuestionEventScope(question); shouldApplyEventScope(plan, eventScope) {
		scoped := applyEventScope(candidates, dataset, eventScope)
		if len(scoped) == 0 {
			verified.Status = "not_found"
			verified.Notes = []string{fmt.Sprintf("No records match the requested %s scope.", eventScope)}
			return verified
		}
		candidates = scoped
	}

	if strings.TrimSpace(plan.SubjectText) != "" {
		matches := matchSubjectRows(candidates, plan.SubjectText)
		if len(matches) == 0 {
			verified.Status = "not_found"
			verified.Notes = []string{"No matching person was found."}
			return verified
		}
		selected := selectSubjectRows(matches)
		if len(selected) > 1 && (plan.Intent == "describe_subject" || plan.Intent == "field_lookup") {
			verified.Status = "needs_clarification"
			verified.ClarificationQuestion = buildRowClarificationQuestion(selected, dataset)
			verified.ClarificationCandidateRowIDs = rowIDsFromRows(selected)
			return verified
		}
		candidates = selected
	}

	for _, searchTerm := range plan.SearchTerms {
		searchTerm = strings.TrimSpace(searchTerm)
		if searchTerm == "" {
			continue
		}
		filtered := applyMultiColumnFilter(candidates, dataset, searchTerm)
		if len(filtered) == 0 && len(candidates) > 0 {
			verified.Status = "not_found"
			verified.Notes = []string{fmt.Sprintf("No records found containing '%s' in the available data.", searchTerm)}
			return verified
		}
		candidates = filtered
	}

	for _, filter := range plan.Filters {
		field, ok := dataset.schema.resolveField(filter.FieldID)
		if !ok {
			continue
		}
		filtered := applyFilter(candidates, field, filter)
		if (filter.Op == "eq" || filter.Op == "contains") && isLocationLikeField(field) {
			if preferred := applyPreferredLocationFilter(candidates, dataset, field, filter.Value); len(preferred) > 0 {
				candidates = preferred
				continue
			}
		}
		if len(filtered) == 0 {
			verified.Status = "not_found"
			verified.Notes = []string{fmt.Sprintf("No records match the filter on '%s'.", field.Label)}
			return verified
		}
		candidates = filtered
	}

	if len(candidates) == 0 {
		verified.Status = "not_found"
		return verified
	}

	if field, ok := dataset.schema.resolveField(plan.TargetFieldID); ok {
		verified.TargetFieldID = field.ID
		verified.TargetFieldLabel = field.Label
	}

	switch plan.Intent {
	case "describe_subject":
		return buildDescribeVerifiedResult(dataset, verified, candidates)
	case "field_lookup":
		return buildFieldLookupVerifiedResult(dataset, verified, candidates)
	case "count_rows":
		verified.Value = len(candidates)
		verified.FocusRowIDs = rowIDsFromRows(candidates)
		verified.SourceRows = cloneRows(candidates)
		return verified
	case "list_values":
		return buildListValuesVerifiedResult(dataset, verified, candidates)
	case "extreme":
		return buildExtremeVerifiedResult(dataset, verified, candidates, plan.SortDirection)
	case "group_count_extreme":
		return buildGroupCountExtremeVerifiedResult(dataset, verified, candidates, plan)
	default:
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The question could not be mapped safely.")
		return verified
	}
}

func buildDescribeVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow) chatVerifiedResult {
	if len(rows) != 1 {
		verified.Status = "needs_clarification"
		verified.ClarificationQuestion = buildRowClarificationQuestion(rows, dataset)
		verified.ClarificationCandidateRowIDs = rowIDsFromRows(rows)
		return verified
	}

	rowID := rows[0].RowID
	verified.MatchedRowID = &rowID
	verified.FocusRowIDs = rowIDsFromRows(rows)
	verified.SourceRows = cloneRows(rows)
	return verified
}

func buildFieldLookupVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow) chatVerifiedResult {
	if verified.TargetFieldID == "" {
		verified.Status = "needs_clarification"
		verified.ClarificationQuestion = "Which detail would you like to know?"
		return verified
	}

	field, ok := dataset.schema.resolveField(verified.TargetFieldID)
	if !ok {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The requested field could not be resolved.")
		return verified
	}
	verified.TargetFieldLabel = field.Label

	if len(rows) != 1 {
		verified.Status = "needs_clarification"
		verified.ClarificationQuestion = buildRowClarificationQuestion(rows, dataset)
		verified.ClarificationCandidateRowIDs = rowIDsFromRows(rows)
		return verified
	}

	rowID := rows[0].RowID
	verified.MatchedRowID = &rowID
	verified.FocusRowIDs = rowIDsFromRows(rows)
	verified.SourceRows = cloneRows(rows)
	verified.Value = strings.TrimSpace(rows[0].valueByField(field.ID, &dataset.schema))
	return verified
}

func buildListValuesVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow) chatVerifiedResult {
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

	values := make([]string, 0)
	seen := map[string]struct{}{}
	for _, row := range rows {
		value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
		if value == "" {
			continue
		}
		key := normalizeSearchText(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, value)
	}
	sort.Strings(values)

	verified.TargetFieldID = field.ID
	verified.TargetFieldLabel = field.Label
	verified.FocusRowIDs = rowIDsFromRows(rows)
	verified.SourceRows = cloneRows(rows)
	verified.Value = values
	return verified
}

func buildExtremeVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow, direction string) chatVerifiedResult {
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

	var selected []cachedChatRow
	switch field.Role {
	case chatFieldRoleDate:
		var winner *cachedChatRow
		var tied []cachedChatRow
		var note string
		if direction == "desc" {
			winner, tied, note = determineLatestDate(rows, field.ID)
		} else {
			winner, tied, note = determineEarliestDate(rows, field.ID)
		}
		if note != "" {
			verified.Status = "cannot_determine_exactly"
			verified.Notes = append(verified.Notes, note)
			return verified
		}
		if winner != nil {
			selected = []cachedChatRow{*winner}
			rowID := winner.RowID
			verified.MatchedRowID = &rowID
		} else {
			selected = tied
		}
	case chatFieldRoleNumber:
		selected = determineExtremeNumber(rows, field.ID, direction)
	default:
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "That comparison is not supported exactly yet.")
		return verified
	}

	if len(selected) == 0 {
		verified.Status = "not_found"
		return verified
	}

	verified.FocusRowIDs = rowIDsFromRows(selected)
	verified.SourceRows = cloneRows(selected)
	if len(selected) == 1 {
		verified.Value = strings.TrimSpace(selected[0].valueByField(field.ID, &dataset.schema))
	}
	return verified
}

func buildGroupCountExtremeVerifiedResult(dataset *chatDatasetCacheEntry, verified chatVerifiedResult, rows []cachedChatRow, plan chatPlannerOutput) chatVerifiedResult {
	groupFieldID := plan.GroupByFieldID
	if groupFieldID == "" {
		groupFieldID = verified.TargetFieldID
	}
	field, ok := dataset.schema.resolveField(groupFieldID)
	if !ok {
		verified.Status = "cannot_determine_exactly"
		verified.Notes = append(verified.Notes, "The grouped comparison field could not be resolved.")
		return verified
	}

	type bucketCount struct {
		Key     string
		Display string
		Count   int
	}

	counts := map[string]int{}
	displayValues := map[string]string{}
	groupRows := map[string][]cachedChatRow{}
	uncertainRows := 0

	for _, row := range rows {
		key, display, exact, present := extractGroupCountKey(row, field, plan.GroupByGranularity, &dataset.schema)
		switch {
		case exact:
			counts[key]++
			if displayValues[key] == "" {
				displayValues[key] = display
			}
			groupRows[key] = append(groupRows[key], row)
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
		if plan.SortDirection == "asc" {
			return buckets[i].Count < buckets[j].Count
		}
		return buckets[i].Count > buckets[j].Count
	})

	bestCount := buckets[0].Count
	winners := make([]bucketCount, 0, len(buckets))
	for _, bucket := range buckets {
		if bucket.Count != bestCount {
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
		if len(buckets) > 1 {
			secondCount := buckets[1].Count
			if plan.SortDirection == "asc" {
				if bestCount+uncertainRows >= secondCount {
					verified.Status = "cannot_determine_exactly"
					verified.Notes = append(verified.Notes, "Some matching rows do not identify one exact group, so the exact top result could change.")
					return verified
				}
			} else if bestCount <= secondCount+uncertainRows {
				verified.Status = "cannot_determine_exactly"
				verified.Notes = append(verified.Notes, "Some matching rows do not identify one exact group, so the exact top result could change.")
				return verified
			}
		}
	}

	winnerValues := make([]string, 0, len(winners))
	winnerNames := make([]string, 0)
	seenNames := map[string]struct{}{}
	winnerRows := make([]cachedChatRow, 0)
	seenRowIDs := map[int]struct{}{}
	for _, winner := range winners {
		winnerValues = append(winnerValues, winner.Display)
		for _, row := range groupRows[winner.Key] {
			if _, ok := seenRowIDs[row.RowID]; ok {
				continue
			}
			seenRowIDs[row.RowID] = struct{}{}
			winnerRows = append(winnerRows, row)
			if name := strings.TrimSpace(row.primaryName()); name != "" {
				key := normalizeSearchText(name)
				if _, ok := seenNames[key]; ok {
					continue
				}
				seenNames[key] = struct{}{}
				winnerNames = append(winnerNames, name)
			}
		}
	}
	sort.Strings(winnerNames)

	verified.FocusRowIDs = rowIDsFromRows(winnerRows)
	verified.SourceRows = cloneRows(winnerRows)
	verified.Value = map[string]any{
		"group_by":       firstNonEmpty(plan.GroupByGranularity, field.Label),
		"group_field":    field.Label,
		"winning_values": winnerValues,
		"max_count":      bestCount,
		"sort_direction": firstNonEmpty(plan.SortDirection, "desc"),
		"winner_names":   winnerNames,
	}
	return verified
}

func renderVerifiedAnswer(question string, dataset *chatDatasetCacheEntry, verified chatVerifiedResult) string {
	switch verified.Status {
	case "needs_clarification":
		return strings.TrimSpace(verified.ClarificationQuestion)
	case "not_found":
		return renderNotFoundAnswer(question, verified)
	case "cannot_determine_exactly":
		if len(verified.Notes) > 0 {
			return "I can't determine that exactly from the available data. " + strings.TrimSpace(verified.Notes[0])
		}
		return "I can't determine that exactly from the available data."
	}

	switch verified.Intent {
	case "describe_subject":
		return renderDescribeAnswer(dataset, verified)
	case "field_lookup":
		return renderFieldLookupAnswer(verified)
	case "count_rows":
		return renderCountAnswer(question, verified)
	case "list_values":
		return renderListAnswer(verified)
	case "extreme":
		return renderExtremeAnswer(question, verified)
	case "group_count_extreme":
		return renderGroupAnswer(question, verified)
	default:
		return "I couldn't map that question safely."
	}
}

func renderNotFoundAnswer(question string, verified chatVerifiedResult) string {
	if isExistenceCountQuestion(normalizeSearchText(question)) {
		return "No. I couldn't find any matching records in the available data."
	}
	if len(verified.Notes) > 0 {
		return strings.TrimSpace(verified.Notes[0])
	}
	return "I couldn't find a matching answer in the available data."
}

func renderDescribeAnswer(dataset *chatDatasetCacheEntry, verified chatVerifiedResult) string {
	if len(verified.SourceRows) == 0 {
		return "I couldn't find a matching record in the available data."
	}
	row := verified.SourceRows[0]
	name := firstNonEmpty(row.primaryName(), "This record")
	sentences := []string{name + "."}

	if community := firstNonEmpty(row.Communities...); community != "" {
		sentences = append(sentences, "The record links them to "+community+".")
	}
	if school := firstNonEmpty(row.Schools...); school != "" {
		sentences = append(sentences, "The school listed is "+school+".")
	}
	if field := dataset.schema.bestDateFieldForQuestion("death"); field != nil {
		if value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema)); value != "" {
			sentences = append(sentences, "The date listed is "+value+".")
		}
	}
	if causeField := dataset.schema.bestFieldForQuestion("cause of death"); causeField != nil {
		if value := strings.TrimSpace(row.valueByField(causeField.ID, &dataset.schema)); value != "" {
			sentences = append(sentences, "The recorded cause or detail is "+value+".")
		}
	}
	if len(sentences) == 1 {
		for _, field := range dataset.schema.Fields {
			value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
			if value == "" || field.Role == chatFieldRoleName {
				continue
			}
			sentences = append(sentences, fmt.Sprintf("%s is listed as %s.", strings.ToLower(field.Label), value))
			if len(sentences) >= 3 {
				break
			}
		}
	}
	return strings.Join(sentences, " ")
}

func renderFieldLookupAnswer(verified chatVerifiedResult) string {
	if len(verified.SourceRows) == 0 {
		return "I couldn't find a matching record in the available data."
	}
	row := verified.SourceRows[0]
	name := row.primaryName()
	fieldLabel := strings.ToLower(strings.TrimSpace(verified.TargetFieldLabel))
	value := strings.TrimSpace(fmt.Sprint(verified.Value))
	if value == "" || value == "<nil>" {
		if name != "" {
			return fmt.Sprintf("The records don't specify %s's %s.", name, fieldLabel)
		}
		return fmt.Sprintf("The records don't specify %s.", fieldLabel)
	}
	if name != "" {
		return fmt.Sprintf("The records show %s's %s as %s.", name, fieldLabel, value)
	}
	return fmt.Sprintf("The %s listed in the records is %s.", fieldLabel, value)
}

func renderCountAnswer(question string, verified chatVerifiedResult) string {
	count, _ := verified.Value.(int)
	noun := groupedEventNoun(question)
	if isExistenceCountQuestion(normalizeSearchText(question)) {
		if count == 0 {
			return "No. I couldn't find any matching records in the available data."
		}
		if questionRequestsMatchedNames(normalizeSearchText(question)) {
			names := uniqueNamesFromRows(verified.SourceRows)
			if len(names) > 0 {
				return fmt.Sprintf("Yes. I found %s: %s.", groupedCountPhrase(noun, count), joinWithAnd(names))
			}
		}
		return fmt.Sprintf("Yes. I found %s.", groupedCountPhrase(noun, count))
	}

	answer := fmt.Sprintf("I found %s.", groupedCountPhrase(noun, count))
	if questionRequestsMatchedNames(normalizeSearchText(question)) {
		names := uniqueNamesFromRows(verified.SourceRows)
		if len(names) > 0 {
			answer += " The names listed are " + joinWithAnd(names) + "."
		}
	}
	return answer
}

func renderListAnswer(verified chatVerifiedResult) string {
	values, _ := verified.Value.([]string)
	if len(values) == 0 {
		return "I couldn't find any values to list in the available data."
	}
	return "The records list " + joinWithAnd(values) + "."
}

func renderExtremeAnswer(question string, verified chatVerifiedResult) string {
	if len(verified.SourceRows) == 0 {
		return "I couldn't determine an exact result from the available data."
	}
	fieldLabel := strings.ToLower(strings.TrimSpace(verified.TargetFieldLabel))
	value := strings.TrimSpace(fmt.Sprint(verified.Value))
	names := uniqueNamesFromRows(verified.SourceRows)
	qnorm := normalizeSearchText(question)
	descriptor := "earliest"
	if containsAny(qnorm, "latest", "last", "youngest") {
		descriptor = "latest"
	}
	if value == "" || value == "<nil>" {
		return "I couldn't determine an exact result from the available data."
	}
	if len(names) == 1 {
		return fmt.Sprintf("The %s %s in the matching records is %s for %s.", descriptor, fieldLabel, value, names[0])
	}
	if len(names) > 1 {
		return fmt.Sprintf("The %s %s in the matching records is %s for %s.", descriptor, fieldLabel, value, joinWithAnd(names))
	}
	return fmt.Sprintf("The %s %s in the matching records is %s.", descriptor, fieldLabel, value)
}

func renderGroupAnswer(question string, verified chatVerifiedResult) string {
	valueMap, ok := verified.Value.(map[string]any)
	if !ok {
		return "I couldn't determine an exact grouped result from the available data."
	}
	values := extractStringListFromAny(valueMap["winning_values"])
	names := extractStringListFromAny(valueMap["winner_names"])
	maxCount, _ := extractIntFromAny(valueMap["max_count"])
	groupBy := strings.TrimSpace(fmt.Sprint(valueMap["group_by"]))
	sortDirection := normalizeSearchText(fmt.Sprint(valueMap["sort_direction"]))
	noun := groupedEventNoun(question)
	valueText := joinWithAnd(values)
	if valueText == "" {
		return "I couldn't determine an exact grouped result from the available data."
	}

	comparisonWord := "highest"
	if sortDirection == "asc" {
		comparisonWord = "lowest"
	}
	answer := ""
	if groupBy == "year" || groupBy == "month" || groupBy == "day" {
		answer = fmt.Sprintf("%s had the %s number of %s, with %s.", valueText, comparisonWord, noun, groupedCountPhrase(noun, maxCount))
	} else {
		answer = fmt.Sprintf("%s had the %s number of %s, with %s.", valueText, comparisonWord, noun, groupedCountPhrase(noun, maxCount))
	}
	if questionRequestsMatchedNames(normalizeSearchText(question)) && len(names) > 0 {
		answer += " The names listed are " + joinWithAnd(names) + "."
	}
	return answer
}

func detectQuestionEventScope(question string) string {
	qnorm := normalizeSearchText(question)
	switch {
	case containsAny(qnorm, "death", "deaths", "die", "died", "dead", "deceased", "cause of death", "death cause"):
		return "death"
	case containsAny(qnorm, "birth", "births", "born"):
		return "birth"
	case containsAny(qnorm, "burial", "burials", "buried"):
		return "burial"
	case containsAny(qnorm, "admission", "admissions", "admitted", "admit"):
		return "admission"
	case containsAny(qnorm, "discharge", "discharges", "discharged"):
		return "discharge"
	default:
		return ""
	}
}

func shouldApplyEventScope(plan chatPlannerOutput, eventScope string) bool {
	if eventScope == "" {
		return false
	}
	switch plan.Intent {
	case "clarify", "describe_subject":
		return false
	default:
		return true
	}
}

func applyEventScope(rows []cachedChatRow, dataset *chatDatasetCacheEntry, eventScope string) []cachedChatRow {
	fieldIDs := dataset.schema.eventFieldIDs[eventScope]
	if len(fieldIDs) == 0 {
		return rows
	}
	filtered := make([]cachedChatRow, 0, len(rows))
	for _, row := range rows {
		if rowMatchesEventScope(row, &dataset.schema, eventScope, fieldIDs) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func rowMatchesEventScope(row cachedChatRow, schema *chatDatasetSchema, eventScope string, fieldIDs []string) bool {
	for _, fieldID := range fieldIDs {
		field, ok := schema.resolveField(fieldID)
		if !ok {
			continue
		}
		raw := strings.TrimSpace(row.valueByField(fieldID, schema))
		if raw == "" {
			continue
		}
		if eventScope == "death" && field.Role == chatFieldRoleBoolean {
			return isTruthy(raw)
		}
		return true
	}
	return false
}

func focusIDsForSession(verified chatVerifiedResult) []int {
	if verified.Status != "ok" {
		return nil
	}
	switch verified.Intent {
	case "describe_subject", "field_lookup", "extreme":
		if len(verified.FocusRowIDs) > 0 && len(verified.FocusRowIDs) <= 4 {
			return verified.FocusRowIDs
		}
	}
	return nil
}

func buildRowClarificationQuestion(rows []cachedChatRow, dataset *chatDatasetCacheEntry) string {
	labels := make([]string, 0, minInt(len(rows), 4))
	for idx := range rows {
		if len(labels) >= 4 {
			break
		}
		name := firstNonEmpty(rows[idx].primaryName(), fmt.Sprintf("row %d", rows[idx].RowID))
		qualifier := rowDisambiguator(rows[idx], dataset)
		if qualifier != "" {
			labels = append(labels, fmt.Sprintf("%s (%s)", name, qualifier))
		} else {
			labels = append(labels, name)
		}
	}
	if len(labels) == 0 {
		return "Which exact record do you mean?"
	}
	return "I found multiple possible matches: " + joinWithAnd(labels) + ". Which one do you mean?"
}

func rowDisambiguator(row cachedChatRow, dataset *chatDatasetCacheEntry) string {
	if community := firstNonEmpty(row.Communities...); community != "" {
		return community
	}
	if school := firstNonEmpty(row.Schools...); school != "" {
		return school
	}
	for _, field := range dataset.schema.Fields {
		if field.Role == chatFieldRoleDate {
			if value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema)); value != "" {
				return value
			}
		}
	}
	return ""
}

func refineClarificationRows(rows []cachedChatRow, dataset *chatDatasetCacheEntry, reply string) []cachedChatRow {
	refined := applyMultiColumnFilter(rows, dataset, reply)
	if len(refined) > 0 {
		return refined
	}

	tokens := strings.Fields(normalizeSearchText(reply))
	refined = rows
	for _, token := range tokens {
		next := applyMultiColumnFilter(refined, dataset, token)
		if len(next) == 0 {
			continue
		}
		refined = next
	}
	return refined
}

func matchSubjectRows(rows []cachedChatRow, subject string) []chatSubjectMatch {
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

func determineEarliestDate(rows []cachedChatRow, fieldID string) (*cachedChatRow, []cachedChatRow, string) {
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

func extractGroupCountKey(row cachedChatRow, field *chatSchemaField, granularity string, schema *chatDatasetSchema) (key string, display string, exact bool, present bool) {
	raw := strings.TrimSpace(row.valueByField(field.ID, schema))
	if raw == "" {
		return "", "", false, false
	}
	if field.Role != chatFieldRoleDate {
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

func applyFilter(rows []cachedChatRow, field *chatSchemaField, filter chatPlannerFilter) []cachedChatRow {
	filtered := make([]cachedChatRow, 0, len(rows))
	for _, row := range rows {
		if rowMatchesFilter(row, field, filter) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func rowMatchesFilter(row cachedChatRow, field *chatSchemaField, filter chatPlannerFilter) bool {
	raw := row.valueByField(field.ID, nil)
	switch filter.Op {
	case "is_empty":
		return strings.TrimSpace(raw) == ""
	case "is_not_empty":
		return strings.TrimSpace(raw) != ""
	}
	switch field.Role {
	case chatFieldRoleBoolean:
		switch filter.Op {
		case "yes":
			return isTruthy(raw)
		case "no":
			return isFalsy(raw)
		}
	case chatFieldRoleNumber:
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
	case chatFieldRoleDate:
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
	case chatFieldRoleCommunity, chatFieldRoleSchool, chatFieldRoleLocation:
		switch filter.Op {
		case "eq", "contains":
			return scopedLocationMatch(raw, filter.Value)
		case "yes":
			return isTruthy(raw)
		case "no":
			return isFalsy(raw)
		}
	default:
		switch filter.Op {
		case "eq":
			if isLocationLikeField(field) {
				return scopedLocationMatch(raw, filter.Value)
			}
			return normalizeSearchText(raw) == normalizeSearchText(filter.Value)
		case "contains":
			if isLocationLikeField(field) {
				return scopedLocationMatch(raw, filter.Value)
			}
			return strings.Contains(normalizeSearchText(raw), normalizeSearchText(filter.Value))
		case "yes":
			return isTruthy(raw)
		case "no":
			return isFalsy(raw)
		}
	}
	return false
}

func applyMultiColumnFilter(rows []cachedChatRow, dataset *chatDatasetCacheEntry, keyword string) []cachedChatRow {
	keywordNorm := normalizeSearchText(keyword)
	if keywordNorm == "" {
		return rows
	}

	filtered := make([]cachedChatRow, 0, len(rows))
	for _, row := range rows {
		if valueMatchesSearchTerm(strings.Join(row.SearchTokens, " "), keywordNorm) {
			filtered = append(filtered, row)
			continue
		}
		found := false
		for _, field := range dataset.schema.Fields {
			if !field.Searchable {
				continue
			}
			value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
			if value == "" {
				continue
			}
			if valueMatchesSearchTerm(normalizeSearchText(value), keywordNorm) {
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
	if len(token) <= 4 {
		return token
	}
	root := token
	suffixes := []string{"ingly", "edly", "ation", "ition", "ments", "ment", "ings", "ness", "tion", "sion", "ing", "ers", "ies", "ied", "est", "ous", "ive", "ful", "ed", "es", "s"}
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

func scopedLocationMatch(raw, target string) bool {
	rawNorm := normalizeSearchText(raw)
	targetNorm := normalizeSearchText(target)
	if rawNorm == "" || targetNorm == "" {
		return false
	}
	if rawNorm == targetNorm || strings.Contains(rawNorm, targetNorm) || strings.Contains(targetNorm, rawNorm) {
		return true
	}
	score := scoreLocationMatch(targetNorm, rawNorm)
	if reverse := scoreLocationMatch(rawNorm, targetNorm); reverse > score {
		score = reverse
	}
	return score >= 0.45
}

func applyPreferredLocationFilter(rows []cachedChatRow, dataset *chatDatasetCacheEntry, preferredField *chatSchemaField, target string) []cachedChatRow {
	groups := map[string][]*chatSchemaField{
		"school":    {},
		"community": {},
		"other":     {},
	}
	for idx := range dataset.schema.Fields {
		field := &dataset.schema.Fields[idx]
		if !isLocationLikeField(field) {
			continue
		}
		groups[locationFieldCategory(field)] = append(groups[locationFieldCategory(field)], field)
	}

	order := []string{"school", "community", "other"}
	switch locationFieldCategory(preferredField) {
	case "community":
		order = []string{"community", "school", "other"}
	case "school":
		order = []string{"school", "community", "other"}
	}

	for _, category := range order {
		filtered := applyLocationFields(rows, dataset, groups[category], target)
		if len(filtered) > 0 {
			return filtered
		}
	}
	return nil
}

func applyLocationFields(rows []cachedChatRow, dataset *chatDatasetCacheEntry, fields []*chatSchemaField, target string) []cachedChatRow {
	if len(fields) == 0 {
		return nil
	}
	filtered := make([]cachedChatRow, 0, len(rows))
	for _, row := range rows {
		for _, field := range fields {
			if scopedLocationMatch(row.valueByField(field.ID, &dataset.schema), target) {
				filtered = append(filtered, row)
				break
			}
		}
	}
	return filtered
}

func scoreLocationMatch(questionNorm, locationNorm string) float64 {
	if strings.Contains(questionNorm, locationNorm) {
		return 1.0
	}
	locationWords := nonGenericLocationTokens(locationNorm)
	if len(locationWords) == 0 {
		return 0
	}
	matchedWords := 0
	for _, word := range locationWords {
		if len(word) >= 4 && strings.Contains(questionNorm, word) {
			matchedWords++
		}
	}
	if matchedWords > 0 {
		score := float64(matchedWords) / float64(len(locationWords))
		if strings.Contains(questionNorm, locationWords[0]) {
			score += 0.2
		}
		if score > 1 {
			score = 1
		}
		return score
	}
	return 0
}

func inferQuestionFilters(question string, dataset *chatDatasetCacheEntry, existing []chatPlannerFilter) []chatPlannerFilter {
	if dataset == nil {
		return existing
	}
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

	if filter, ok := inferMissingFieldFilter(question, dataset, seenFields); ok {
		return append(filters, filter)
	}

	type inferredCandidate struct {
		field *chatSchemaField
		value string
		score float64
	}
	best := inferredCandidate{}
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
				best = inferredCandidate{field: field, value: value, score: score}
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
		"a": {}, "about": {}, "after": {}, "all": {}, "an": {}, "and": {}, "any": {}, "are": {}, "at": {}, "before": {},
		"birth": {}, "births": {}, "by": {}, "child": {}, "children": {}, "community": {}, "count": {}, "date": {}, "dates": {},
		"death": {}, "deaths": {}, "describe": {}, "did": {}, "do": {}, "does": {}, "during": {}, "earliest": {}, "find": {},
		"first": {}, "for": {}, "from": {}, "had": {}, "has": {}, "have": {}, "highest": {}, "how": {}, "in": {}, "information": {},
		"institution": {}, "is": {}, "it": {}, "its": {}, "last": {}, "latest": {}, "least": {}, "list": {}, "many": {}, "month": {},
		"months": {}, "most": {}, "much": {}, "number": {}, "occur": {}, "occurred": {}, "of": {}, "on": {}, "or": {}, "records": {},
		"reserve": {}, "reserves": {}, "school": {}, "show": {}, "student": {}, "students": {}, "tell": {}, "that": {}, "the": {},
		"their": {}, "there": {}, "these": {}, "they": {}, "this": {}, "those": {}, "total": {}, "was": {}, "were": {}, "what": {},
		"when": {}, "where": {}, "which": {}, "who": {}, "with": {}, "year": {}, "years": {}, "name": {}, "names": {},
		"without": {}, "missing": {}, "blank": {}, "empty": {}, "not": {},
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
		dest[token] = struct{}{}
	}
}

func detectGroupedCountSpec(questionNorm string) (string, string, bool) {
	if questionNorm == "" {
		return "", "", false
	}
	if containsAny(questionNorm, "earliest", "latest", "first", "last", "oldest", "youngest") {
		return "", "", false
	}

	direction := "desc"
	if containsAny(questionNorm, "least", "lowest", "fewest", "smallest number") {
		direction = "asc"
	} else if !containsAny(questionNorm, "most", "highest", "largest number", "greatest number", "maximum", "max") {
		return "", "", false
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
		return "", "", false
	}

	if !containsAny(questionNorm, "death", "deaths", "died", "birth", "births", "born", "burial", "buried", "admission", "admissions", "admitted", "discharge", "discharges", "occur", "occurred") {
		return "", "", false
	}
	return granularity, direction, true
}

func questionRequestsMatchedNames(questionNorm string) bool {
	return containsAny(
		questionNorm,
		"what are the names",
		"what were the names",
		"names of those",
		"their names",
		"which children",
		"who were the children",
		"who are the children",
		"who were they",
	)
}

func questionAsksForMissingField(questionNorm string) bool {
	return containsAny(
		questionNorm,
		"does not have",
		"do not have",
		"doesnt have",
		"dont have",
		"without",
		"missing",
		"blank",
		"empty",
		"not have",
	)
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
	if session.Pending != nil && isLikelyClarificationReply(qnorm) {
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
	return containsAny(questionNorm, "did any", "were there any", "are there any", "is there any", "was there any", "have any")
}

func hasSessionPronoun(questionNorm string) bool {
	return containsAny(questionNorm, " he ", " she ", " they ", " them ", " him ", " her ", " his ", " their ", " same person", " same one", " that person", " that one", " this one", " this person")
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

func extractStringListFromAny(value any) []string {
	switch v := value.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				out = append(out, strings.TrimSpace(text))
			}
		}
		return out
	}
	return nil
}

func extractIntFromAny(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		i, err := v.Int64()
		return int(i), err == nil
	}
	return 0, false
}

func uniqueNamesFromRows(rows []cachedChatRow) []string {
	names := make([]string, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		name := strings.TrimSpace(row.primaryName())
		if name == "" {
			continue
		}
		key := normalizeSearchText(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		names = append(names, name)
	}
	return names
}

func cloneRows(rows []cachedChatRow) []cachedChatRow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]cachedChatRow, len(rows))
	copy(out, rows)
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
	configJSON, configUpdatedAt, err := cs.loadChatConfigJSON(file.Filename)
	if err != nil {
		return nil, err
	}
	cacheKey := chatDatasetCacheKey(file.ID, file.Version, configUpdatedAt)
	if cached, ok := cs.datasetCache.Load(cacheKey); ok {
		if entry, ok := cached.(*chatDatasetCacheEntry); ok {
			return entry, nil
		}
	}

	var records []chatRowRecord
	if err := cs.DB.Table("file_data fd").
		Select(`
			fd.id AS row_id,
			fd.row_data,
			fd.updated_at AS row_updated_at,
			fdn.row_data_normalized,
			fdn.search_text,
			fdn.canonical_name,
			fdn.canonical_community,
			fdn.canonical_school,
			fdn.status,
			fdn.source_updated_at
		`).
		Joins("LEFT JOIN file_data_normalized fdn ON fdn.source_row_id = fd.id").
		Where("fd.file_id = ? AND fd.version = ?", file.ID, file.Version).
		Order("fd.id ASC").
		Scan(&records).Error; err != nil {
		return nil, fmt.Errorf("file data not found: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("file data not found")
	}

	rawRows := make([]chatRawRow, 0, len(records))
	for _, record := range records {
		values, err := rowJSONToStrings(record.RowData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal file data: %w", err)
		}
		rawRows = append(rawRows, chatRawRow{
			RowID:              int(record.RowID),
			RowJSON:            string(record.RowData),
			Values:             values,
			NormalizedPayload:  parseNormalizedPayload(record.RowDataNormalized),
			SearchText:         strings.TrimSpace(record.SearchText),
			CanonicalName:      strings.TrimSpace(record.CanonicalName),
			CanonicalCommunity: strings.TrimSpace(record.CanonicalCommunity),
			CanonicalSchool:    strings.TrimSpace(record.CanonicalSchool),
		})
	}

	columns := extractOrderedColumns(file.ColumnsOrder, rawRows)
	schema := buildChatSchema(columns, rawRows, configJSON)
	cachedRows := make([]cachedChatRow, 0, len(rawRows))
	rowByID := make(map[int]*cachedChatRow, len(rawRows))
	for _, rawRow := range rawRows {
		row := buildCachedChatRow(rawRow, &schema)
		cachedRows = append(cachedRows, row)
		rowByID[row.RowID] = &cachedRows[len(cachedRows)-1]
	}

	entry := &chatDatasetCacheEntry{
		FileID:   file.ID,
		Filename: file.Filename,
		Version:  file.Version,
		ConfigAt: configUpdatedAt,
		rows:     cachedRows,
		rowByID:  rowByID,
		schema:   schema,
	}

	actual, _ := cs.datasetCache.LoadOrStore(cacheKey, entry)
	if cached, ok := actual.(*chatDatasetCacheEntry); ok {
		return cached, nil
	}
	return entry, nil
}

func (cs *ChatService) loadChatConfigJSON(filename string) ([]byte, time.Time, error) {
	service := dc.DataConfigService{DB: cs.DB}
	result, err := service.GetByFileNameIfModified(filename, nil)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "record not found") {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, err
	}
	if result == nil || result.Config == nil {
		return nil, time.Time{}, nil
	}
	return []byte(result.Config.Config), result.Config.UpdatedAt.UTC(), nil
}

func chatDatasetCacheKey(fileID uint, version int, configAt time.Time) string {
	stamp := "nocfg"
	if !configAt.IsZero() {
		stamp = configAt.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%d:%d:%s", fileID, version, stamp)
}

type chatRawRow struct {
	RowID              int
	RowJSON            string
	Values             map[string]string
	NormalizedPayload  chatNormalizedPayload
	SearchText         string
	CanonicalName      string
	CanonicalCommunity string
	CanonicalSchool    string
}

func parseNormalizedPayload(raw datatypes.JSON) chatNormalizedPayload {
	payload := chatNormalizedPayload{Fields: map[string]chatNormalizedField{}}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return payload
	}
	_ = json.Unmarshal(raw, &payload)
	if payload.Fields == nil {
		payload.Fields = map[string]chatNormalizedField{}
	}
	return payload
}

func rowJSONToStrings(rowData datatypes.JSON) (map[string]string, error) {
	var raw map[string]interface{}
	if err := json.Unmarshal(rowData, &raw); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		out[key] = stringifyRowValue(value)
	}
	return out, nil
}

func stringifyRowValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func extractOrderedColumns(columnsJSON []byte, rows []chatRawRow) []string {
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

func buildChatSchema(columns []string, rows []chatRawRow, configJSON []byte) chatDatasetSchema {
	configLookup := parseChatConfig(configJSON)
	schema := chatDatasetSchema{
		Fields:        make([]chatSchemaField, 0, len(columns)),
		fieldByID:     map[string]*chatSchemaField{},
		fieldByLookup: map[string]*chatSchemaField{},
		eventFieldIDs: map[string][]string{},
	}

	usedIDs := map[string]int{}
	for _, column := range columns {
		meta := configLookup.lookup(column)
		fieldID := uniqueFieldID(column, usedIDs)
		role := meta.Role
		if role == "" {
			role = inferFieldRole(column, rows)
		}

		aliases := inferFieldAliases(column, role)
		aliases = append(aliases, meta.Aliases...)
		aliases = uniqueStrings(aliases)

		searchable := true
		if meta.Searchable != nil {
			searchable = *meta.Searchable
		}
		groupable := role == chatFieldRoleCommunity || role == chatFieldRoleSchool || role == chatFieldRoleLocation || role == chatFieldRoleDate
		if meta.Groupable != nil {
			groupable = *meta.Groupable
		}
		exactOnly := false
		if meta.ExactOnly != nil {
			exactOnly = *meta.ExactOnly
		}

		field := chatSchemaField{
			ID:         fieldID,
			Label:      column,
			Role:       role,
			Aliases:    aliases,
			Examples:   collectFieldExamples(column, rows),
			Searchable: searchable,
			Groupable:  groupable,
			ExactOnly:  exactOnly,
		}
		schema.Fields = append(schema.Fields, field)
		switch role {
		case chatFieldRoleName:
			schema.nameFieldIDs = append(schema.nameFieldIDs, fieldID)
		case chatFieldRoleCommunity:
			schema.communityFieldIDs = append(schema.communityFieldIDs, fieldID)
		case chatFieldRoleSchool:
			schema.schoolFieldIDs = append(schema.schoolFieldIDs, fieldID)
		case chatFieldRoleLocation:
			schema.locationFieldIDs = append(schema.locationFieldIDs, fieldID)
		case chatFieldRoleDate:
			schema.dateFieldIDs = append(schema.dateFieldIDs, fieldID)
		}
		for _, event := range inferFieldEvents(field) {
			schema.eventFieldIDs[event] = append(schema.eventFieldIDs[event], fieldID)
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
			if aliasNorm != "" && strings.Contains(qnorm, aliasNorm) && len(aliasNorm) > score {
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
			if aliasNorm != "" && strings.Contains(qnorm, aliasNorm) {
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
	return schema.defaultExtremeField(qnorm)
}

func (schema *chatDatasetSchema) defaultExtremeField(questionNorm string) *chatSchemaField {
	if containsAny(questionNorm, "oldest", "youngest") {
		for idx := range schema.Fields {
			field := &schema.Fields[idx]
			if field.Role == chatFieldRoleNumber && strings.Contains(strings.ToLower(field.Label), "age") {
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

func (schema *chatDatasetSchema) bestCommunityFieldForQuestion(question string) *chatSchemaField {
	qnorm := normalizeSearchText(question)
	bestScore := -1
	var best *chatSchemaField
	for idx := range schema.Fields {
		field := &schema.Fields[idx]
		if field.Role != chatFieldRoleCommunity {
			continue
		}
		score := 10
		blob := fieldSearchBlob(field)
		if containsAny(qnorm, "community", "communities") && containsAny(blob, "community") {
			score += 40
		}
		if containsAny(qnorm, "reserve", "reserves") && containsAny(blob, "reserve") {
			score += 40
		}
		if containsAny(qnorm, "first nation", "first nations") && containsAny(blob, "first nation") {
			score += 40
		}
		for _, alias := range field.Aliases {
			aliasNorm := normalizeSearchText(alias)
			if aliasNorm != "" && strings.Contains(qnorm, aliasNorm) {
				score += len(aliasNorm)
			}
		}
		if score > bestScore {
			bestScore = score
			best = field
		}
	}
	return best
}

func (schema *chatDatasetSchema) bestSchoolFieldForQuestion(question string) *chatSchemaField {
	qnorm := normalizeSearchText(question)
	bestScore := -1
	var best *chatSchemaField
	for idx := range schema.Fields {
		field := &schema.Fields[idx]
		if locationFieldCategory(field) != "school" {
			continue
		}
		score := 10
		blob := fieldSearchBlob(field)
		if containsAny(qnorm, "school", "residential school", "institution") && containsAny(blob, "school", "institution", "residential") {
			score += 50
		}
		for _, alias := range field.Aliases {
			aliasNorm := normalizeSearchText(alias)
			if aliasNorm != "" && strings.Contains(qnorm, aliasNorm) {
				score += len(aliasNorm)
			}
		}
		if score > bestScore {
			bestScore = score
			best = field
		}
	}
	return best
}

func (schema *chatDatasetSchema) bestLocationFieldForQuestion(question string) *chatSchemaField {
	qnorm := normalizeSearchText(question)
	bestScore := -1
	var best *chatSchemaField
	for idx := range schema.Fields {
		field := &schema.Fields[idx]
		if field.Role != chatFieldRoleLocation {
			continue
		}
		score := 10
		blob := fieldSearchBlob(field)
		if containsAny(qnorm, "location", "locations", "place", "places") && containsAny(blob, "location", "place") {
			score += 40
		}
		for _, alias := range field.Aliases {
			aliasNorm := normalizeSearchText(alias)
			if aliasNorm != "" && strings.Contains(qnorm, aliasNorm) {
				score += len(aliasNorm)
			}
		}
		if score > bestScore {
			bestScore = score
			best = field
		}
	}
	return best
}

func (schema *chatDatasetSchema) describeForPlanner() string {
	lines := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		line := fmt.Sprintf("- id=%s | label=%s | role=%s", field.ID, field.Label, field.Role)
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

func fieldSearchBlob(field *chatSchemaField) string {
	if field == nil {
		return ""
	}
	parts := []string{field.ID, field.Label}
	parts = append(parts, field.Aliases...)
	return normalizeSearchText(strings.Join(parts, " "))
}

func inferFieldRole(label string, rows []chatRawRow) chatFieldRole {
	lower := strings.ToLower(strings.TrimSpace(label))
	switch {
	case strings.Contains(lower, "deceased"):
		return chatFieldRoleBoolean
	case strings.Contains(lower, "community"), strings.Contains(lower, "reserve"), strings.Contains(lower, "first nation"), strings.Contains(lower, "home"):
		return chatFieldRoleCommunity
	case strings.Contains(lower, "school"), strings.Contains(lower, "residential"), strings.Contains(lower, "institution"):
		return chatFieldRoleSchool
	case strings.Contains(lower, "place of death"),
		strings.Contains(lower, "location of death"),
		strings.Contains(lower, "death location"),
		strings.Contains(lower, "death place"),
		strings.Contains(lower, "place died"),
		strings.Contains(lower, "place of burial"),
		strings.Contains(lower, "burial place"),
		(strings.Contains(lower, "location") && !strings.Contains(lower, "date")),
		(strings.Contains(lower, "place") && !strings.Contains(lower, "date")):
		return chatFieldRoleLocation
	case strings.Contains(lower, "name") && !strings.Contains(lower, "parent") && !strings.Contains(lower, "sibling"):
		return chatFieldRoleName
	case strings.Contains(lower, "date"), strings.Contains(lower, "birth"), strings.Contains(lower, "death"), strings.Contains(lower, "burial"), strings.Contains(lower, "admit"), strings.Contains(lower, "discharge"):
		return chatFieldRoleDate
	case strings.Contains(lower, "age"), strings.Contains(lower, "number"), lower == "no.", lower == "no":
		return chatFieldRoleNumber
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
		if _, ok := parseNumericValue(value); ok {
			numberCount++
		}
		if isTruthy(value) || isFalsy(value) {
			boolCount++
		}
		tv := parseTemporalValue(value)
		if tv.Kind != temporalKindUnknown && tv.Kind != temporalKindMalformed {
			dateCount++
		}
		if sampleCount >= 12 {
			break
		}
	}
	switch {
	case sampleCount > 0 && dateCount*2 >= sampleCount:
		return chatFieldRoleDate
	case sampleCount > 0 && numberCount*2 >= sampleCount:
		return chatFieldRoleNumber
	case sampleCount > 0 && boolCount*2 >= sampleCount:
		return chatFieldRoleBoolean
	default:
		return chatFieldRoleText
	}
}

func inferFieldAliases(label string, role chatFieldRole) []string {
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

	switch role {
	case chatFieldRoleName:
		add("name", "person", "student", "child")
	case chatFieldRoleDate:
		add("date")
	}
	return aliases
}

func inferFieldEvents(field chatSchemaField) []string {
	blob := fieldSearchBlob(&field)
	events := make([]string, 0, 2)
	add := func(event string) {
		for _, existing := range events {
			if existing == event {
				return
			}
		}
		events = append(events, event)
	}
	if containsAny(blob, "death", "deaths", "died", "deceased", "cause of death", "death cause", "death factor", "place of death", "death location") {
		add("death")
	}
	if containsAny(blob, "birth", "births", "born") {
		add("birth")
	}
	if containsAny(blob, "burial", "buried") {
		add("burial")
	}
	if containsAny(blob, "admission", "admissions", "admitted", "admit") {
		add("admission")
	}
	if containsAny(blob, "discharge", "discharges", "discharged") {
		add("discharge")
	}
	return events
}

func collectFieldExamples(label string, rows []chatRawRow) []string {
	counts := map[string]int{}
	for _, row := range rows {
		value := strings.TrimSpace(row.Values[label])
		if value == "" {
			continue
		}
		counts[value]++
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
	limit := minInt(len(items), 5)
	examples := make([]string, 0, limit)
	for _, item := range items[:limit] {
		examples = append(examples, item.value)
	}
	return examples
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

func buildCachedChatRow(rawRow chatRawRow, schema *chatDatasetSchema) cachedChatRow {
	row := cachedChatRow{
		RowID:      rawRow.RowID,
		RowJSON:    rawRow.RowJSON,
		Values:     rawRow.Values,
		Normalized: map[string]string{},
		Tokens:     map[string][]string{},
		Temporal:   map[string]temporalValue{},
	}

	searchTokens := make([]string, 0)
	for _, field := range schema.Fields {
		rawValue := strings.TrimSpace(rawRow.Values[field.Label])
		normValue := normalizeSearchText(rawValue)
		tokens := tokenizeSearchValue(rawValue)

		if normalizedField, ok := rawRow.NormalizedPayload.Fields[field.Label]; ok {
			if strings.TrimSpace(normalizedField.Normalized) != "" {
				normValue = strings.TrimSpace(normalizedField.Normalized)
			}
			if len(normalizedField.Tokens) > 0 {
				tokens = cloneStringSlice(normalizedField.Tokens)
			}
		}

		row.Normalized[field.ID] = normValue
		row.Tokens[field.ID] = uniqueStrings(tokens)
		searchTokens = append(searchTokens, tokens...)

		if field.Role == chatFieldRoleDate && rawValue != "" {
			row.Temporal[field.ID] = parseTemporalValue(rawValue)
		}
	}

	row.SearchTokens = uniqueStrings(append(searchTokens, strings.Fields(normalizeSearchText(rawRow.SearchText))...))
	row.Names = uniqueStrings(append(cloneStringSlice(rawRow.NormalizedPayload.Names), collectRowNames(row, schema)...))
	row.Communities = uniqueStrings(append(cloneStringSlice(rawRow.NormalizedPayload.Communities), normalizeNonEmpty(rawRow.CanonicalCommunity)...))
	row.Schools = uniqueStrings(append(cloneStringSlice(rawRow.NormalizedPayload.Schools), normalizeNonEmpty(rawRow.CanonicalSchool)...))
	row.Locations = uniqueStrings(cloneStringSlice(rawRow.NormalizedPayload.Locations))

	if len(row.Communities) == 0 {
		for _, fieldID := range schema.communityFieldIDs {
			if value := normalizeSearchText(row.valueByField(fieldID, schema)); value != "" {
				row.Communities = append(row.Communities, value)
			}
		}
	}
	if len(row.Schools) == 0 {
		for _, fieldID := range schema.schoolFieldIDs {
			if value := normalizeSearchText(row.valueByField(fieldID, schema)); value != "" {
				row.Schools = append(row.Schools, value)
			}
		}
	}
	if len(row.Locations) == 0 {
		for _, fieldID := range schema.locationFieldIDs {
			if value := normalizeSearchText(row.valueByField(fieldID, schema)); value != "" {
				row.Locations = append(row.Locations, value)
			}
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
	return names
}

func normalizeNonEmpty(value string) []string {
	value = normalizeSearchText(value)
	if value == "" {
		return nil
	}
	return []string{value}
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
		matched := false
		for _, community := range row.Communities {
			if _, ok := allowed[normalizeSearchText(community)]; ok {
				matched = true
				break
			}
		}
		if matched {
			out = append(out, row)
		}
	}
	return out
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
			if raw != "" {
				out["target_field"] = field.Label
				out["target_value"] = raw
			}
			if tv, ok := row.temporalByField(field.ID); ok {
				out["target_value_meta"] = renderTemporalMetadata(tv)
			}
		}
	}

	if includeAll {
		fields := map[string]any{}
		for _, field := range dataset.schema.Fields {
			value := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
			if value == "" {
				continue
			}
			fields[field.Label] = value
			if tv, ok := row.temporalByField(field.ID); ok {
				fields[field.Label+"__meta"] = renderTemporalMetadata(tv)
			}
		}
		if len(fields) > 0 {
			out["fields"] = fields
		}
	}

	return out
}

func (dataset *chatDatasetCacheEntry) bestFilterValueForQuestion(field *chatSchemaField, questionNorm string) (string, float64, bool) {
	bestRaw := ""
	bestScore := 0.0
	seen := map[string]struct{}{}

	for _, example := range field.Examples {
		raw := strings.TrimSpace(example)
		norm := normalizeSearchText(raw)
		if len(norm) < 3 {
			continue
		}
		if isGenericFilterValue(norm) {
			continue
		}
		seen[norm] = struct{}{}
		score := scoreLocationMatch(questionNorm, norm)
		if score > bestScore {
			bestScore = score
			bestRaw = raw
		}
	}

	for _, row := range dataset.rows {
		raw := strings.TrimSpace(row.valueByField(field.ID, &dataset.schema))
		norm := normalizeSearchText(raw)
		if len(norm) < 3 || len(norm) > 80 {
			continue
		}
		if isGenericFilterValue(norm) {
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
	if bestScore >= 0.4 {
		return bestRaw, bestScore, true
	}
	return "", 0, false
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

func parseChatConfig(configJSON []byte) chatConfigLookup {
	lookup := chatConfigLookup{byKey: map[string]*chatConfigFieldMeta{}}
	if len(strings.TrimSpace(string(configJSON))) == 0 {
		return lookup
	}

	var raw map[string]any
	if err := json.Unmarshal(configJSON, &raw); err != nil {
		return lookup
	}

	for _, key := range []string{"date_fields", "dates"} {
		lookup.applyRoleList(chatFieldRoleDate, raw[key])
	}
	for _, key := range []string{"name_fields", "names"} {
		lookup.applyRoleList(chatFieldRoleName, raw[key])
	}
	for _, key := range []string{"community_fields", "reserve_fields"} {
		lookup.applyRoleList(chatFieldRoleCommunity, raw[key])
	}
	for _, key := range []string{"school_fields"} {
		lookup.applyRoleList(chatFieldRoleSchool, raw[key])
	}
	for _, key := range []string{"location_fields", "place_fields"} {
		lookup.applyRoleList(chatFieldRoleLocation, raw[key])
	}
	lookup.applyFlagList("searchable_fields", raw["searchable_fields"], func(meta *chatConfigFieldMeta) { v := true; meta.Searchable = &v })
	lookup.applyFlagList("groupable_fields", raw["groupable_fields"], func(meta *chatConfigFieldMeta) { v := true; meta.Groupable = &v })
	lookup.applyFlagList("exact_match_fields", raw["exact_match_fields"], func(meta *chatConfigFieldMeta) { v := true; meta.ExactOnly = &v })
	lookup.applyFlagList("exact_fields", raw["exact_fields"], func(meta *chatConfigFieldMeta) { v := true; meta.ExactOnly = &v })

	lookup.applyAliasMap(raw["aliases"])
	lookup.applyAliasMap(raw["field_aliases"])
	lookup.applyRoleMap(raw["field_roles"])
	lookup.applyRoleMap(raw["roles"])
	lookup.applyFieldConfigObject(raw["fields"])
	lookup.applyFieldConfigObject(raw["columns"])
	lookup.applyFieldConfigObject(raw["schema"])

	return lookup
}

func (lookup chatConfigLookup) ensure(field string) *chatConfigFieldMeta {
	key := normalizeSearchText(field)
	if key == "" {
		return &chatConfigFieldMeta{}
	}
	if existing, ok := lookup.byKey[key]; ok {
		return existing
	}
	meta := &chatConfigFieldMeta{}
	lookup.byKey[key] = meta
	return meta
}

func (lookup chatConfigLookup) lookup(field string) chatConfigFieldMeta {
	key := normalizeSearchText(field)
	if existing, ok := lookup.byKey[key]; ok && existing != nil {
		return *existing
	}
	return chatConfigFieldMeta{}
}

func (lookup chatConfigLookup) applyRoleList(role chatFieldRole, value any) {
	for _, field := range extractStringList(value) {
		meta := lookup.ensure(field)
		meta.Role = role
	}
}

func (lookup chatConfigLookup) applyFlagList(_ string, value any, apply func(*chatConfigFieldMeta)) {
	for _, field := range extractStringList(value) {
		meta := lookup.ensure(field)
		apply(meta)
	}
}

func (lookup chatConfigLookup) applyAliasMap(value any) {
	items, ok := value.(map[string]any)
	if !ok {
		return
	}
	for key, rawAliases := range items {
		meta := lookup.ensure(key)
		meta.Aliases = append(meta.Aliases, extractStringList(rawAliases)...)
	}
}

func (lookup chatConfigLookup) applyRoleMap(value any) {
	items, ok := value.(map[string]any)
	if !ok {
		return
	}
	for key, rawRole := range items {
		role := parseConfiguredRole(rawRole)
		if role == "" {
			continue
		}
		meta := lookup.ensure(key)
		meta.Role = role
	}
}

func (lookup chatConfigLookup) applyFieldConfigObject(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if fieldMap, ok := item.(map[string]any); ok {
				fieldName := firstNonEmpty(extractFieldReference(fieldMap), key)
				meta := lookup.ensure(fieldName)
				mergeFieldConfig(meta, fieldMap)
			}
		}
	case []any:
		for _, item := range typed {
			fieldMap, ok := item.(map[string]any)
			if !ok {
				continue
			}
			fieldName := extractFieldReference(fieldMap)
			if fieldName == "" {
				continue
			}
			meta := lookup.ensure(fieldName)
			mergeFieldConfig(meta, fieldMap)
		}
	}
}

func mergeFieldConfig(meta *chatConfigFieldMeta, fieldMap map[string]any) {
	if meta == nil {
		return
	}
	if role := parseConfiguredRole(fieldMap["role"]); role != "" {
		meta.Role = role
	}
	meta.Aliases = append(meta.Aliases, extractStringList(fieldMap["aliases"])...)
	if value, ok := extractBool(fieldMap["searchable"]); ok {
		meta.Searchable = &value
	}
	if value, ok := extractBool(fieldMap["groupable"]); ok {
		meta.Groupable = &value
	}
	if value, ok := extractBool(fieldMap["exact_match"]); ok {
		meta.ExactOnly = &value
	}
	if value, ok := extractBool(fieldMap["exact_only"]); ok {
		meta.ExactOnly = &value
	}
}

func extractFieldReference(fieldMap map[string]any) string {
	for _, key := range []string{"field", "column", "name", "label", "id", "source", "field_name"} {
		if value, ok := fieldMap[key]; ok {
			if text := strings.TrimSpace(fmt.Sprint(value)); text != "" {
				return text
			}
		}
	}
	return ""
}

func parseConfiguredRole(value any) chatFieldRole {
	role := normalizeSearchText(fmt.Sprint(value))
	switch role {
	case "name", "person", "student", "child":
		return chatFieldRoleName
	case "date", "death date", "birth date":
		return chatFieldRoleDate
	case "community", "reserve", "first nation":
		return chatFieldRoleCommunity
	case "school", "institution":
		return chatFieldRoleSchool
	case "location", "place":
		return chatFieldRoleLocation
	case "number", "numeric":
		return chatFieldRoleNumber
	case "boolean", "bool":
		return chatFieldRoleBoolean
	case "text", "notes":
		return chatFieldRoleText
	default:
		return ""
	}
}

func extractStringList(value any) []string {
	switch typed := value.(type) {
	case string:
		parts := strings.Split(typed, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	case []string:
		return cloneStringSlice(typed)
	default:
		return nil
	}
}

func extractBool(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch normalizeSearchText(typed) {
		case "true", "yes", "1":
			return true, true
		case "false", "no", "0":
			return false, true
		}
	}
	return false, false
}

func fieldSupportsValueInference(field *chatSchemaField) bool {
	if field == nil {
		return false
	}
	switch field.Role {
	case chatFieldRoleCommunity, chatFieldRoleSchool, chatFieldRoleLocation:
		return true
	case chatFieldRoleText:
		blob := fieldSearchBlob(field)
		return containsAny(blob, "school", "institution", "community", "reserve", "location", "place")
	default:
		return false
	}
}

func inferMissingFieldFilter(question string, dataset *chatDatasetCacheEntry, seenFields map[string]struct{}) (chatPlannerFilter, bool) {
	if dataset == nil {
		return chatPlannerFilter{}, false
	}
	qnorm := normalizeSearchText(question)
	if qnorm == "" || !questionAsksForMissingField(qnorm) {
		return chatPlannerFilter{}, false
	}

	field := dataset.schema.bestFieldForQuestion(question)
	if field == nil {
		switch {
		case containsAny(qnorm, "community", "reserve", "first nation", "home"):
			field = dataset.schema.bestCommunityFieldForQuestion(question)
		case containsAny(qnorm, "school", "institution", "residential"):
			field = dataset.schema.bestSchoolFieldForQuestion(question)
		case containsAny(qnorm, "location", "place"):
			field = dataset.schema.bestLocationFieldForQuestion(question)
		}
	}
	if field == nil {
		return chatPlannerFilter{}, false
	}
	if _, seen := seenFields[field.ID]; seen {
		return chatPlannerFilter{}, false
	}
	return chatPlannerFilter{FieldID: field.ID, Op: "is_empty"}, true
}

func isGenericFilterValue(norm string) bool {
	if norm == "" {
		return true
	}
	generic := map[string]struct{}{
		"community": {}, "communities": {}, "reserve": {}, "reserves": {}, "first nation": {}, "first nations": {},
		"school": {}, "schools": {}, "institution": {}, "institutions": {}, "location": {}, "locations": {},
		"place": {}, "places": {}, "name": {}, "names": {}, "student": {}, "students": {}, "child": {}, "children": {},
		"record": {}, "records": {}, "date": {}, "dates": {}, "death": {}, "deaths": {}, "birth": {}, "births": {},
		"admission": {}, "admissions": {}, "discharge": {}, "discharges": {},
	}
	if _, ok := generic[norm]; ok {
		return true
	}
	tokens := strings.Fields(norm)
	if len(tokens) == 0 {
		return true
	}
	for _, token := range tokens {
		if _, ok := generic[token]; !ok {
			return false
		}
	}
	return true
}

func filterFieldPreference(field *chatSchemaField, questionNorm string) float64 {
	if field == nil {
		return 0
	}
	blob := fieldSearchBlob(field)
	score := 0.0
	switch field.Role {
	case chatFieldRoleSchool:
		score += 0.25
	case chatFieldRoleCommunity:
		score += 0.10
	case chatFieldRoleLocation:
		score -= 0.10
	}
	if containsAny(questionNorm, "school", "residential school", "institution") && containsAny(blob, "school", "institution", "residential") {
		score += 0.35
	}
	if containsAny(questionNorm, "community", "reserve", "home") && containsAny(blob, "community", "reserve", "home") {
		score += 0.25
	}
	if containsAny(questionNorm, "location", "locations", "place", "places", "where") && containsAny(blob, "location", "place", "where") {
		score += 0.30
	}
	if !containsAny(questionNorm, "location", "locations", "place", "places", "where") {
		if field.Role == chatFieldRoleSchool {
			score += 0.30
		}
		if field.Role == chatFieldRoleLocation {
			score -= 0.15
		}
	}
	return score
}

func nonGenericLocationTokens(locationNorm string) []string {
	tokens := strings.Fields(locationNorm)
	if len(tokens) == 0 {
		return nil
	}
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if !isGenericFilterValue(token) {
			out = append(out, token)
		}
	}
	if len(out) == 0 {
		return tokens
	}
	return out
}

func isLocationLikeField(field *chatSchemaField) bool {
	if field == nil {
		return false
	}
	switch field.Role {
	case chatFieldRoleCommunity, chatFieldRoleSchool, chatFieldRoleLocation:
		return true
	default:
		return fieldSupportsValueInference(field)
	}
}

func locationFieldCategory(field *chatSchemaField) string {
	if field == nil {
		return "other"
	}
	switch field.Role {
	case chatFieldRoleSchool:
		return "school"
	case chatFieldRoleCommunity:
		return "community"
	default:
		return "other"
	}
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

func tokenizeSearchValue(value string) []string {
	normalized := normalizeSearchText(value)
	if normalized == "" {
		return nil
	}
	parts := strings.Fields(normalized)
	tokens := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) <= 1 {
			continue
		}
		tokens = append(tokens, part)
		if strings.HasSuffix(part, "ing") && len(part) > 4 {
			tokens = append(tokens, strings.TrimSuffix(part, "ing"))
		}
		if strings.HasSuffix(part, "ed") && len(part) > 3 {
			tokens = append(tokens, strings.TrimSuffix(part, "ed"))
		}
		if strings.HasSuffix(part, "s") && len(part) > 3 {
			tokens = append(tokens, strings.TrimSuffix(part, "s"))
		}
	}
	return uniqueStrings(tokens)
}

func extractJSONObjectCandidate(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "```") {
		raw = strings.TrimPrefix(raw, "```json")
		raw = strings.TrimPrefix(raw, "```JSON")
		raw = strings.TrimPrefix(raw, "```")
		raw = strings.TrimSpace(raw)
		raw = strings.TrimSuffix(raw, "```")
		raw = strings.TrimSpace(raw)
	}
	if json.Valid([]byte(raw)) {
		return raw
	}
	start := strings.IndexByte(raw, '{')
	end := strings.LastIndexByte(raw, '}')
	if start == -1 || end == -1 || end < start {
		return ""
	}
	candidate := strings.TrimSpace(raw[start : end+1])
	if !json.Valid([]byte(candidate)) {
		return ""
	}
	return candidate
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
	if soundsLike(query, candidate) {
		return 0.88
	}

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

	dist := levenshteinDistance(query, candidate)
	maxLen := maxInt(len(query), len(candidate))
	if maxLen == 0 {
		return tokenScore
	}
	editScore := 1 - (float64(dist) / float64(maxLen))
	if maxLen <= 8 && dist <= 2 {
		editScore = math.Max(editScore, 0.85)
	}
	return math.Max(tokenScore, editScore)
}

func soundsLike(name1, name2 string) bool {
	n1 := phoneticallyNormalize(strings.ToLower(strings.TrimSpace(name1)))
	n2 := phoneticallyNormalize(strings.ToLower(strings.TrimSpace(name2)))
	if n1 == n2 && n1 != "" {
		return true
	}
	return levenshteinDistance(name1, name2) <= 2 && len(name1) >= 4
}

func phoneticallyNormalize(s string) string {
	s = strings.NewReplacer(
		"a", "a",
		"e", "a",
		"i", "a",
		"o", "a",
		"u", "a",
	).Replace(strings.ToLower(s))
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
	raw = strings.TrimSpace(strings.ReplaceAll(raw, ",", ""))
	if raw == "" {
		return 0, false
	}
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

func extractSubjectCandidate(question string) string {
	text := strings.TrimSpace(question)
	replacements := []string{
		"tell me about ", "describe ", "information about ", "who is ", "who was ", "what is ", "what was ",
		"when did ", "where did ", "how did ", "which ", "give me details about ",
	}
	lower := strings.ToLower(text)
	for _, prefix := range replacements {
		if strings.HasPrefix(lower, prefix) {
			text = strings.TrimSpace(text[len(prefix):])
			break
		}
	}
	for _, suffix := range []string{" die", " died", " pass away", " date of death", " cause of death", " community", " school"} {
		if strings.HasSuffix(strings.ToLower(text), suffix) {
			text = strings.TrimSpace(text[:len(text)-len(suffix)])
			break
		}
	}
	if len(strings.Fields(text)) > 6 {
		return ""
	}
	return strings.Trim(text, " ?.")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := normalizeSearchText(value)
		if key == "" {
			key = value
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
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
