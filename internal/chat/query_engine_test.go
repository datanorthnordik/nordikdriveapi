package chat

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"google.golang.org/genai"
)

func buildDatasetForQueryEngineTest(t *testing.T, rows []map[string]string) *chatDatasetCacheEntry {
	t.Helper()

	rawRows := make([]chatRawRow, 0, len(rows))
	for idx, row := range rows {
		rowAny := make(map[string]any, len(row))
		for key, value := range row {
			rowAny[key] = value
		}
		rowJSON, err := json.Marshal(rowAny)
		if err != nil {
			t.Fatalf("marshal row %d: %v", idx, err)
		}
		rawRows = append(rawRows, chatRawRow{
			RowID:   idx + 1,
			RowJSON: string(rowJSON),
			Values:  row,
		})
	}

	columns := extractOrderedColumns(nil, rawRows)
	schema := buildChatSchema(columns, rawRows, nil)
	cachedRows := make([]cachedChatRow, 0, len(rawRows))
	rowByID := make(map[int]*cachedChatRow, len(rawRows))
	for _, rawRow := range rawRows {
		row := buildCachedChatRow(rawRow, &schema)
		cachedRows = append(cachedRows, row)
		rowByID[row.RowID] = &cachedRows[len(cachedRows)-1]
	}

	return &chatDatasetCacheEntry{
		rows:    cachedRows,
		rowByID: rowByID,
		schema:  schema,
	}
}

func TestDeterministicChatPlan_GroupCountExtremeByYear(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "Alice", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1890-05-06"},
		{"NAME": "Beatrice", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1890-06-01"},
		{"NAME": "Charlotte", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1891-01-04"},
		{"NAME": "Dora", "SCHOOL": "Wawanosh", "DATE OF DEATH": "1891-02-10"},
	})

	question := "In what years did the highest number of deaths occur at Shingwauk?"
	plan, ok := deterministicChatPlan(question, dataset, &chatSessionState{})
	if !ok {
		t.Fatal("expected deterministic plan")
	}
	if plan.Intent != "group_count_extreme" {
		t.Fatalf("intent = %q want group_count_extreme", plan.Intent)
	}
	if plan.GroupByGranularity != "year" {
		t.Fatalf("granularity = %q want year", plan.GroupByGranularity)
	}
	if len(plan.Filters) != 1 || plan.Filters[0].Value != "Shingwauk" {
		t.Fatalf("expected Shingwauk filter, got %+v", plan.Filters)
	}

	verified := executeChatPlan(plan, question, dataset, dataset.rows, &chatSessionState{})
	if verified.Status != "ok" {
		t.Fatalf("status = %q want ok (notes=%v)", verified.Status, verified.Notes)
	}
	valueMap, ok := verified.Value.(map[string]any)
	if !ok {
		t.Fatalf("unexpected value payload: %#v", verified.Value)
	}
	values := extractStringListFromAny(valueMap["winning_values"])
	if len(values) != 1 || values[0] != "1890" {
		t.Fatalf("unexpected winners: %#v", values)
	}
	maxCount, ok := extractIntFromAny(valueMap["max_count"])
	if !ok || maxCount != 2 {
		t.Fatalf("unexpected max_count: %#v", valueMap["max_count"])
	}
}

func TestExecuteChatPlan_GroupCountExtremeRejectsUnsafeTieBreaker(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "Alice", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1890-05-06"},
		{"NAME": "Beatrice", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1891-06-01"},
		{"NAME": "Charlotte", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1890-1891"},
	})

	plan := chatPlannerOutput{
		Intent:             "group_count_extreme",
		TargetFieldID:      "date_of_death",
		GroupByFieldID:     "date_of_death",
		GroupByGranularity: "year",
		Filters: []chatPlannerFilter{
			{FieldID: "school", Op: "eq", Value: "Shingwauk"},
		},
	}

	verified := executeChatPlan(plan, "Which year had the most deaths at Shingwauk?", dataset, dataset.rows, &chatSessionState{})
	if verified.Status != "cannot_determine_exactly" {
		t.Fatalf("status = %q want cannot_determine_exactly", verified.Status)
	}
}

func TestResolvePendingCandidateClarification_NarrowsToSingleRow(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "John Smith", "First Nation/Community": "Garden River", "SCHOOL": "Shingwauk"},
		{"NAME": "John Smith", "First Nation/Community": "Walpole Island", "SCHOOL": "Wawanosh"},
	})

	session := &chatSessionState{
		Pending: &chatPendingClarification{
			Prompt:           "I found multiple possible matches.",
			Attempts:         1,
			OriginalQuestion: "Tell me about John Smith",
			CandidateRowIDs:  []int{1, 2},
			Plan:             chatPlannerOutput{Intent: "describe_subject", SubjectText: "John Smith"},
		},
	}

	verified, handled := (&ChatService{}).resolvePendingCandidateClarification("Garden River", dataset, dataset.rows, session)
	if !handled {
		t.Fatal("expected clarification handler to run")
	}
	if verified.Status != "ok" {
		t.Fatalf("status = %q want ok", verified.Status)
	}
	if verified.MatchedRowID == nil || *verified.MatchedRowID != 1 {
		t.Fatalf("unexpected matched row: %#v", verified.MatchedRowID)
	}
}

func TestChatService_Chat_DoesNotLeakPriorFocusIntoFreshQuestion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatDatasetQueries(mock)
	ts := time.Now().UTC()
	mock.ExpectQuery(`(?i)select.*from.*file_data fd.*left join.*file_data_normalized`).
		WillReturnRows(sqlmock.NewRows([]string{
			"row_id", "row_data", "row_updated_at", "row_data_normalized", "search_text", "canonical_name", "canonical_community", "canonical_school", "status", "source_updated_at",
		}).
			AddRow(uint(42), `{"NAME":"Audrey Lesage","SCHOOL":"Other School","CAUSE OF DEATH":"Tuberculosis"}`, ts, `{"fields":{"NAME":{"normalized":"audrey lesage","tokens":["audrey","lesage"],"role":"name"},"SCHOOL":{"normalized":"other school","tokens":["other","school"],"role":"school"},"CAUSE OF DEATH":{"normalized":"tuberculosis","tokens":["tuberculosis"],"role":"text"}},"names":["Audrey Lesage"],"schools":["other school"],"search_tokens":["audrey","lesage","tuberculosis"]}`, "audrey lesage tuberculosis", "audrey lesage", "", "other school", "ready", ts).
			AddRow(uint(43), `{"NAME":"Alice","SCHOOL":"Shingwauk Indian Residential School","CAUSE OF DEATH":"Drowned while crossing the river"}`, ts, `{"fields":{"NAME":{"normalized":"alice","tokens":["alice"],"role":"name"},"SCHOOL":{"normalized":"shingwauk indian residential school","tokens":["shingwauk","indian","residential","school"],"role":"school"},"CAUSE OF DEATH":{"normalized":"drowned while crossing the river","tokens":["drowned","crossing","river"],"role":"text"}},"names":["Alice"],"schools":["shingwauk indian residential school"],"search_tokens":["alice","shingwauk","drowned","crossing","river"]}`, "alice shingwauk drowned crossing river", "alice", "", "shingwauk indian residential school", "ready", ts))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = plannerThenAnswerHook(
		t,
		`{"intent":"describe_subject","subject_text":"Audrey Lesage"}`,
		`The records show Audrey Lesage.`,
		nil,
		func(prompt string) {
			if strings.Contains(prompt, `"name": "Audrey Lesage"`) || strings.Contains(prompt, `"name":"Audrey Lesage"`) {
				t.Fatalf("fresh drowning question should not use prior focus row in verified result: %s", prompt)
			}
			if !strings.Contains(prompt, `"name": "Alice"`) && !strings.Contains(prompt, `"name":"Alice"`) {
				t.Fatalf("expected Alice row in verified result: %s", prompt)
			}
		},
	)
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	cs.sessionCache.Store(chatSessionKey(55), &chatSessionState{
		UserKey:     chatSessionKey(55),
		FileID:      1,
		Filename:    "sheet.xlsx",
		Version:     1,
		FocusRowIDs: []int{42},
		UpdatedAt:   timeNowUTC(),
	})

	result, err := cs.ChatForUser(55, "Did any children die from drowning at Shingwauk Indian Residential School?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if strings.TrimSpace(result.Answer) == "" {
		t.Fatalf("expected answer, got %#v", result)
	}
}

func timeNowUTC() time.Time {
	return time.Now().UTC()
}
