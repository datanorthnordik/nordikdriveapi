package chat

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"google.golang.org/genai"
)

func buildDatasetForQueryEngineTest(t *testing.T, rows []map[string]string) *chatDatasetCacheEntry {
	t.Helper()

	rawRows := make([]rawChatRow, 0, len(rows))
	for idx, row := range rows {
		rowAny := make(map[string]any, len(row))
		for key, value := range row {
			rowAny[key] = value
		}
		rowJSON, err := json.Marshal(rowAny)
		if err != nil {
			t.Fatalf("marshal row %d: %v", idx, err)
		}
		rawRows = append(rawRows, rawChatRow{
			RowID:   idx + 1,
			RowJSON: string(rowJSON),
			Values:  row,
		})
	}

	columns := extractOrderedColumns(nil, rawRows)
	schema := buildChatSchema(columns, rawRows)
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

func TestHeuristicChatPlan_GroupCountExtremeByYear(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "Alice", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1890-05-06"},
		{"NAME": "Beatrice", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1890-06-01"},
		{"NAME": "Charlotte", "SCHOOL": "Shingwauk", "DATE OF DEATH": "1891-01-04"},
		{"NAME": "Dora", "SCHOOL": "Wawanosh", "DATE OF DEATH": "1891-02-10"},
	})

	question := "In what years did the highest number of deaths occur at Shingwauk?"
	plan, ok := heuristicChatPlan(question, dataset, &chatSessionState{})
	if !ok {
		t.Fatal("expected heuristic plan")
	}
	if plan.Intent != "group_count_extreme" {
		t.Fatalf("intent = %q want group_count_extreme", plan.Intent)
	}
	if plan.GroupByGranularity != "year" {
		t.Fatalf("granularity = %q want year", plan.GroupByGranularity)
	}
	if plan.GroupByFieldID == "" || plan.TargetFieldID == "" {
		t.Fatalf("expected date field ids, got plan=%+v", plan)
	}
	if len(plan.Filters) != 1 || plan.Filters[0].Value != "Shingwauk" {
		t.Fatalf("expected Shingwauk filter, got %+v", plan.Filters)
	}

	verified := executeChatPlan(plan, question, dataset, dataset.rows, &chatSessionState{})
	if verified.Status != "ok" {
		t.Fatalf("status = %q want ok (notes=%v)", verified.Status, verified.Notes)
	}
	if len(verified.Rows) != 1 {
		t.Fatalf("expected 1 winning row, got %#v", verified.Rows)
	}
	if got := verified.Rows[0]["value"]; got != "1890" {
		t.Fatalf("winner value = %#v want 1890", got)
	}
	if got := verified.Rows[0]["count"]; got != 2 {
		t.Fatalf("winner count = %#v want 2", got)
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
	if len(verified.Notes) == 0 {
		t.Fatalf("expected explanatory note, got %#v", verified)
	}
}

func TestChatService_Chat_OverridesClarifyForGroupedDeathYearQuestion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}).
			AddRow(uint(1), "sheet.xlsx", 2, `["NAME","SCHOOL","DATE OF DEATH"]`))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"NAME":"Alice","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-05-06"}`).
			AddRow(uint(2), uint(1), 2, `{"NAME":"Beatrice","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-06-01"}`).
			AddRow(uint(3), uint(1), 2, `{"NAME":"Charlotte","SCHOOL":"Shingwauk","DATE OF DEATH":"1891-01-04"}`).
			AddRow(uint(4), uint(1), 2, `{"NAME":"Dora","SCHOOL":"Wawanosh","DATE OF DEATH":"1891-02-10"}`))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = plannerThenAnswerHook(
		t,
		`{"intent":"clarify","clarification_question":"Would you like the total number of deaths or a list of dates?"}`,
		`GROUPED_OK`,
		nil,
		func(prompt string) {
			if !strings.Contains(prompt, `"winning_values": [`) || !strings.Contains(prompt, `"1890"`) {
				t.Fatalf("expected grouped winning year in verified result, got:\n%s", prompt)
			}
			if !strings.Contains(prompt, `"max_count": 2`) {
				t.Fatalf("expected grouped max count in verified result, got:\n%s", prompt)
			}
		},
	)
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("In what years did the highest number of deaths occur at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Answer != "GROUPED_OK" {
		t.Fatalf("unexpected answer: %#v", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTemporalValue_GroupBucketYear(t *testing.T) {
	alt := parseTemporalValue("1888-03-30 (1888-03-03)")
	key, _, exact, present := alt.groupBucket("year")
	if !present || !exact || key != "1888" {
		t.Fatalf("alternative same-year bucket = (%q, %v, %v)", key, exact, present)
	}

	rng := parseTemporalValue("1890-1891")
	key, _, exact, present = rng.groupBucket("year")
	if !present || exact || key != "" {
		t.Fatalf("cross-year range bucket = (%q, %v, %v)", key, exact, present)
	}
}

func TestChatService_Chat_GroupCountUnsafeFallsBackToExactnessWarning(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}).
			AddRow(uint(1), "sheet.xlsx", 2, `["NAME","SCHOOL","DATE OF DEATH"]`))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"NAME":"Alice","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-05-06"}`).
			AddRow(uint(2), uint(1), 2, `{"NAME":"Beatrice","SCHOOL":"Shingwauk","DATE OF DEATH":"1891-06-01"}`).
			AddRow(uint(3), uint(1), 2, `{"NAME":"Charlotte","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-1891"}`))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		var out genai.GenerateContentResponse
		if strings.Contains(prompt, "VERIFIED RESULT (only source of truth):") {
			if !strings.Contains(prompt, `"status": "cannot_determine_exactly"`) {
				t.Fatalf("expected exactness warning, got:\n%s", prompt)
			}
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"I can't determine that exactly from the data."}]}}]}`), &out)
			return &out, nil
		}
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"intent\":\"clarify\",\"clarification_question\":\"Need clarification\"}"}]}}]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Which year had the most deaths at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "can't determine") {
		t.Fatalf("expected exactness answer, got %#v", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
