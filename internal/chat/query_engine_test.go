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

func TestHeuristicChatPlan_ExistenceQuestionExtractsSearchAndFilter(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "Alice", "SCHOOL": "Shingwauk Indian Residential School", "CAUSE OF DEATH": "Drowned in the river"},
		{"NAME": "Beatrice", "SCHOOL": "Wawanosh", "CAUSE OF DEATH": "Tuberculosis"},
	})

	question := "Did any children die from drowning at Shingwauk Indian Residential School?"
	plan, ok := heuristicChatPlan(question, dataset, &chatSessionState{})
	if !ok {
		t.Fatal("expected heuristic plan")
	}
	if plan.Intent != "count_rows" {
		t.Fatalf("intent = %q want count_rows", plan.Intent)
	}
	if len(plan.SearchTerms) == 0 || plan.SearchTerms[0] != "drowning" {
		t.Fatalf("expected drowning search term, got %+v", plan.SearchTerms)
	}
	if len(plan.Filters) != 1 || plan.Filters[0].Value != "Shingwauk Indian Residential School" {
		t.Fatalf("expected school filter, got %+v", plan.Filters)
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

func TestApplyMultiColumnFilter_MatchesStemmedKeywords(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "Alice", "CAUSE OF DEATH": "Drowned while crossing the river"},
		{"NAME": "Beatrice", "CAUSE OF DEATH": "Tuberculosis"},
	})

	filtered := applyMultiColumnFilter(dataset.rows, dataset, "drowning")
	if len(filtered) != 1 {
		t.Fatalf("expected one fuzzy match, got %d", len(filtered))
	}
	if filtered[0].primaryName() != "Alice" {
		t.Fatalf("expected Alice match, got %+v", filtered)
	}
}

func TestScopedLocationMatch_AllowsShortAndLongSchoolNames(t *testing.T) {
	if !scopedLocationMatch("Shingwauk", "Shingwauk Indian Residential School") {
		t.Fatal("expected scoped location match to treat Shingwauk as matching the full school name")
	}
	if !scopedLocationMatch("Shingwauk Indian Residential School", "Shingwauk") {
		t.Fatal("expected scoped location match to work in reverse as well")
	}
}

func TestInferFieldKind_TreatsPlaceOfDeathAsLocation(t *testing.T) {
	if kind := inferFieldKind("PLACE OF DEATH", nil); kind != chatFieldKindLocation {
		t.Fatalf("kind = %q want %q", kind, chatFieldKindLocation)
	}
	if kind := inferFieldKind("SCHOOL", nil); kind != chatFieldKindLocation {
		t.Fatalf("kind = %q want %q", kind, chatFieldKindLocation)
	}
}

func TestExecuteChatPlan_LocationFallbackSearchesAcrossSchoolFields(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "Alice", "SCHOOL": "Shingwauk", "PLACE OF DEATH": "Sault Ste. Marie", "DATE OF DEATH": "1890-05-06"},
		{"NAME": "Beatrice", "SCHOOL": "Shingwauk", "PLACE OF DEATH": "Garden River", "DATE OF DEATH": "1890-06-01"},
		{"NAME": "Charlotte", "SCHOOL": "Wawanosh", "PLACE OF DEATH": "Shingwauk", "DATE OF DEATH": "1891-01-04"},
	})

	plan := chatPlannerOutput{
		Intent:             "group_count_extreme",
		TargetFieldID:      "date_of_death",
		GroupByFieldID:     "date_of_death",
		GroupByGranularity: "year",
		Filters: []chatPlannerFilter{
			{FieldID: "place_of_death", Op: "eq", Value: "Shingwauk Indian Residential School"},
		},
	}

	verified := executeChatPlan(plan, "In what years did the highest number of deaths occur at Shingwauk?", dataset, dataset.rows, &chatSessionState{})
	if verified.Status != "ok" {
		t.Fatalf("status = %q want ok (notes=%v)", verified.Status, verified.Notes)
	}
	if len(verified.Rows) != 1 {
		t.Fatalf("expected one winner row, got %#v", verified.Rows)
	}
	if verified.Rows[0]["value"] != "1890" {
		t.Fatalf("expected 1890 winner, got %#v", verified.Rows)
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
	if result.Answer != "1890 had the highest number of deaths, with 2 recorded deaths." {
		t.Fatalf("unexpected answer: %#v", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatService_Chat_DoesNotLeakPriorFocusIntoFreshSearchQuestion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}).
			AddRow(uint(1), "sheet.xlsx", 2, `["NAME","SCHOOL","CAUSE OF DEATH"]`))
	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(42), uint(1), 2, `{"NAME":"Audrey Lesage","SCHOOL":"Other School","CAUSE OF DEATH":"Tuberculosis"}`).
			AddRow(uint(43), uint(1), 2, `{"NAME":"Alice","SCHOOL":"Shingwauk Indian Residential School","CAUSE OF DEATH":"Drowned while crossing the river"}`))
	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}).
			AddRow(uint(1), "sheet.xlsx", 2, `["NAME","SCHOOL","CAUSE OF DEATH"]`))

	old := genaiGenerateContentHook
	callCount := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		callCount++
		prompt := contents[0].Parts[0].Text
		var out genai.GenerateContentResponse
		switch callCount {
		case 1:
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"intent\":\"describe_subject\",\"subject_text\":\"Audrey Lesage\"}"}]}}]}`), &out)
		case 2:
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"FIRST_OK"}]}}]}`), &out)
		case 3:
			if strings.Contains(prompt, "Current focus: Audrey Lesage") {
				t.Fatalf("fresh search question should not include prior focus in planner prompt:\n%s", prompt)
			}
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"intent\":\"count_rows\",\"use_session_focus\":true,\"search_terms\":[\"drowning\"],\"filters\":[{\"field_id\":\"school\",\"op\":\"eq\",\"value\":\"Shingwauk Indian Residential School\"}]}"}]}}]}`), &out)
		default:
			t.Fatalf("unexpected extra LLM call %d", callCount)
		}
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	first, err := cs.ChatForUser(9, "Tell me about Audrey Lesage", nil, "sheet.xlsx", nil)
	if err != nil || first.Answer != "FIRST_OK" {
		t.Fatalf("unexpected first result: %#v err=%v", first, err)
	}

	second, err := cs.ChatForUser(9, "Did any children die from drowning at Shingwauk Indian Residential School?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected second err: %v", err)
	}
	if !strings.Contains(second.Answer, "Yes. I found 1 matching record") {
		t.Fatalf("expected deterministic yes answer, got %#v", second)
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

func TestExecuteChatPlan_GroupCountExtremeByCommunity_UsesDeathScopedRows(t *testing.T) {
	dataset := buildDatasetForQueryEngineTest(t, []map[string]string{
		{"NAME": "Alive One", "First Nation/Community": "Community A", "DATE OF DEATH": ""},
		{"NAME": "Alive Two", "First Nation/Community": "Community A", "DATE OF DEATH": ""},
		{"NAME": "Deceased A", "First Nation/Community": "Community A", "DATE OF DEATH": "1890-05-06"},
		{"NAME": "Deceased B1", "First Nation/Community": "Community B", "DATE OF DEATH": "1891-01-04"},
		{"NAME": "Deceased B2", "First Nation/Community": "Community B", "DATE OF DEATH": "1891-02-10"},
	})

	question := "What community / reserve had the most deaths? What are the names of those children?"
	plan, ok := deterministicChatPlan(question, dataset, &chatSessionState{})
	if !ok {
		t.Fatal("expected deterministic plan")
	}
	if plan.Intent != "group_count_extreme" {
		t.Fatalf("intent = %q want group_count_extreme", plan.Intent)
	}
	if plan.GroupByFieldID == "" {
		t.Fatalf("expected community group field, got %+v", plan)
	}

	verified := executeChatPlan(plan, question, dataset, dataset.rows, &chatSessionState{})
	if verified.Status != "ok" {
		t.Fatalf("status = %q want ok (notes=%v)", verified.Status, verified.Notes)
	}

	valueMap, ok := verified.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected value map, got %#v", verified.Value)
	}
	if winners, ok := valueMap["winning_values"].([]string); !ok || len(winners) != 1 || winners[0] != "Community B" {
		t.Fatalf("winning_values = %#v want Community B", valueMap["winning_values"])
	}
	if winnerNames, ok := valueMap["winner_names"].([]string); !ok || len(winnerNames) != 2 || winnerNames[0] != "Deceased B1" || winnerNames[1] != "Deceased B2" {
		t.Fatalf("winner_names = %#v want [Deceased B1 Deceased B2]", valueMap["winner_names"])
	}
}

func TestChatService_Chat_GroupCountByCommunity_ReturnsDeterministicNamesWithoutLLM(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}).
			AddRow(uint(1), "sheet.xlsx", 2, `["NAME","First Nation/Community","DATE OF DEATH"]`))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"NAME":"Alive One","First Nation/Community":"Community A","DATE OF DEATH":""}`).
			AddRow(uint(2), uint(1), 2, `{"NAME":"Alive Two","First Nation/Community":"Community A","DATE OF DEATH":""}`).
			AddRow(uint(3), uint(1), 2, `{"NAME":"Deceased A","First Nation/Community":"Community A","DATE OF DEATH":"1890-05-06"}`).
			AddRow(uint(4), uint(1), 2, `{"NAME":"Deceased B1","First Nation/Community":"Community B","DATE OF DEATH":"1891-01-04"}`).
			AddRow(uint(5), uint(1), 2, `{"NAME":"Deceased B2","First Nation/Community":"Community B","DATE OF DEATH":"1891-02-10"}`))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content) (*genai.GenerateContentResponse, error) {
		t.Fatal("LLM should not be called for deterministic grouped community answer")
		return nil, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("What community / reserve had the most deaths? What are the names of those children?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := "Community B had the highest number of deaths, with 2 recorded deaths. The children were Deceased B1 and Deceased B2."
	if result.Answer != want {
		t.Fatalf("answer = %q want %q", result.Answer, want)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
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
