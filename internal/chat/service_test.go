package chat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"google.golang.org/api/googleapi"
	"google.golang.org/genai"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newMockDBChatSvc(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}

	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 sqlDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}

	return gdb, mock, func() { _ = sqlDB.Close() }
}

func makeAudioFileHeader(t *testing.T, contentType string, data []byte) *multipart.FileHeader {
	t.Helper()

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)

	partHeader := make(textprotoMIMEHeader)
	partHeader.Set("Content-Disposition", `form-data; name="audio"; filename="a.bin"`)
	if contentType != "" {
		partHeader.Set("Content-Type", contentType)
	}
	part, err := w.CreatePart(partHeader.toHeader())
	if err != nil {
		t.Fatalf("CreatePart: %v", err)
	}
	_, _ = part.Write(data)
	_ = w.Close()

	req := httptest.NewRequest(http.MethodPost, "/x", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	if err := req.ParseMultipartForm(10 << 20); err != nil {
		t.Fatalf("ParseMultipartForm: %v", err)
	}
	return req.MultipartForm.File["audio"][0]
}

type textprotoMIMEHeader map[string][]string

func (h textprotoMIMEHeader) Set(k, v string) { h[k] = []string{v} }

func (h textprotoMIMEHeader) toHeader() map[string][]string {
	out := map[string][]string{}
	for k, v := range h {
		out[k] = v
	}
	return out
}

func expectChatFileLookup(mock sqlmock.Sqlmock, filename string, descriptions ...string) {
	description := ""
	if len(descriptions) > 0 {
		description = descriptions[0]
	}
	mock.ExpectQuery(`(?i)select.*id.*version.*description.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "version", "description"}).
			AddRow(uint(1), 1, description))
}

func expectChatRowsLookup(mock sqlmock.Sqlmock, rows ...string) {
	result := sqlmock.NewRows([]string{"id", "row_data"})
	for idx, row := range rows {
		result.AddRow(uint(idx+1), row)
	}
	mock.ExpectQuery(`(?i)select.*id.*row_data.*from.*file_data.*where.*file_id.*version.*order.*id.*asc`).
		WillReturnRows(result)
}

func expectChatNormalizedRowsLookup(mock sqlmock.Sqlmock, rows ...string) {
	result := sqlmock.NewRows([]string{
		"source_row_id",
		"canonical_name",
		"canonical_community",
		"canonical_school",
		"search_text",
		"row_data_normalized",
	})
	for idx, row := range rows {
		canonicalName, canonicalCommunity, canonicalSchool, searchText := normalizedChatRowColumns(row)
		result.AddRow(uint(idx+1), canonicalName, canonicalCommunity, canonicalSchool, searchText, row)
	}
	mock.ExpectQuery(`(?i)select.*source_row_id.*canonical_name.*canonical_community.*canonical_school.*search_text.*row_data_normalized.*from.*file_data_normalized.*where.*file_id.*version.*status.*normalization_version.*order.*source_row_id.*asc`).
		WillReturnRows(result)
}

func normalizedChatRowColumns(row string) (string, string, string, string) {
	var payload map[string]any
	_ = json.Unmarshal([]byte(row), &payload)

	var canonicalName string
	var canonicalCommunity string
	var canonicalSchool string
	var searchValues []string

	if canonical, ok := payload["canonical"].(map[string]any); ok {
		canonicalName, _ = canonical["display_name"].(string)
		canonicalCommunity, _ = canonical["community"].(string)
		canonicalSchool, _ = canonical["school"].(string)
		searchValues = append(searchValues, flattenStringValues(canonical)...)
	}
	if chatPayload, ok := payload["chat"].(map[string]any); ok {
		if defaultBundle, ok := chatPayload["default_bundle"].(map[string]any); ok {
			searchValues = append(searchValues, flattenStringValues(defaultBundle)...)
		}
		if narrativeBundle, ok := chatPayload["narrative_bundle"].(map[string]any); ok {
			searchValues = append(searchValues, flattenStringValues(narrativeBundle)...)
		}
	}
	if fieldsPayload, ok := payload["fields"].(map[string]any); ok {
		searchValues = append(searchValues, flattenNormalizedFieldValues(fieldsPayload)...)
	}

	return normalizeChatSearchValue(canonicalName),
		normalizeChatSearchValue(canonicalCommunity),
		normalizeChatSearchValue(canonicalSchool),
		normalizeChatSearchValue(strings.Join(searchValues, " "))
}

func flattenStringValues(value any) []string {
	switch v := value.(type) {
	case map[string]any:
		out := make([]string, 0, len(v))
		for _, child := range v {
			out = append(out, flattenStringValues(child)...)
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, child := range v {
			out = append(out, flattenStringValues(child)...)
		}
		return out
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []string{v}
	default:
		return nil
	}
}

func normalizedChatRowJSON(defaultBundle map[string]any, options ...func(map[string]any)) string {
	payload := map[string]any{
		"chat": map[string]any{
			"default_bundle": defaultBundle,
		},
	}
	for _, option := range options {
		option(payload)
	}
	out, _ := json.Marshal(payload)
	return string(out)
}

func withCanonical(values map[string]any) func(map[string]any) {
	return func(payload map[string]any) {
		payload["canonical"] = values
	}
}

func withFields(values map[string]any) func(map[string]any) {
	return func(payload map[string]any) {
		payload["fields"] = values
	}
}

func withNarrative(values map[string]any) func(map[string]any) {
	return func(payload map[string]any) {
		chatPayload, _ := payload["chat"].(map[string]any)
		if chatPayload == nil {
			chatPayload = map[string]any{}
			payload["chat"] = chatPayload
		}
		chatPayload["narrative_bundle"] = values
	}
}

func flattenNormalizedFieldValues(fields map[string]any) []string {
	out := make([]string, 0, len(fields)*2)
	for _, rawField := range fields {
		field, ok := rawField.(map[string]any)
		if !ok {
			continue
		}
		if raw, ok := field["raw"].(string); ok && strings.TrimSpace(raw) != "" {
			out = append(out, raw)
		}
		if normalized, ok := field["normalized"].(string); ok && strings.TrimSpace(normalized) != "" {
			out = append(out, normalized)
		}
		if tokens, ok := field["tokens"].([]any); ok {
			for _, token := range tokens {
				if text, ok := token.(string); ok && strings.TrimSpace(text) != "" {
					out = append(out, text)
				}
			}
		}
	}
	return out
}

func jsonStructuredAnswer(answer string, matchedRowRef *string) *genai.GenerateContentResponse {
	payload := map[string]any{
		"answer":          answer,
		"matched_row_ref": nil,
	}
	if matchedRowRef != nil {
		payload["matched_row_ref"] = *matchedRowRef
	}

	text, _ := json.Marshal(payload)

	var out genai.GenerateContentResponse
	_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":`+strconvQuote(string(text))+`}]}}]}`), &out)
	return &out
}

func rawAnswer(text string) *genai.GenerateContentResponse {
	var out genai.GenerateContentResponse
	_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":`+strconvQuote(text)+`}]}}]}`), &out)
	return &out
}

func strconvQuote(v string) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func Test_parseRateFromMime(t *testing.T) {
	if parseRateFromMime("") != 0 {
		t.Fatal("expected 0")
	}
	if parseRateFromMime("audio/L16") != 0 {
		t.Fatal("expected 0 when no rate")
	}
	if parseRateFromMime("audio/L16;rate=48000") != 48000 {
		t.Fatal("expected 48000")
	}
}

func Test_decodeBase64Loose(t *testing.T) {
	plain := []byte("ABC")
	padded := base64.StdEncoding.EncodeToString(plain)
	raw := base64.RawStdEncoding.EncodeToString(plain)

	if b, _ := decodeBase64Loose(padded); string(b) != "ABC" {
		t.Fatalf("padded decode failed: %q", string(b))
	}
	if b, _ := decodeBase64Loose(raw); string(b) != "ABC" {
		t.Fatalf("raw decode failed: %q", string(b))
	}
}

func Test_pcmToWav_defaults(t *testing.T) {
	out := pcmToWav([]byte{0x01, 0x02}, 0, 0, 0)
	if len(out) < 12 || string(out[:4]) != "RIFF" || string(out[8:12]) != "WAVE" {
		t.Fatalf("bad wav header: %q", out[:12])
	}
}

func TestFirstTextFromResp_ReturnsFirstNonEmptyPart(t *testing.T) {
	var resp genai.GenerateContentResponse
	_ = json.Unmarshal([]byte(`{
		"candidates":[
			{"content":{"parts":[
				{"text":"first"},
				{"text":"second"}
			]}}
		]
	}`), &resp)

	if got := firstTextFromResp(&resp); got != "first" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveChatResponse_ParsesMatchedRowID(t *testing.T) {
	rowRef := "r2"
	answer, matchedRowID := resolveChatResponse(
		`{"answer":"Alice attended Shingwauk.","matched_row_ref":"`+rowRef+`"}`,
		map[string]int{"R1": 11, "R2": 22},
	)

	if answer != "Alice attended Shingwauk." {
		t.Fatalf("answer = %q", answer)
	}
	if matchedRowID == nil || *matchedRowID != 22 {
		t.Fatalf("matched row = %#v", matchedRowID)
	}
}

func TestResolveChatResponse_FallsBackToRawText(t *testing.T) {
	answer, matchedRowID := resolveChatResponse("Plain text answer.", map[string]int{"R1": 11})
	if answer != "Plain text answer." {
		t.Fatalf("answer = %q", answer)
	}
	if matchedRowID != nil {
		t.Fatalf("matched row = %#v", matchedRowID)
	}
}

func TestChatService_Chat_FileNotFound(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*id.*version.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "version", "description"}))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("q", nil, "missing.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "file not found") {
		t.Fatalf("expected file not found, got %v", err)
	}
}

func TestChatService_Chat_UsesStructuredPrompt(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(mock, normalizedChatRowJSON(
		map[string]any{
			"name":      "Alice",
			"community": "Garden River",
		},
		withCanonical(map[string]any{
			"display_name": "Alice",
			"community":    "Garden River",
		}),
	))

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		if model != "gemini-2.5-flash" {
			t.Fatalf("unexpected generate model %q", model)
		}
		if config != nil {
			t.Fatalf("expected nil config, got %#v", config)
		}
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected prompt contents, got %#v", contents)
		}

		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "Structured output requirements:") {
			t.Fatalf("expected structured output instructions, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, "default_bundle") {
			t.Fatalf("expected compact bundle data, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, `"matched_row_ref": "R1" or null`) {
			t.Fatalf("expected matched_row_ref guidance, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, `"row_ref":"R1"`) {
			t.Fatalf("expected prompt to include row refs, got:\n%s", prompt)
		}

		return jsonStructuredAnswer("Yes. I found 1 recorded death connected to drowning at Shingwauk.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Did any children die from drowning at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Answer != "Yes. I found 1 recorded death connected to drowning at Shingwauk." {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.MatchedRowID != nil {
		t.Fatalf("unexpected matched row: %#v", result.MatchedRowID)
	}
}

func TestChatService_Chat_ReturnsMatchedRowID(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(mock, normalizedChatRowJSON(
		map[string]any{
			"name":      "Alice",
			"community": "Garden River",
		},
		withCanonical(map[string]any{
			"display_name": "Alice",
			"community":    "Garden River",
		}),
	))

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	rowRef := "R1"
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		if config != nil {
			t.Fatalf("expected nil config, got %#v", config)
		}
		return jsonStructuredAnswer("Alice is from Garden River.", &rowRef), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Who is Alice?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.MatchedRowID == nil || *result.MatchedRowID != 1 {
		t.Fatalf("unexpected matched row: %#v", result.MatchedRowID)
	}
}

func TestChatService_Chat_StructuredEntityLookupNarrowsPromptRows(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Alice Johnson",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Alice Johnson",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Bob Thomas",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Bob Thomas",
				"community":    "Garden River",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	rowRef := "R1"
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "Alice Johnson") {
			t.Fatalf("expected Alice row in prompt, got:\n%s", prompt)
		}
		if strings.Contains(prompt, "Bob Thomas") {
			t.Fatalf("did not expect Bob row in entity lookup prompt, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Alice Johnson is from Garden River.", &rowRef), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Who is Alice Johnson?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.RetrievalMode != "entity_rows" {
		t.Fatalf("expected entity retrieval mode, got %#v", result.Debug)
	}
}

func TestChatService_Chat_FallsBackToProOn429(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(mock, normalizedChatRowJSON(
		map[string]any{
			"name": "Alice",
		},
		withCanonical(map[string]any{
			"display_name": "Alice",
		}),
	))

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, _ []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		if config != nil {
			t.Fatalf("expected nil config, got %#v", config)
		}
		if model == "gemini-2.5-flash" {
			return nil, &googleapi.Error{Code: http.StatusTooManyRequests}
		}
		if model != "gemini-2.5-pro" {
			t.Fatalf("unexpected fallback model %q", model)
		}
		return jsonStructuredAnswer("Based on the records provided, the highest number of deaths occurred in 1913.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("In what years did the highest number of deaths occur?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "1913") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.Debug == nil {
		t.Fatal("expected debug metrics")
	}
	if result.Debug.UsedModel != "gemini-2.5-pro" {
		t.Fatalf("used model = %q want gemini-2.5-pro", result.Debug.UsedModel)
	}
}

func TestChatService_Chat_PopulatesDebugMetrics(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Alice",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Alice",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Bob",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Bob",
				"community":    "Garden River",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		return jsonStructuredAnswer("Garden River has two matching rows.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Who is from Garden River?", nil, "sheet.xlsx", []string{"Garden River"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil {
		t.Fatal("expected debug metrics")
	}
	if result.Debug.Strategy != "structured_retrieval" {
		t.Fatalf("strategy = %q want structured_retrieval", result.Debug.Strategy)
	}
	if result.Debug.TotalRowsLoaded != 2 {
		t.Fatalf("total rows loaded = %d want 2", result.Debug.TotalRowsLoaded)
	}
	if result.Debug.RowsSelected != 2 {
		t.Fatalf("rows selected = %d want 2", result.Debug.RowsSelected)
	}
	if result.Debug.CommunityFilterCount != 1 {
		t.Fatalf("community filter count = %d want 1", result.Debug.CommunityFilterCount)
	}
	if result.Debug.RetrievalMode != "keyword_rows" {
		t.Fatalf("retrieval mode = %q want keyword_rows", result.Debug.RetrievalMode)
	}
	if result.Debug.PromptProjectionMode != "full" {
		t.Fatalf("prompt projection mode = %q want full", result.Debug.PromptProjectionMode)
	}
	if result.Debug.PromptChars <= 0 || result.Debug.PromptBytes <= 0 {
		t.Fatalf("expected positive prompt sizing, got %#v", result.Debug)
	}
	if result.Debug.PrimaryModel != "gemini-2.5-flash" {
		t.Fatalf("primary model = %q want gemini-2.5-flash", result.Debug.PrimaryModel)
	}
	if result.Debug.UsedModel != "gemini-2.5-flash" {
		t.Fatalf("used model = %q want gemini-2.5-flash", result.Debug.UsedModel)
	}
}

func TestChatService_Chat_CompressesBroadStructuredPrompt(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	rows := []string{
		normalizedChatRowJSON(
			map[string]any{
				"name":             "Alice Johnson",
				"community":        "Garden River",
				"school":           "Shingwauk",
				"parents_names":    "John and Mary Johnson",
				"mapping_location": "Garden River, Ontario",
			},
			withCanonical(map[string]any{
				"display_name": "Alice Johnson",
				"community":    "Garden River",
				"school":       "Shingwauk",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":             "Bob Thomas",
				"community":        "Garden River",
				"school":           "Shingwauk",
				"parents_names":    "Peter and Alice Thomas",
				"mapping_location": "Garden River, Ontario",
			},
			withCanonical(map[string]any{
				"display_name": "Bob Thomas",
				"community":    "Garden River",
				"school":       "Shingwauk",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":             "Carol Pine",
				"community":        "Garden River",
				"school":           "Shingwauk",
				"parents_names":    "David and Ruth Pine",
				"mapping_location": "Garden River, Ontario",
			},
			withCanonical(map[string]any{
				"display_name": "Carol Pine",
				"community":    "Garden River",
				"school":       "Shingwauk",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":             "Daniel Roy",
				"community":        "Garden River",
				"school":           "Shingwauk",
				"parents_names":    "Emma Roy",
				"mapping_location": "Garden River, Ontario",
			},
			withCanonical(map[string]any{
				"display_name": "Daniel Roy",
				"community":    "Garden River",
				"school":       "Shingwauk",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":             "Eva White",
				"community":        "Garden River",
				"school":           "Shingwauk",
				"parents_names":    "Henry and Lucy White",
				"mapping_location": "Garden River, Ontario",
			},
			withCanonical(map[string]any{
				"display_name": "Eva White",
				"community":    "Garden River",
				"school":       "Shingwauk",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":             "Frank Stone",
				"community":        "Garden River",
				"school":           "Shingwauk",
				"parents_names":    "Samuel and Grace Stone",
				"mapping_location": "Garden River, Ontario",
			},
			withCanonical(map[string]any{
				"display_name": "Frank Stone",
				"community":    "Garden River",
				"school":       "Shingwauk",
			}),
		),
	}
	expectChatNormalizedRowsLookup(mock, rows...)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"community":"Garden River"`) {
			t.Fatalf("expected compressed prompt to keep community, got:\n%s", prompt)
		}
		if strings.Contains(prompt, `"parents_names":`) {
			t.Fatalf("did not expect broad compressed prompt to keep parents_names, got:\n%s", prompt)
		}
		if strings.Contains(prompt, `"mapping_location":`) {
			t.Fatalf("did not expect broad compressed prompt to keep mapping_location, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Garden River has multiple matching students.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Who is from Garden River?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.PromptProjectionMode != "relevant_only" {
		t.Fatalf("expected relevant_only prompt projection, got %#v", result.Debug)
	}
}

func TestChatService_Chat_KeepsFullPromptForEntityQuestion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(mock, normalizedChatRowJSON(
		map[string]any{
			"name":             "Alice Johnson",
			"community":        "Garden River",
			"parents_names":    "John and Mary Johnson",
			"mapping_location": "Garden River, Ontario",
		},
		withCanonical(map[string]any{
			"display_name": "Alice Johnson",
			"community":    "Garden River",
		}),
	))

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"parents_names":"John and Mary Johnson"`) {
			t.Fatalf("expected full entity prompt to keep parents_names, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, `"mapping_location":"Garden River, Ontario"`) {
			t.Fatalf("expected full entity prompt to keep mapping_location, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Alice Johnson is from Garden River.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Who is Alice Johnson?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.PromptProjectionMode != "full" {
		t.Fatalf("expected full prompt projection, got %#v", result.Debug)
	}
}

func TestChatService_Chat_IncludesSourceFieldsForArbitraryColumnQuestion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(mock, normalizedChatRowJSON(
		map[string]any{
			"name":      "Alice Johnson",
			"community": "Garden River",
		},
		withCanonical(map[string]any{
			"display_name": "Alice Johnson",
			"community":    "Garden River",
		}),
		withFields(map[string]any{
			"Age": map[string]any{
				"raw":        "14",
				"normalized": "14",
				"role":       "text",
				"tokens":     []any{"14"},
			},
			"Siblings": map[string]any{
				"raw":        "Mary and John",
				"normalized": "mary and john",
				"role":       "text",
				"tokens":     []any{"mary", "john"},
			},
		}),
	))

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"source_fields":{`) || !strings.Contains(prompt, `"Age":"14"`) {
			t.Fatalf("expected arbitrary source field in prompt, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Alice Johnson is listed as age 14.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("What age was Alice Johnson?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.PromptProjectionMode != "full" {
		t.Fatalf("expected full prompt projection for single-row arbitrary column question, got %#v", result.Debug)
	}
}

func TestChatService_Chat_IncludesSourceFieldsWhenValueMatchOnlyExistsInRawColumn(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Alice Johnson",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Alice Johnson",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Raymond Fisher",
				"community":       "Garden River",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Raymond Fisher",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
			withFields(map[string]any{
				"Incident Type": map[string]any{
					"raw":        "Drowning",
					"normalized": "drowning",
					"role":       "text",
					"tokens":     []any{"drowning", "drown"},
				},
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"source_fields":{`) || !strings.Contains(prompt, `"Incident Type":"Drowning"`) {
			t.Fatalf("expected raw source field carrying the drowning match, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Yes. One matched record indicates drowning.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Did any children die from drowning?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.RetrievalMode == "legacy_full_dataset_fallback" {
		t.Fatalf("expected structured retrieval path, got %#v", result.Debug)
	}
}

func TestChatService_Chat_AudioReadError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(mock, normalizedChatRowJSON(
		map[string]any{"name": "Alice"},
		withCanonical(map[string]any{"display_name": "Alice"}),
	))

	oldRead := readAllHook
	readAllHook = func(_ io.Reader) ([]byte, error) {
		return nil, errors.New("read fail")
	}
	t.Cleanup(func() { readAllHook = oldRead })

	fh := makeAudioFileHeader(t, "audio/webm", []byte("xxx"))
	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("hello", fh, "sheet.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "failed to read audio file") {
		t.Fatalf("expected audio read error, got %v", err)
	}
}

func TestChatService_Chat_IncludesNarrativeBundleForNarrativeKeywordMatches(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name": "Alice",
			},
			withCanonical(map[string]any{
				"display_name": "Alice",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Raymond Fisher",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Raymond Fisher",
				"community":    "Garden River",
			}),
			withNarrative(map[string]any{
				"notes": "Drowning was mentioned in the school correspondence.",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"narrative_bundle":{`) || !strings.Contains(prompt, "Drowning was mentioned in the school correspondence.") {
			t.Fatalf("expected narrative bundle in prompt, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Yes. One matched record mentions drowning.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Did any children die from drowning at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.NarrativeRowsIncluded != 1 {
		t.Fatalf("expected one narrative row, got %#v", result.Debug)
	}
}

func TestChatService_Chat_MatchesDrownedNarrativeForDrowningQuestion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name": "Alice",
			},
			withCanonical(map[string]any{
				"display_name": "Alice",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Raymond Fisher",
				"community":       "Garden River",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Raymond Fisher",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
			withNarrative(map[string]any{
				"notes": "He drowned while returning home from school.",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"narrative_bundle":{`) || !strings.Contains(prompt, "He drowned while returning home from school.") {
			t.Fatalf("expected drowned narrative variant in prompt, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Yes. One matched record describes a child who drowned.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Did any children die from drowning at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.NarrativeRowsIncluded != 1 {
		t.Fatalf("expected one narrative row, got %#v", result.Debug)
	}
}

func TestChatService_Chat_DeathYearQuestionIncludesDeathNarrativeAndDatasetContext(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	filename := "Shingwauk And Wawanosh Students Master List"
	expectChatFileLookup(mock, filename, "Master list spanning Shingwauk and Wawanosh records, including obituary and death details where available.")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":              "Alice Johnson",
				"community":         "Garden River",
				"deceased_status":   "Yes",
				"has_death_details": true,
			},
			withCanonical(map[string]any{
				"display_name":    "Alice Johnson",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
			withNarrative(map[string]any{
				"death_details": "Died 13 Jan 1913, obituary.",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":              "Bob Thomas",
				"community":         "Batchawana Bay",
				"deceased_status":   "Yes",
				"has_death_details": true,
			},
			withCanonical(map[string]any{
				"display_name":    "Bob Thomas",
				"community":       "Batchawana Bay",
				"deceased_status": "yes",
			}),
			withNarrative(map[string]any{
				"death_details": "Died 2 Dec 1913, grave marker.",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":              "Carol Pine",
				"community":         "Garden River",
				"deceased_status":   "Yes",
				"has_death_details": true,
			},
			withCanonical(map[string]any{
				"display_name":    "Carol Pine",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
			withNarrative(map[string]any{
				"death_details": "Died 4 Mar 1914, church record.",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "Dataset context:") || !strings.Contains(prompt, filename) {
			t.Fatalf("expected dataset context in prompt, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, "Master list spanning Shingwauk and Wawanosh records") {
			t.Fatalf("expected file description in prompt, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, `"narrative_bundle":{`) || !strings.Contains(prompt, "Died 13 Jan 1913, obituary.") || !strings.Contains(prompt, "Died 2 Dec 1913, grave marker.") {
			t.Fatalf("expected death narrative details in prompt, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("The highest number of recorded deaths occurred in 1913.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("In what years did the highest number of deaths occur at Shingwauk?", nil, filename, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil {
		t.Fatal("expected debug metrics")
	}
	if result.Debug.PromptProjectionMode != "full" {
		t.Fatalf("expected full prompt projection for death-year question, got %#v", result.Debug)
	}
	if result.Debug.NarrativeRowsIncluded != 3 {
		t.Fatalf("expected all death rows to include narrative, got %#v", result.Debug)
	}
}

func TestChatService_Chat_FallsBackToFullDatasetWhenStructuredRowsUnavailable(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	mock.ExpectQuery(`(?i)select.*source_row_id.*canonical_name.*canonical_community.*canonical_school.*search_text.*row_data_normalized.*from.*file_data_normalized.*where.*file_id.*version.*status.*normalization_version.*order.*source_row_id.*asc`).
		WillReturnRows(sqlmock.NewRows([]string{"source_row_id", "canonical_name", "canonical_community", "canonical_school", "search_text", "row_data_normalized"}))
	mock.ExpectQuery(`(?i)select.*source_row_id.*canonical_name.*canonical_community.*canonical_school.*search_text.*row_data_normalized.*from.*file_data_normalized.*where.*file_id.*version.*status.*normalization_version.*order.*source_row_id.*asc`).
		WillReturnRows(sqlmock.NewRows([]string{"source_row_id", "canonical_name", "canonical_community", "canonical_school", "search_text", "row_data_normalized"}))
	expectChatRowsLookup(mock, `{"NAME":"Alice","First Nation/Community":"Garden River"}`)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"row_data":{"NAME":"Alice","First Nation/Community":"Garden River"}`) {
			t.Fatalf("expected legacy row_data fallback prompt, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("Alice is from Garden River.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Who is Alice?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil || result.Debug.RetrievalMode != "legacy_full_dataset_fallback" {
		t.Fatalf("expected legacy fallback mode, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesCountQueryDeterministically(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Alice Johnson",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Alice Johnson",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Bob Thomas",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Bob Thomas",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Carol Pine",
				"community": "Batchawana Bay",
			},
			withCanonical(map[string]any{
				"display_name": "Carol Pine",
				"community":    "Batchawana Bay",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic count route without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("How many students are from Garden River?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "2") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.Debug == nil {
		t.Fatal("expected debug metrics")
	}
	if result.Debug.Strategy != "deterministic_router" {
		t.Fatalf("strategy = %q want deterministic_router", result.Debug.Strategy)
	}
	if result.Debug.ExecutionMode != "deterministic" {
		t.Fatalf("execution mode = %q want deterministic", result.Debug.ExecutionMode)
	}
	if result.Debug.QueryType != "count" {
		t.Fatalf("query type = %q want count", result.Debug.QueryType)
	}
	if result.Debug.PromptBytes != 0 {
		t.Fatalf("expected zero prompt bytes for deterministic route, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesFieldLookupDeterministically(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":           "Alice Johnson",
				"student_number": "1940.442",
				"community":      "Garden River",
			},
			withCanonical(map[string]any{
				"display_name":       "Alice Johnson",
				"student_number":     "1940-442",
				"student_number_raw": "1940.442",
				"community":          "Garden River",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic field lookup without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("What is Alice Johnson's student number?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "1940.442") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.MatchedRowID == nil || *result.MatchedRowID != 1 {
		t.Fatalf("unexpected matched row: %#v", result.MatchedRowID)
	}
	if result.Debug == nil || result.Debug.QueryType != "field_lookup" {
		t.Fatalf("expected deterministic field lookup debug, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesGroupExtremeDeterministically(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Alice Johnson",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Alice Johnson",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Bob Thomas",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Bob Thomas",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Carol Pine",
				"community": "Batchawana Bay",
			},
			withCanonical(map[string]any{
				"display_name": "Carol Pine",
				"community":    "Batchawana Bay",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic grouped route without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Which community has the most students?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "Garden River") || !strings.Contains(result.Answer, "2") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.Debug == nil || result.Debug.QueryType != "group_extreme" {
		t.Fatalf("expected deterministic group_extreme debug, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesDeathGroupExtremeDeterministically(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Alice Johnson",
				"community":       "Garden River",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Alice Johnson",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Bob Thomas",
				"community":       "Garden River",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Bob Thomas",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Carol Pine",
				"community":       "Batchawana Bay",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Carol Pine",
				"community":       "Batchawana Bay",
				"deceased_status": "yes",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Daniel Roy",
				"community":       "Batchawana Bay",
				"deceased_status": "No",
			},
			withCanonical(map[string]any{
				"display_name":    "Daniel Roy",
				"community":       "Batchawana Bay",
				"deceased_status": "no",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic death grouped route without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Which community has highest number of deaths?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "Garden River") || !strings.Contains(result.Answer, "2") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.Debug == nil || result.Debug.QueryType != "group_extreme" {
		t.Fatalf("expected deterministic group_extreme debug, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesDeathGroupExtremeWithNamesDeterministically(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Alice Johnson",
				"community":       "Garden River",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Alice Johnson",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Bob Thomas",
				"community":       "Garden River",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Bob Thomas",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":            "Carol Pine",
				"community":       "Batchawana Bay",
				"deceased_status": "Yes",
			},
			withCanonical(map[string]any{
				"display_name":    "Carol Pine",
				"community":       "Batchawana Bay",
				"deceased_status": "yes",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic death grouped route with names without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("What community had the most deaths, and what are the names of those children?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "Garden River") || !strings.Contains(result.Answer, "Alice Johnson") || !strings.Contains(result.Answer, "Bob Thomas") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if strings.Contains(result.Answer, "Carol Pine") {
		t.Fatalf("expected only top community names, got %#v", result)
	}
	if result.Debug == nil || result.Debug.QueryType != "group_extreme" {
		t.Fatalf("expected deterministic group_extreme debug, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesDeathDatasetCommunityExtremeDeterministically(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	filename := "Confirmed- Shingwauk (Wawanosh)"
	expectChatFileLookup(mock, filename)
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":          "Andrew Johnson",
				"community":     "Walpole Island",
				"school":        "Shingwauk, Sault Ste. Marie, ON",
				"date_of_death": "1906-03-07",
			},
			withCanonical(map[string]any{
				"display_name": "Andrew Johnson",
				"community":    "Walpole Island",
				"school":       "Shingwauk, Sault Ste. Marie, ON",
				"dates": map[string]any{
					"death": map[string]any{
						"raw":  "1906-03-07",
						"iso":  "1906-03-07",
						"year": 1906,
					},
				},
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":          "Albert Penance",
				"community":     "Walpole Island",
				"school":        "Shingwauk, Sault Ste. Marie, ON",
				"date_of_death": "1903-07-30",
			},
			withCanonical(map[string]any{
				"display_name": "Albert Penance",
				"community":    "Walpole Island",
				"school":       "Shingwauk, Sault Ste. Marie, ON",
				"dates": map[string]any{
					"death": map[string]any{
						"raw":  "1903-07-30",
						"iso":  "1903-07-30",
						"year": 1903,
					},
				},
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":          "Freida Augustine",
				"community":     "Garden River",
				"school":        "Shingwauk, Sault Ste. Marie, ON",
				"date_of_death": "1913-04-17",
			},
			withCanonical(map[string]any{
				"display_name": "Freida Augustine",
				"community":    "Garden River",
				"school":       "Shingwauk, Sault Ste. Marie, ON",
				"dates": map[string]any{
					"death": map[string]any{
						"raw":  "1913-04-17",
						"iso":  "1913-04-17",
						"year": 1913,
					},
				},
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic death dataset community route without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("What community / reserve had the most deaths? What are the names of those children?", nil, filename, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "Walpole Island") || !strings.Contains(result.Answer, "Andrew Johnson") || !strings.Contains(result.Answer, "Albert Penance") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.Debug == nil || result.Debug.QueryType != "group_extreme" {
		t.Fatalf("expected deterministic group_extreme debug, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesLowestGroupExtremeDeterministically(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Alice Johnson",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Alice Johnson",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Bob Thomas",
				"community": "Garden River",
			},
			withCanonical(map[string]any{
				"display_name": "Bob Thomas",
				"community":    "Garden River",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":      "Carol Pine",
				"community": "Batchawana Bay",
			},
			withCanonical(map[string]any{
				"display_name": "Carol Pine",
				"community":    "Batchawana Bay",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic grouped route without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Which community has the fewest students?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "Batchawana Bay") || !strings.Contains(result.Answer, "1") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.Debug == nil || result.Debug.QueryType != "group_extreme" {
		t.Fatalf("expected deterministic group_extreme debug, got %#v", result.Debug)
	}
}

func TestChatService_Chat_FallsBackToLLMForUnsupportedDeterministicCount(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name": "Alice Johnson",
			},
			withCanonical(map[string]any{
				"display_name": "Alice Johnson",
			}),
			withNarrative(map[string]any{
				"notes": "Drowning was mentioned in the school correspondence.",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		return jsonStructuredAnswer("Yes. One matched record mentions drowning.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("How many students died from drowning?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil {
		t.Fatal("expected debug metrics")
	}
	if result.Debug.ExecutionMode != "llm" {
		t.Fatalf("execution mode = %q want llm", result.Debug.ExecutionMode)
	}
}

func TestChatService_Chat_UsesAllDeathRowsForAggregateYearQuestion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	filename := "Shingwauk And Wawanosh Students Master List"
	expectChatFileLookup(mock, filename)
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":              "Alice Johnson",
				"community":         "Garden River",
				"deceased_status":   "Yes",
				"has_death_details": true,
			},
			withCanonical(map[string]any{
				"display_name":    "Alice Johnson",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
			withNarrative(map[string]any{
				"notes":         "Referenced in the Shingwauk register.",
				"death_details": "Died 13 Jan 1913, obituary.",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":              "Bob Thomas",
				"community":         "Batchawana Bay",
				"deceased_status":   "Yes",
				"has_death_details": true,
			},
			withCanonical(map[string]any{
				"display_name":    "Bob Thomas",
				"community":       "Batchawana Bay",
				"deceased_status": "yes",
			}),
			withNarrative(map[string]any{
				"death_details": "Died 2 Dec 1913, grave marker.",
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":              "Carol Pine",
				"community":         "Garden River",
				"deceased_status":   "Yes",
				"has_death_details": true,
			},
			withCanonical(map[string]any{
				"display_name":    "Carol Pine",
				"community":       "Garden River",
				"deceased_status": "yes",
			}),
			withNarrative(map[string]any{
				"death_details": "Died 4 Mar 1914, church record.",
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "Died 13 Jan 1913, obituary.") || !strings.Contains(prompt, "Died 2 Dec 1913, grave marker.") || !strings.Contains(prompt, "Died 4 Mar 1914, church record.") {
			t.Fatalf("expected all death rows in prompt, got:\n%s", prompt)
		}
		return jsonStructuredAnswer("The highest number of recorded deaths occurred in 1913.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("In what years did the highest number of deaths occur at Shingwauk?", nil, filename, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil {
		t.Fatal("expected debug metrics")
	}
	if result.Debug.RetrievalMode != "aggregate_dataset" {
		t.Fatalf("expected aggregate_dataset retrieval for broad death-year question, got %#v", result.Debug)
	}
	if result.Debug.RowsSelected != 3 {
		t.Fatalf("expected all death rows to be selected, got %#v", result.Debug)
	}
}

func TestChatService_Chat_RoutesDeathYearExtremeDeterministicallyForDeathDataset(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	filename := "Confirmed- Shingwauk (Wawanosh)"
	expectChatFileLookup(mock, filename)
	expectChatNormalizedRowsLookup(
		mock,
		normalizedChatRowJSON(
			map[string]any{
				"name":          "Freida Augustine",
				"school":        "Shingwauk, Sault Ste. Marie, ON",
				"date_of_death": "1913-04-17",
			},
			withCanonical(map[string]any{
				"display_name": "Freida Augustine",
				"school":       "Shingwauk, Sault Ste. Marie, ON",
				"dates": map[string]any{
					"death": map[string]any{
						"raw":  "1913-04-17",
						"iso":  "1913-04-17",
						"year": 1913,
					},
				},
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":          "Lena Paibomsai",
				"school":        "Shingwauk, Sault Ste. Marie, ON",
				"date_of_death": "1913-05-01",
			},
			withCanonical(map[string]any{
				"display_name": "Lena Paibomsai",
				"school":       "Shingwauk, Sault Ste. Marie, ON",
				"dates": map[string]any{
					"death": map[string]any{
						"raw":  "1913-05-01",
						"iso":  "1913-05-01",
						"year": 1913,
					},
				},
			}),
		),
		normalizedChatRowJSON(
			map[string]any{
				"name":          "Andrew Johnson",
				"school":        "Shingwauk, Sault Ste. Marie, ON",
				"date_of_death": "1906-03-07",
			},
			withCanonical(map[string]any{
				"display_name": "Andrew Johnson",
				"school":       "Shingwauk, Sault Ste. Marie, ON",
				"dates": map[string]any{
					"death": map[string]any{
						"raw":  "1906-03-07",
						"iso":  "1906-03-07",
						"year": 1906,
					},
				},
			}),
		),
	)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		t.Fatal("expected deterministic death-year route without LLM generation")
		return nil, nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("In what years did the highest number of deaths occur at Shingwauk?", nil, filename, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "1913") || !strings.Contains(result.Answer, "2") {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.Debug == nil || result.Debug.QueryType != "death_year_extreme" {
		t.Fatalf("expected deterministic death_year_extreme debug, got %#v", result.Debug)
	}
}

func TestChatService_Chat_UsesProPrimaryForBroadCompactPrompt(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	rows := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		rows = append(rows, normalizedChatRowJSON(
			map[string]any{
				"name":      "Student " + strconv.Itoa(i+1),
				"community": "Garden River",
				"school":    "Shingwauk",
			},
			withCanonical(map[string]any{
				"display_name": "Student " + strconv.Itoa(i+1),
				"community":    "Garden River",
				"school":       "Shingwauk",
			}),
		))
	}
	expectChatNormalizedRowsLookup(mock, rows...)

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})

	calls := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		calls++
		if calls == 1 && model != "gemini-2.5-pro" {
			t.Fatalf("expected pro as primary model for broad compact prompt, got %q", model)
		}
		return jsonStructuredAnswer("I found 20 matching student records.", nil), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Show the records", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Debug == nil {
		t.Fatal("expected debug metrics")
	}
	if result.Debug.PrimaryModel != "gemini-2.5-pro" {
		t.Fatalf("primary model = %q want gemini-2.5-pro", result.Debug.PrimaryModel)
	}
	if result.Debug.UsedModel != "gemini-2.5-pro" {
		t.Fatalf("used model = %q want gemini-2.5-pro", result.Debug.UsedModel)
	}
}

func TestDescribeRow_RowNotFound(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "row_data", "inserted_by", "created_at", "updated_at", "version"}))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.DescribeRow(99)
	if err == nil || !strings.Contains(err.Error(), "row not found") {
		t.Fatalf("expected row not found, got %v", err)
	}
}

func TestGenerateWith429Fallback_UsesFallbackModel(t *testing.T) {
	old := genaiGenerateContentHook
	defer func() { genaiGenerateContentHook = old }()

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, _ []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		if config != nil {
			t.Fatalf("expected nil config, got %#v", config)
		}
		if model == "gemini-2.5-flash" {
			return nil, &googleapi.Error{Code: http.StatusTooManyRequests}
		}
		return rawAnswer("fallback ok"), nil
	}

	cs := &ChatService{Client: &genai.Client{}}
	resp, model, err := cs.generateWith429Fallback(context.Background(), []*genai.Content{{Role: "user", Parts: []*genai.Part{{Text: "hi"}}}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if model != "gemini-2.5-pro" {
		t.Fatalf("model = %q want gemini-2.5-pro", model)
	}
	if firstTextFromResp(resp) != "fallback ok" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}
