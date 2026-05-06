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

func expectChatFileLookup(mock sqlmock.Sqlmock, filename string) {
	mock.ExpectQuery(`(?i)select.*id.*version.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "version"}).
			AddRow(uint(1), 1))
}

func expectChatRowsLookup(mock sqlmock.Sqlmock, rows ...string) {
	result := sqlmock.NewRows([]string{"id", "row_data"})
	for idx, row := range rows {
		result.AddRow(uint(idx+1), row)
	}
	mock.ExpectQuery(`(?i)select.*id.*row_data.*from.*file_data.*where.*file_id.*version.*order.*id.*asc`).
		WillReturnRows(result)
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
		WillReturnRows(sqlmock.NewRows([]string{"id", "version"}))

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
	expectChatRowsLookup(mock, `{"NAME":"Alice","First Nation/Community":"Garden River"}`)

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
	expectChatRowsLookup(mock, `{"NAME":"Alice","First Nation/Community":"Garden River"}`)

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

func TestChatService_Chat_FallsBackToProOn429(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock, `{"NAME":"Alice","First Nation/Community":"Garden River"}`)

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
}

func TestChatService_Chat_AudioReadError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock, `{"NAME":"Alice"}`)

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
