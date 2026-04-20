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
	"time"

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

func plannerThenAnswerHook(t *testing.T, plannerJSON string, answerText string, inspectPlanner func(string), inspectAnswer func(string)) func(*genai.Client, context.Context, string, []*genai.Content) (*genai.GenerateContentResponse, error) {
	t.Helper()
	return func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected prompt contents, got %#v", contents)
		}
		prompt := contents[0].Parts[0].Text
		var out genai.GenerateContentResponse
		if strings.Contains(prompt, "VERIFIED RESULT (only source of truth):") {
			if inspectAnswer != nil {
				inspectAnswer(prompt)
			}
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":`+strconv.Quote(answerText)+`}]}}]}`), &out)
			return &out, nil
		}
		if inspectPlanner != nil {
			inspectPlanner(prompt)
		}
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":`+strconv.Quote(plannerJSON)+`}]}}]}`), &out)
		return &out, nil
	}
}

func expectChatDatasetQueries(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}).
			AddRow(uint(1), "sheet.xlsx", 1, `["NAME","SCHOOL","DATE OF DEATH","CAUSE OF DEATH","First Nation/Community"]`))

	mock.ExpectQuery(`(?i)select.*from.*data_config.*where.*is_active.*lower\(file_name\)`).
		WillReturnError(gorm.ErrRecordNotFound)
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

func TestChatService_Chat_FileNotFound(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("q", nil, "missing.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "file not found") {
		t.Fatalf("expected file not found, got %v", err)
	}
}

func TestChatService_Chat_UsesVerifiedResultPrompt(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatDatasetQueries(mock)
	ts := time.Now().UTC()
	mock.ExpectQuery(`(?i)select.*from.*file_data fd.*left join.*file_data_normalized`).
		WillReturnRows(sqlmock.NewRows([]string{
			"row_id", "row_data", "row_updated_at", "row_data_normalized", "search_text", "canonical_name", "canonical_community", "canonical_school", "status", "source_updated_at",
		}).
			AddRow(uint(1), `{"NAME":"Alice","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-05-06","CAUSE OF DEATH":"Drowned while crossing the river","First Nation/Community":"Garden River"}`, ts, `{"fields":{"NAME":{"normalized":"alice","tokens":["alice"],"role":"name"},"SCHOOL":{"normalized":"shingwauk","tokens":["shingwauk"],"role":"school"},"DATE OF DEATH":{"normalized":"1890 05 06","tokens":["1890","05","06"],"role":"date"},"CAUSE OF DEATH":{"normalized":"drowned while crossing the river","tokens":["drowned","crossing","river"],"role":"text"},"First Nation/Community":{"normalized":"garden river","tokens":["garden","river"],"role":"community"}},"names":["Alice"],"communities":["garden river"],"schools":["shingwauk"],"search_tokens":["alice","shingwauk","drowned","crossing","river","garden"]}`, "alice shingwauk drowned crossing river garden", "alice", "garden river", "shingwauk", "ready", ts))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = plannerThenAnswerHook(
		t,
		`{"intent":"count_rows","search_terms":["drowning"],"filters":[{"field_id":"school","op":"eq","value":"Shingwauk"}],"limit":5}`,
		`Yes. I found 1 recorded death connected to drowning at Shingwauk.`,
		nil,
		func(prompt string) {
			if !strings.Contains(prompt, `"status": "ok"`) {
				t.Fatalf("expected verified result status in prompt, got:\n%s", prompt)
			}
			if !strings.Contains(prompt, `"value": 1`) {
				t.Fatalf("expected verified count in prompt, got:\n%s", prompt)
			}
			if !strings.Contains(prompt, `"CAUSE OF DEATH": "Drowned while crossing the river"`) && !strings.Contains(prompt, `"CAUSE OF DEATH":"Drowned while crossing the river"`) {
				t.Fatalf("expected verified row fields in prompt, got:\n%s", prompt)
			}
		},
	)
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Did any children die from drowning at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "Yes.") {
		t.Fatalf("unexpected answer: %#v", result)
	}
}

func TestChatService_Chat_AudioReadError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatDatasetQueries(mock)
	ts := time.Now().UTC()
	mock.ExpectQuery(`(?i)select.*from.*file_data fd.*left join.*file_data_normalized`).
		WillReturnRows(sqlmock.NewRows([]string{
			"row_id", "row_data", "row_updated_at", "row_data_normalized", "search_text", "canonical_name", "canonical_community", "canonical_school", "status", "source_updated_at",
		}).
			AddRow(uint(1), `{"NAME":"Alice"}`, ts, `{}`, "", "", "", "", "ready", ts))

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

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, _ []*genai.Content) (*genai.GenerateContentResponse, error) {
		if model == "gemini-2.5-flash" {
			return nil, &googleapi.Error{Code: http.StatusTooManyRequests}
		}
		var out genai.GenerateContentResponse
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"fallback ok"}]}}]}`), &out)
		return &out, nil
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
