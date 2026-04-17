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
	p, err := w.CreatePart(partHeader.toHeader())
	if err != nil {
		t.Fatalf("CreatePart: %v", err)
	}
	_, _ = p.Write(data)
	_ = w.Close()

	req := httptest.NewRequest(http.MethodPost, "/x", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	if err := req.ParseMultipartForm(10 << 20); err != nil {
		t.Fatalf("ParseMultipartForm: %v", err)
	}
	return req.MultipartForm.File["audio"][0]
}

// Tiny helper to avoid importing net/textproto (keeps your deps minimal)
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
	if parseRateFromMime("audio/L16;rate=abc") != 0 {
		t.Fatal("expected 0 for bad rate")
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
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("q", nil, "missing.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "file not found") {
		t.Fatalf("expected file not found, got %v", err)
	}
}

func TestChatService_Chat_FileDataError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnError(errors.New("db fail"))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("q", nil, "sheet.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "file data not found") {
		t.Fatalf("expected file data not found, got %v", err)
	}
}

func TestChatService_Chat_MarshalError_InvalidRowJSON(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	// invalid JSON (RawMessage must be valid, otherwise json.Marshal fails)
	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{bad`))
	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("q", nil, "sheet.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "failed to marshal file data") {
		t.Fatalf("expected marshal error, got %v", err)
	}
}

func TestChatService_Chat_GenerationError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"First Nation/Community":"A"}`))
	old := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content) (*genai.GenerateContentResponse, error) {
		return nil, errors.New("gemini down")
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("q", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("expected heuristic + fallback answer, got err=%v", err)
	}
	if strings.TrimSpace(result.Answer) == "" {
		t.Fatalf("expected fallback answer, got %#v", result)
	}
}

func TestChatService_Chat_NoResponseFromGemini(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"First Nation/Community":"A"}`))
	old := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content) (*genai.GenerateContentResponse, error) {
		var out genai.GenerateContentResponse
		// No candidates -> response stays ""
		_ = json.Unmarshal([]byte(`{"candidates":[]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("q", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("expected heuristic + fallback answer, got err=%v", err)
	}
	if strings.TrimSpace(result.Answer) == "" {
		t.Fatalf("expected fallback answer, got %#v", result)
	}
}

func TestChatService_Chat_Audio_OpenError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"First Nation/Community":"A"}`))
	oldOpen := openMultipartFileHook
	openMultipartFileHook = func(_ *multipart.FileHeader) (multipart.File, error) {
		return nil, errors.New("open fail")
	}
	t.Cleanup(func() { openMultipartFileHook = oldOpen })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("q", &multipart.FileHeader{}, "sheet.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "failed to open audio file") {
		t.Fatalf("expected open error, got %v", err)
	}
}

func TestChatService_Chat_Audio_ReadError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"First Nation/Community":"A"}`))
	// Use real fileheader but force readAll error
	fh := makeAudioFileHeader(t, "audio/webm", []byte("xxx"))

	oldRead := readAllHook
	readAllHook = func(_ io.Reader) ([]byte, error) { return nil, errors.New("read fail") }
	t.Cleanup(func() { readAllHook = oldRead })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.Chat("q", fh, "sheet.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "failed to read audio file") {
		t.Fatalf("expected read error, got %v", err)
	}
}

func TestChatService_Chat_Audio_OctetStreamMimeFallback_Success(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"First Nation/Community":"A"}`))
	fh := makeAudioFileHeader(t, "application/octet-stream", []byte("abc"))

	oldGen := genaiGenerateContentHook
	callCount := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		callCount++
		b, _ := json.Marshal(contents)
		if callCount == 1 && !strings.Contains(string(b), "audio/webm") {
			t.Fatalf("expected audio/webm fallback on planner call, got %s", string(b))
		}
		var out genai.GenerateContentResponse
		if callCount == 1 {
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"intent\":\"describe_subject\",\"transcribed_question\":\"q\"}"}]}}]}`), &out)
			return &out, nil
		}
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"AUDIO_OK"}]}}]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = oldGen })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("q", fh, "sheet.xlsx", nil)
	if err != nil || result.Answer != "AUDIO_OK" {
		t.Fatalf("expected AUDIO_OK, got result=%#v err=%v", result, err)
	}
}

func TestChatService_Chat_ReturnsMatchedRowID_WhenSinglePersonMatchFound(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(42), uint(1), 2, `{"firstname":"Audrey","lastname":"Lesage"}`).
			AddRow(uint(99), uint(1), 2, `{"firstname":"Mary","lastname":"Martin"}`))
	old := genaiGenerateContentHook
	genaiGenerateContentHook = plannerThenAnswerHook(
		t,
		`{"intent":"describe_subject","subject_text":"Audry Lesage"}`,
		`MATCH_OK`,
		nil,
		func(prompt string) {
			if !strings.Contains(prompt, `"row_id": 42`) {
				t.Fatalf("expected verified result to include matched row 42, got %s", prompt)
			}
		},
	)
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Tell me about Audry Lesage", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Answer != "MATCH_OK" {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.MatchedRowID == nil || *result.MatchedRowID != 42 {
		t.Fatalf("expected matched_row_id=42, got %#v", result)
	}
}

func TestChatService_Chat_RespectsNullMatchedRowRefFromAI(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(42), uint(1), 2, `{"firstname":"Audrey","lastname":"Lesage"}`))
	old := genaiGenerateContentHook
	genaiGenerateContentHook = plannerThenAnswerHook(
		t,
		`{"intent":"describe_subject","subject_text":"Audrey Lesage"}`,
		`{"answer":"MATCH_OK","matched_row_ref":null}`,
		nil,
		nil,
	)
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Tell me about Audrey Lesage", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Answer != "MATCH_OK" {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.MatchedRowID == nil || *result.MatchedRowID != 42 {
		t.Fatalf("expected backend matched row id 42, got %#v", result)
	}
}

func TestChatService_Chat_DoesNotReturnMatchedRowID_WhenMultipleRowsMatch(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(42), uint(1), 2, `{"firstname":"Audrey","lastname":"Lesage"}`).
			AddRow(uint(43), uint(1), 2, `{"Resident Name":"Audrey Lesage"}`))
	old := genaiGenerateContentHook
	genaiGenerateContentHook = plannerThenAnswerHook(
		t,
		`{"intent":"describe_subject","subject_text":"Audrey Lesage"}`,
		`AMBIGUOUS_OK`,
		nil,
		nil,
	)
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Tell me about Audrey Lesage", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if result.Answer != "AMBIGUOUS_OK" {
		t.Fatalf("unexpected answer: %#v", result)
	}
	if result.MatchedRowID != nil {
		t.Fatalf("expected no matched row for ambiguous result, got %#v", result)
	}
}

func TestChatService_Chat_CachesFileDataByFileVersion(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))
	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "row_data"}).
			AddRow(uint(42), `{"firstname":"Audrey","lastname":"Lesage"}`))
	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	old := genaiGenerateContentHook
	callCount := 0
	genaiGenerateContentHook = plannerThenAnswerHook(
		t,
		`{"intent":"describe_subject","subject_text":"Audrey Lesage"}`,
		`CACHE_OK`,
		nil,
		nil,
	)
	wrapped := genaiGenerateContentHook
	genaiGenerateContentHook = func(client *genai.Client, ctx context.Context, model string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		callCount++
		return wrapped(client, ctx, model, contents)
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	for i := 0; i < 2; i++ {
		result, err := cs.Chat("Tell me about Audrey Lesage", nil, "sheet.xlsx", nil)
		if err != nil {
			t.Fatalf("unexpected err on run %d: %v", i, err)
		}
		if result.Answer != "CACHE_OK" {
			t.Fatalf("unexpected answer on run %d: %#v", i, result)
		}
	}
	if callCount != 4 {
		t.Fatalf("expected 4 gemini calls, got %d", callCount)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatService_Chat_UsesInMemorySessionFocusOnFollowup(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))
	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(42), uint(1), 2, `{"firstname":"Audrey","lastname":"Lesage","date_of_death":"1901-05-06"}`))
	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	old := genaiGenerateContentHook
	callCount := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		callCount++
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected prompt contents, got %#v", contents)
		}
		prompt := contents[0].Parts[0].Text
		var out genai.GenerateContentResponse
		switch callCount {
		case 1:
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"intent\":\"describe_subject\",\"subject_text\":\"Audry Lesage\"}"}]}}]}`), &out)
		case 2:
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"FIRST_OK"}]}}]}`), &out)
		case 3:
			if !strings.Contains(prompt, "Current focus: Audrey Lesage") {
				t.Fatalf("expected session focus in planner prompt, got:\n%s", prompt)
			}
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"intent\":\"field_lookup\",\"use_session_focus\":true,\"target_field_id\":\"date_of_death\"}"}]}}]}`), &out)
		case 4:
			if !strings.Contains(prompt, `"target_field_label": "date_of_death"`) && !strings.Contains(prompt, `"target_field_label":"date_of_death"`) {
				t.Fatalf("expected verified result to target date_of_death, got:\n%s", prompt)
			}
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"SECOND_OK"}]}}]}`), &out)
		default:
			t.Fatalf("unexpected extra call %d", callCount)
		}
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	first, err := cs.ChatForUser(7, "Tell me about Audry Lesage", nil, "sheet.xlsx", nil)
	if err != nil || first.Answer != "FIRST_OK" {
		t.Fatalf("unexpected first result: %#v err=%v", first, err)
	}

	second, err := cs.ChatForUser(7, "When did she die?", nil, "sheet.xlsx", nil)
	if err != nil || second.Answer != "SECOND_OK" {
		t.Fatalf("unexpected second result: %#v err=%v", second, err)
	}
	if second.MatchedRowID == nil || *second.MatchedRowID != 42 {
		t.Fatalf("expected matched row id on follow-up, got %#v", second)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestParseTemporalValue_Examples(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		kind        temporalKind
		approx      bool
		lower       string
		upper       string
		alternatives int
	}{
		{name: "approx year", raw: "abt 1890", kind: temporalKindApproximate, approx: true, lower: "1890-01-01", upper: "1890-12-31"},
		{name: "year range", raw: "1890-1891", kind: temporalKindRange, lower: "1890-01-01", upper: "1891-12-31"},
		{name: "partial month", raw: "1904-07-00", kind: temporalKindExactMonth, lower: "1904-07-01", upper: "1904-07-31"},
		{name: "alternatives", raw: "1888-03-30 (1888-03-03)", kind: temporalKindAlternative, lower: "1888-03-03", upper: "1888-03-30", alternatives: 2},
		{name: "before", raw: "bef. 2010", kind: temporalKindBefore, upper: "2009-12-31"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTemporalValue(tt.raw)
			if got.Kind != tt.kind {
				t.Fatalf("kind = %s want %s", got.Kind, tt.kind)
			}
			if got.Approximate != tt.approx {
				t.Fatalf("approximate = %v want %v", got.Approximate, tt.approx)
			}
			if tt.lower != "" {
				if got.Lower == nil || got.Lower.String() != tt.lower {
					t.Fatalf("lower = %#v want %s", got.Lower, tt.lower)
				}
			}
			if tt.upper != "" {
				if got.Upper == nil || got.Upper.String() != tt.upper {
					t.Fatalf("upper = %#v want %s", got.Upper, tt.upper)
				}
			}
			if len(got.Alternatives) != tt.alternatives {
				t.Fatalf("alternatives = %d want %d", len(got.Alternatives), tt.alternatives)
			}
		})
	}
}

func TestChatService_TTS_ProjectMissing(t *testing.T) {
	cs := &ChatService{ProjectID: "", Location: ""}
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	_, err := cs.TTS("hi")
	if err == nil || !strings.Contains(err.Error(), "missing project id") {
		t.Fatalf("expected missing project id, got %v", err)
	}
}

func TestChatService_TTS_HTTPClientError(t *testing.T) {
	cs := &ChatService{ProjectID: "p", Location: "global"}

	old := defaultHTTPClientHook
	defaultHTTPClientHook = func(context.Context) (*http.Client, error) {
		return nil, errors.New("adc fail")
	}
	t.Cleanup(func() { defaultHTTPClientHook = old })

	_, err := cs.TTS("hi")
	if err == nil || !strings.Contains(err.Error(), "adc auth error") {
		t.Fatalf("expected adc auth error, got %v", err)
	}
}

func TestChatService_TTS_DoError(t *testing.T) {
	cs := &ChatService{ProjectID: "p", Location: "global"}

	oldC := defaultHTTPClientHook
	oldDo := httpDoHook
	defaultHTTPClientHook = func(context.Context) (*http.Client, error) { return &http.Client{}, nil }
	httpDoHook = func(_ *http.Client, _ *http.Request) (*http.Response, error) {
		return nil, errors.New("net down")
	}
	t.Cleanup(func() { defaultHTTPClientHook = oldC; httpDoHook = oldDo })

	_, err := cs.TTS("hi")
	if err == nil || !strings.Contains(err.Error(), "vertex tts request error") {
		t.Fatalf("expected request error, got %v", err)
	}
}

func TestChatService_TTS_StatusError(t *testing.T) {
	cs := &ChatService{ProjectID: "p", Location: "global"}

	oldC := defaultHTTPClientHook
	oldDo := httpDoHook
	defaultHTTPClientHook = func(context.Context) (*http.Client, error) { return &http.Client{}, nil }
	httpDoHook = func(_ *http.Client, _ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 500,
			Body:       io.NopCloser(strings.NewReader(`{"error":"boom"}`)),
		}, nil
	}
	t.Cleanup(func() { defaultHTTPClientHook = oldC; httpDoHook = oldDo })

	_, err := cs.TTS("hi")
	if err == nil || !strings.Contains(err.Error(), "vertex tts failed") {
		t.Fatalf("expected status error, got %v", err)
	}
}

func TestChatService_TTS_Success_NonGlobalHost_AndWav(t *testing.T) {
	cs := &ChatService{ProjectID: "proj", Location: "us-central1"}

	pcm := []byte{0x01, 0x02, 0x03, 0x04}
	b64 := base64.StdEncoding.EncodeToString(pcm)

	var capturedURL string

	oldC := defaultHTTPClientHook
	oldDo := httpDoHook
	defaultHTTPClientHook = func(context.Context) (*http.Client, error) { return &http.Client{}, nil }
	httpDoHook = func(_ *http.Client, req *http.Request) (*http.Response, error) {
		capturedURL = req.URL.String()
		body := `{"candidates":[{"content":{"parts":[{"inlineData":{"mimeType":"audio/L16;rate=48000","data":"` + b64 + `"}}]}}]}`
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
	t.Cleanup(func() { defaultHTTPClientHook = oldC; httpDoHook = oldDo })

	audio, err := cs.TTS("hello")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(capturedURL, "us-central1-aiplatform.googleapis.com") {
		t.Fatalf("expected non-global host, got %s", capturedURL)
	}
	if audio.MimeType != "audio/wav" {
		t.Fatalf("expected audio/wav, got %s", audio.MimeType)
	}
	if len(audio.Data) < 12 || string(audio.Data[:4]) != "RIFF" || string(audio.Data[8:12]) != "WAVE" {
		t.Fatalf("bad wav header: %q", audio.Data[:12])
	}
}

func TestChatService_DescribeRow_DBNotInitialized(t *testing.T) {
	cs := &ChatService{DB: nil, Client: &genai.Client{}}
	_, err := cs.DescribeRow(1)
	if err == nil || !strings.Contains(err.Error(), "db not initialized") {
		t.Fatalf("expected db not initialized, got %v", err)
	}
}

func TestChatService_DescribeRow_ClientNotInitialized(t *testing.T) {
	db, _, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	cs := &ChatService{DB: db, Client: nil}
	_, err := cs.DescribeRow(1)
	if err == nil || !strings.Contains(err.Error(), "genai client not initialized") {
		t.Fatalf("expected client not initialized, got %v", err)
	}
}

func TestChatService_DescribeRow_RowNotFound(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.DescribeRow(123)
	if err == nil || !strings.Contains(err.Error(), "row not found") {
		t.Fatalf("expected row not found, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatService_DescribeRow_GenerationError(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(1, 1, 1, `{"Name":"Jane"}`))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content) (*genai.GenerateContentResponse, error) {
		return nil, errors.New("gemini down")
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.DescribeRow(1)
	if err == nil || !strings.Contains(err.Error(), "generation error") {
		t.Fatalf("expected generation error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatService_DescribeRow_NoResponseFromGemini(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(1, 1, 1, `{"Name":"Jane"}`))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content) (*genai.GenerateContentResponse, error) {
		var out genai.GenerateContentResponse
		_ = json.Unmarshal([]byte(`{"candidates":[]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	_, err := cs.DescribeRow(1)
	if err == nil || !strings.Contains(err.Error(), "no response from Gemini") {
		t.Fatalf("expected no response error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatService_DescribeRow_Success_PromptContainsRecord(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(1, 1, 1, `{"Name":"John Doe","First Nation/Community":"B"}`))

	old := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected contents with prompt text, got: %#v", contents)
		}
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"Name":"John Doe"`) {
			t.Fatalf("expected prompt to include row JSON, got prompt:\n%s", prompt)
		}
		var out genai.GenerateContentResponse
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"DESC_OK"}]}}]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	ans, err := cs.DescribeRow(1)
	if err != nil || ans != "DESC_OK" {
		t.Fatalf("expected DESC_OK, got ans=%q err=%v", ans, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatService_DescribeRow_429FallbackToPro(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(1, 1, 1, `{"Name":"Fallback Person"}`))

	old := genaiGenerateContentHook
	calls := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, _ []*genai.Content) (*genai.GenerateContentResponse, error) {
		calls++
		if calls == 1 {
			if model != "gemini-2.5-flash" {
				t.Fatalf("expected first model flash, got %s", model)
			}
			return nil, &googleapi.Error{Code: 429, Message: "RESOURCE_EXHAUSTED"}
		}
		if calls == 2 {
			if model != "gemini-2.5-pro" {
				t.Fatalf("expected fallback model pro, got %s", model)
			}
			var out genai.GenerateContentResponse
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"PRO_OK"}]}}]}`), &out)
			return &out, nil
		}
		t.Fatalf("unexpected extra call %d", calls)
		return nil, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = old })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	ans, err := cs.DescribeRow(1)
	if err != nil || ans != "PRO_OK" {
		t.Fatalf("expected PRO_OK, got ans=%q err=%v", ans, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
