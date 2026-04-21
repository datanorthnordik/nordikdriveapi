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

func expectChatFileLookup(mock sqlmock.Sqlmock, filename string) {
	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version", "columns_order"}).
			AddRow(uint(1), filename, 1, `["NAME","SCHOOL","DATE OF DEATH","CAUSE OF DEATH","First Nation/Community"]`))
	mock.ExpectQuery(`(?i)select.*from.*data_config.*where.*is_active.*lower\(file_name\)`).
		WillReturnError(gorm.ErrRecordNotFound)
}

func expectChatRowsLookup(mock sqlmock.Sqlmock, rows ...string) {
	result := sqlmock.NewRows([]string{"id", "row_data"})
	for idx, row := range rows {
		result.AddRow(uint(idx+1), row)
	}
	mock.ExpectQuery(`(?i)select.*id.*row_data.*from.*file_data.*where.*file_id.*version.*order.*id.*asc`).
		WillReturnRows(result)
}

func jsonAnswer(answer string, matched *string, needsClarification bool, clarification string) *genai.GenerateContentResponse {
	text := answer
	if needsClarification && strings.TrimSpace(clarification) != "" {
		text = clarification
	}
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

func TestFirstTextFromResp_ConcatenatesParts(t *testing.T) {
	var resp genai.GenerateContentResponse
	_ = json.Unmarshal([]byte(`{
		"candidates":[
			{"content":{"parts":[
				{"text":"{\"answer\":\"Based on the records, Walpole Island"},
				{"text":" had the most deaths.\"}"}
			]}}
		]
	}`), &resp)

	got := firstTextFromResp(&resp)
	want := `{"answer":"Based on the records, Walpole Island had the most deaths."}`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestSanitizeAnswerText_WrappedJSONString(t *testing.T) {
	raw := "\"{\\n  \\\"answer\\\": \\\"Based on the records, Walpole Island had the most deaths.\\\"\\n}\""
	got := sanitizeAnswerText(raw)
	if got != "Based on the records, Walpole Island had the most deaths." {
		t.Fatalf("got %q", got)
	}
}

func TestSanitizeAnswerText_FallbackAnswerField(t *testing.T) {
	raw := "{\n  \"answer\": \"Based on the records, Walpole Island is the community that experienced the most deaths.\""
	got := sanitizeAnswerText(raw)
	if got != "Based on the records, Walpole Island is the community that experienced the most deaths." {
		t.Fatalf("got %q", got)
	}
}

func TestSanitizeAnswerText_UserReportedTruncatedJSONString(t *testing.T) {
	raw := ":\n\"{\\n  \\\"answer\\\": \\\"Based on the records, Walpole Island is the community that experienced the most deaths, with a total of\""
	got := sanitizeAnswerText(raw)
	if got != "Based on the records, Walpole Island is the community that experienced the most deaths, with a total of" {
		t.Fatalf("got %q", got)
	}
}

func TestSanitizeAnswerText_StripsAnswerLabel(t *testing.T) {
	raw := "answer: Based on the records, Walpole Island had the most deaths."
	got := sanitizeAnswerText(raw)
	if got != "Based on the records, Walpole Island had the most deaths." {
		t.Fatalf("got %q", got)
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

func TestChatService_Chat_UsesContextCache(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock,
		`{"NAME":"Alice","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-05-06","CAUSE OF DEATH":"Drowned while crossing the river","First Nation/Community":"Garden River"}`,
	)

	oldCreate := genaiCreateCachedContentHook
	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiCreateCachedContentHook = oldCreate
		genaiGenerateContentHook = oldGenerate
	})

	createCalls := 0
	genaiCreateCachedContentHook = func(_ *genai.Client, _ context.Context, model string, config *genai.CreateCachedContentConfig) (*genai.CachedContent, error) {
		createCalls++
		if model != chatPrimaryModel {
			t.Fatalf("unexpected cache model %q", model)
		}
		if config == nil || len(config.Contents) != 1 || len(config.Contents[0].Parts) != 1 {
			t.Fatalf("unexpected cache config: %#v", config)
		}
		if !strings.Contains(config.Contents[0].Parts[0].Text, `"row_ref":"R1"`) {
			t.Fatalf("expected row ref in cache body, got:\n%s", config.Contents[0].Parts[0].Text)
		}
		return &genai.CachedContent{
			Name:       "projects/demo/locations/global/cachedContents/cache-1",
			ExpireTime: time.Now().Add(time.Hour),
		}, nil
	}

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		if model != chatPrimaryModel {
			t.Fatalf("unexpected generate model %q", model)
		}
		if config == nil || config.CachedContent == "" {
			t.Fatalf("expected cached content config, got %#v", config)
		}
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected prompt contents, got %#v", contents)
		}
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "stands on its own") {
			t.Fatalf("expected standalone prompt instructions, got:\n%s", prompt)
		}
		return jsonAnswer("Yes. I found 1 recorded death connected to drowning at Shingwauk.", nil, false, ""), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Did any children die from drowning at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("expected 1 cache create call, got %d", createCalls)
	}
	if !strings.Contains(result.Answer, "Yes.") {
		t.Fatalf("unexpected answer: %#v", result)
	}
}

func TestChatService_Chat_ReusesContextCacheAndIgnoresStandaloneHistory(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock,
		`{"NAME":"Alice","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-05-06","CAUSE OF DEATH":"Drowned while crossing the river","First Nation/Community":"Garden River"}`,
	)
	expectChatFileLookup(mock, "sheet.xlsx")

	oldCreate := genaiCreateCachedContentHook
	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiCreateCachedContentHook = oldCreate
		genaiGenerateContentHook = oldGenerate
	})

	createCalls := 0
	generateCalls := 0
	genaiCreateCachedContentHook = func(_ *genai.Client, _ context.Context, _ string, _ *genai.CreateCachedContentConfig) (*genai.CachedContent, error) {
		createCalls++
		return &genai.CachedContent{
			Name:       "projects/demo/locations/global/cachedContents/cache-1",
			ExpireTime: time.Now().Add(time.Hour),
		}, nil
	}

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		generateCalls++
		prompt := contents[0].Parts[0].Text
		if generateCalls == 2 {
			if !strings.Contains(prompt, "stands on its own") {
				t.Fatalf("expected standalone prompt on second call, got:\n%s", prompt)
			}
			if strings.Contains(prompt, "Recent conversation:") {
				t.Fatalf("did not expect recent conversation in fresh question prompt:\n%s", prompt)
			}
			if config == nil || config.CachedContent == "" {
				t.Fatalf("expected cache reuse on second call, got %#v", config)
			}
		}
		answer := "First answer."
		if generateCalls == 2 {
			answer = "Second answer."
		}
		return jsonAnswer(answer, nil, false, ""), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	if _, err := cs.ChatForUser(77, "Who is Alice?", nil, "sheet.xlsx", nil); err != nil {
		t.Fatalf("first chat err: %v", err)
	}
	if _, err := cs.ChatForUser(77, "How many records are in the data?", nil, "sheet.xlsx", nil); err != nil {
		t.Fatalf("second chat err: %v", err)
	}

	if createCalls != 1 {
		t.Fatalf("expected cache create once, got %d", createCalls)
	}
	if generateCalls != 2 {
		t.Fatalf("expected generate twice, got %d", generateCalls)
	}
}

func TestChatService_Chat_FallsBackToDirectPromptWhenCacheCreateFails(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock, `{"NAME":"Alice","First Nation/Community":"Garden River"}`)

	oldCreate := genaiCreateCachedContentHook
	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiCreateCachedContentHook = oldCreate
		genaiGenerateContentHook = oldGenerate
	})

	genaiCreateCachedContentHook = func(_ *genai.Client, _ context.Context, _ string, _ *genai.CreateCachedContentConfig) (*genai.CachedContent, error) {
		return nil, errors.New("cache unavailable")
	}

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		if config == nil || config.CachedContent != "" {
			t.Fatalf("expected direct prompt fallback, got %#v", config)
		}
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "DATA (only source of truth):") {
			t.Fatalf("expected direct prompt with data, got:\n%s", prompt)
		}
		return jsonAnswer("No. I couldn't find any matching records in the available data.", nil, false, ""), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("Did any children die from drowning at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "No.") {
		t.Fatalf("unexpected answer: %#v", result)
	}
}

func TestChatService_Chat_ClarificationBudgetStopsLoops(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock, `{"NAME":"John Smith","First Nation/Community":"Garden River"}`)
	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatFileLookup(mock, "sheet.xlsx")

	oldCreate := genaiCreateCachedContentHook
	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiCreateCachedContentHook = oldCreate
		genaiGenerateContentHook = oldGenerate
	})

	genaiCreateCachedContentHook = func(_ *genai.Client, _ context.Context, _ string, _ *genai.CreateCachedContentConfig) (*genai.CachedContent, error) {
		return &genai.CachedContent{
			Name:       "projects/demo/locations/global/cachedContents/cache-clarify",
			ExpireTime: time.Now().Add(time.Hour),
		}, nil
	}

	call := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		call++
		prompt := contents[0].Parts[0].Text
		if call == 2 && !strings.Contains(prompt, "Original question:") {
			t.Fatalf("expected original question in clarification reply prompt, got:\n%s", prompt)
		}
		switch call {
		case 1:
			return jsonAnswer("Which John do you mean?", nil, true, "Which John do you mean?"), nil
		case 2:
			return jsonAnswer("Which school John do you mean?", nil, true, "Which school John do you mean?"), nil
		default:
			return jsonAnswer("Could you clarify which John exactly?", nil, true, "Could you clarify which John exactly?"), nil
		}
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}

	first, err := cs.ChatForUser(22, "Tell me about John", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("first chat err: %v", err)
	}
	if first.Answer != "Which John do you mean?" {
		t.Fatalf("unexpected first answer: %#v", first)
	}

	second, err := cs.ChatForUser(22, "Garden River", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("second chat err: %v", err)
	}
	if second.Answer != "Which school John do you mean?" {
		t.Fatalf("unexpected second answer: %#v", second)
	}

	third, err := cs.ChatForUser(22, "the one at the school", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("third chat err: %v", err)
	}
	if third.Answer != "I can't determine that exactly from the available data." {
		t.Fatalf("unexpected third answer: %#v", third)
	}
}

func TestChatService_Chat_RetriesMalformedTruncatedResponse(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock, `{"NAME":"Alice","First Nation/Community":"Garden River"}`)

	oldCreate := genaiCreateCachedContentHook
	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiCreateCachedContentHook = oldCreate
		genaiGenerateContentHook = oldGenerate
	})

	genaiCreateCachedContentHook = func(_ *genai.Client, _ context.Context, _ string, _ *genai.CreateCachedContentConfig) (*genai.CachedContent, error) {
		return &genai.CachedContent{
			Name:       "projects/demo/locations/global/cachedContents/cache-1",
			ExpireTime: time.Now().Add(time.Hour),
		}, nil
	}

	call := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		call++
		if call == 2 {
			if !strings.Contains(contents[0].Parts[0].Text, "previous response was malformed or truncated") {
				t.Fatalf("expected repair retry prompt, got:\n%s", contents[0].Parts[0].Text)
			}
		}

		raw := `{"answer":"Based on the records provided, the year with the highest number of deaths was 1913, when six students passed away. The years 1882 and 1906 also had a"}`
		if call == 1 {
			var out genai.GenerateContentResponse
			_ = json.Unmarshal([]byte(`{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":`+strconvQuote(raw)+`}]}}]}`), &out)
			return &out, nil
		}

		return jsonAnswer("Based on the records provided, the year with the highest number of deaths was 1913, when six students passed away. The years 1882 and 1906 also had five deaths each.", nil, false, ""), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("In what years did the highest number of deaths occur?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if call != 2 {
		t.Fatalf("expected 2 generate calls, got %d", call)
	}
	if result.Answer != "Based on the records provided, the year with the highest number of deaths was 1913, when six students passed away. The years 1882 and 1906 also had five deaths each." {
		t.Fatalf("unexpected answer: %#v", result)
	}
}

func TestChatService_Chat_DeterministicHighestDeathsByYearAtScope(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	rows := []string{
		`{"NAME":"A","SCHOOL":"Shingwauk","DATE OF DEATH":"1913-01-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"B","SCHOOL":"Shingwauk","DATE OF DEATH":"1913-02-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"C","SCHOOL":"Shingwauk","DATE OF DEATH":"1913-03-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"D","SCHOOL":"Shingwauk","DATE OF DEATH":"1913-04-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"E","SCHOOL":"Shingwauk","DATE OF DEATH":"1913-05-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"F","SCHOOL":"Shingwauk","DATE OF DEATH":"1913-06-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"G","SCHOOL":"Shingwauk","DATE OF DEATH":"1882-01-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"H","SCHOOL":"Shingwauk","DATE OF DEATH":"1882-02-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"I","SCHOOL":"Shingwauk","DATE OF DEATH":"1882-03-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"J","SCHOOL":"Shingwauk","DATE OF DEATH":"1882-04-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"K","SCHOOL":"Shingwauk","DATE OF DEATH":"1882-05-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"L","SCHOOL":"Other School","DATE OF DEATH":"1913-01-01","First Nation/Community":"Garden River"}`,
		`{"NAME":"M","SCHOOL":"Other School","DATE OF DEATH":"1913-02-01","First Nation/Community":"Garden River"}`,
	}
	expectChatRowsLookup(mock, rows...)

	oldCreate := genaiCreateCachedContentHook
	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiCreateCachedContentHook = oldCreate
		genaiGenerateContentHook = oldGenerate
	})

	genaiCreateCachedContentHook = func(_ *genai.Client, _ context.Context, _ string, _ *genai.CreateCachedContentConfig) (*genai.CachedContent, error) {
		t.Fatal("did not expect cache creation for deterministic aggregate answer")
		return nil, nil
	}

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected verified-result prompt, got %#v", contents)
		}
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "VERIFIED RESULT:") {
			t.Fatalf("expected verified-result prompt, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, "Highest death count in a single year: 6") {
			t.Fatalf("expected verified count in prompt, got:\n%s", prompt)
		}
		if !strings.Contains(prompt, "Year(s): 1913") {
			t.Fatalf("expected verified year in prompt, got:\n%s", prompt)
		}
		return jsonAnswer("The highest number of deaths at Shingwauk occurred in 1913, when 6 students died.", nil, false, ""), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	result, err := cs.Chat("In what years did the highest number of deaths occur at Shingwauk?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(result.Answer, "1913") || !strings.Contains(result.Answer, "6") {
		t.Fatalf("unexpected deterministic answer: %#v", result)
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

	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, model string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
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
