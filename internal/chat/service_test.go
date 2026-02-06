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

func Test_matchesCommunities(t *testing.T) {
	if matchesCommunities([]byte(`not-json`), []string{"A"}) {
		t.Fatal("expected false for invalid json")
	}
	if matchesCommunities([]byte(`{"x":1}`), []string{"A"}) {
		t.Fatal("expected false when key missing")
	}
	if matchesCommunities([]byte(`{"First Nation/Community":123}`), []string{"A"}) {
		t.Fatal("expected false when value not string")
	}
	if !matchesCommunities([]byte(`{"First Nation/Community":" A "}`), []string{"A"}) {
		t.Fatal("expected true when trimmed matches")
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
	_, err := cs.Chat("q", nil, "sheet.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "generation error") {
		t.Fatalf("expected generation error, got %v", err)
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
	_, err := cs.Chat("q", nil, "sheet.xlsx", nil)
	if err == nil || !strings.Contains(err.Error(), "no response from Gemini") {
		t.Fatalf("expected no response error, got %v", err)
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
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		b, _ := json.Marshal(contents)
		if !strings.Contains(string(b), "audio/webm") {
			t.Fatalf("expected audio/webm fallback, got %s", string(b))
		}
		var out genai.GenerateContentResponse
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"AUDIO_OK"}]}}]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = oldGen })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	ans, err := cs.Chat("q", fh, "sheet.xlsx", nil)
	if err != nil || ans != "AUDIO_OK" {
		t.Fatalf("expected AUDIO_OK, got ans=%q err=%v", ans, err)
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
