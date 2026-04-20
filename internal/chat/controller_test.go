package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"google.golang.org/genai"
	"gorm.io/gorm"
)

func newMockDBChat(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	return newMockDBChatSvc(t)
}

func TestChatController_Chat_MissingFilename_400(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cc := NewChatController(&ChatService{})
	r := gin.New()
	r.POST("/chat", cc.Chat)

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("question", "hi")
	_ = w.Close()

	req := httptest.NewRequest(http.MethodPost, "/chat", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	res := httptest.NewRecorder()

	r.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", res.Code, res.Body.String())
	}
}

func TestChatController_Chat_Success200(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, mock, cleanup := newMockDBChat(t)
	defer cleanup()

	expectChatDatasetQueries(mock)
	ts := time.Now().UTC()
	mock.ExpectQuery(`(?i)select.*from.*file_data fd.*left join.*file_data_normalized`).
		WillReturnRows(sqlmock.NewRows([]string{
			"row_id", "row_data", "row_updated_at", "row_data_normalized", "search_text", "canonical_name", "canonical_community", "canonical_school", "status", "source_updated_at",
		}).
			AddRow(uint(1), `{"NAME":"Alice","SCHOOL":"Shingwauk","DATE OF DEATH":"1890-05-06","CAUSE OF DEATH":"Drowned while crossing the river","First Nation/Community":"Garden River"}`, ts, `{"fields":{"NAME":{"normalized":"alice","tokens":["alice"],"role":"name"},"SCHOOL":{"normalized":"shingwauk","tokens":["shingwauk"],"role":"school"},"DATE OF DEATH":{"normalized":"1890 05 06","tokens":["1890","05","06"],"role":"date"},"CAUSE OF DEATH":{"normalized":"drowned while crossing the river","tokens":["drowned","crossing","river"],"role":"text"},"First Nation/Community":{"normalized":"garden river","tokens":["garden","river"],"role":"community"}},"names":["Alice"],"communities":["garden river"],"schools":["shingwauk"],"search_tokens":["alice","shingwauk","drowned","crossing","river","garden"]}`, "alice shingwauk drowned crossing river garden", "alice", "garden river", "shingwauk", "ready", ts))

	oldGen := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected contents with prompt text, got: %#v", contents)
		}
		prompt := contents[0].Parts[0].Text
		var out genai.GenerateContentResponse
		if strings.Contains(prompt, "VERIFIED RESULT (only source of truth):") {
			_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"Yes. I found 1 recorded death connected to drowning at Shingwauk."}]}}]}`), &out)
			return &out, nil
		}
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"intent\":\"count_rows\",\"search_terms\":[\"drowning\"],\"filters\":[{\"field_id\":\"school\",\"op\":\"eq\",\"value\":\"Shingwauk\"}],\"limit\":5}"}]}}]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = oldGen })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	cc := NewChatController(cs)

	r := gin.New()
	r.POST("/chat", cc.Chat)

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("question", "Did any children die from drowning at Shingwauk?")
	_ = w.WriteField("filename", "sheet.xlsx")
	_ = w.Close()

	req := httptest.NewRequest(http.MethodPost, "/chat", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	res := httptest.NewRecorder()

	r.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"answer":"Yes. I found 1 recorded death connected to drowning at Shingwauk."`) {
		t.Fatalf("unexpected body: %s", res.Body.String())
	}
}

func TestChatController_Describe_BadID_400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cc := NewChatController(&ChatService{})
	r := gin.New()
	r.GET("/describe/:id", cc.Describe)

	req := httptest.NewRequest(http.MethodGet, "/describe/not-a-number", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.Code)
	}
}

func TestChatController_TTS_MissingText_400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cc := NewChatController(&ChatService{})
	r := gin.New()
	r.POST("/tts", cc.TTS)

	req := httptest.NewRequest(http.MethodPost, "/tts", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", res.Code, res.Body.String())
	}
}
