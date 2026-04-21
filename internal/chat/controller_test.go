package chat

import (
	"bytes"
	"context"
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

	expectChatFileLookup(mock, "sheet.xlsx")
	expectChatRowsLookup(mock, `{"NAME":"Alice","First Nation/Community":"Garden River"}`)

	oldCreate := genaiCreateCachedContentHook
	oldGen := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiCreateCachedContentHook = oldCreate
		genaiGenerateContentHook = oldGen
	})

	genaiCreateCachedContentHook = func(_ *genai.Client, _ context.Context, _ string, _ *genai.CreateCachedContentConfig) (*genai.CachedContent, error) {
		return &genai.CachedContent{
			Name:       "projects/demo/locations/global/cachedContents/cache-1",
			ExpireTime: time.Now().Add(time.Hour),
		}, nil
	}
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		return jsonAnswer("Yes. I found 1 recorded death connected to drowning at Shingwauk.", nil, false, ""), nil
	}

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
