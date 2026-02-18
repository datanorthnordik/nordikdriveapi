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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"google.golang.org/genai"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newMockDBChat(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
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

	// Stub Gemini
	oldGen := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		// Inspect the actual prompt text (NOT json.Marshal(contents), which escapes quotes)
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected contents with prompt text, got: %#v", contents)
		}

		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, `"First Nation/Community":"B"`) {
			t.Fatalf("expected prompt to include filtered row for community B; got prompt:\n%s", prompt)
		}

		var out genai.GenerateContentResponse
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"OK"}]}}]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = oldGen })

	// Mock DB for file + file_data
	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}).
			AddRow(uint(1), "sheet.xlsx", 2))

	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*file_id.*and.*version`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(1), uint(1), 2, `{"First Nation/Community":"B","x":1}`))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	cc := NewChatController(cs)

	r := gin.New()
	r.POST("/chat", cc.Chat)

	// Multipart form
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("question", "hello")
	_ = w.WriteField("filename", "sheet.xlsx")
	// communities[]= "A,B" -> util.ParseCommaSeparatedCommunities should split to A and B
	_ = w.WriteField("communities", "A,B")
	_ = w.Close()

	req := httptest.NewRequest(http.MethodPost, "/chat", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	res := httptest.NewRecorder()

	r.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"answer":"OK"`) {
		t.Fatalf("unexpected body: %s", res.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatController_Chat_ServiceError500(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, mock, cleanup := newMockDBChat(t)
	defer cleanup()

	// file not found -> no rows
	mock.ExpectQuery(`(?i)select.*from.*file.*where.*filename.*order.*version.*desc.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "version"}))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	cc := NewChatController(cs)

	r := gin.New()
	r.POST("/chat", cc.Chat)

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("question", "hello")
	_ = w.WriteField("filename", "missing.xlsx")
	_ = w.Close()

	req := httptest.NewRequest(http.MethodPost, "/chat", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	res := httptest.NewRecorder()

	r.ServeHTTP(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "file not found") {
		t.Fatalf("expected file not found, got body=%s", res.Body.String())
	}
}

func TestChatController_Describe_MissingID_400(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cc := NewChatController(&ChatService{})
	r := gin.New()

	// route without :id, so Param("id")=="" and Query("id")=="" -> 400
	r.GET("/chat/describe", cc.Describe)

	req := httptest.NewRequest(http.MethodGet, "/chat/describe", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "valid id is required") {
		t.Fatalf("expected valid id error, got body=%s", res.Body.String())
	}
}

func TestChatController_Describe_InvalidID_400(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cc := NewChatController(&ChatService{})
	r := gin.New()
	r.GET("/chat/describe", cc.Describe)

	req := httptest.NewRequest(http.MethodGet, "/chat/describe?id=abc", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "valid id is required") {
		t.Fatalf("expected valid id error, got body=%s", res.Body.String())
	}
}

func TestChatController_Describe_Success200_PathParam(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, mock, cleanup := newMockDBChat(t)
	defer cleanup()

	// DB: file_data row lookup by id
	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(1, 1, 1, `{"Name":"John Doe","First Nation/Community":"B","Notes":"Test"}`))

	// Stub Gemini
	oldGen := genaiGenerateContentHook
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		if len(contents) == 0 || contents[0] == nil || len(contents[0].Parts) == 0 {
			t.Fatalf("expected contents with prompt text, got: %#v", contents)
		}
		prompt := contents[0].Parts[0].Text
		if !strings.Contains(prompt, "RECORD (only source of truth):") {
			t.Fatalf("expected RECORD section, got prompt:\n%s", prompt)
		}
		if !strings.Contains(prompt, `"Name":"John Doe"`) {
			t.Fatalf("expected prompt to include row JSON, got prompt:\n%s", prompt)
		}

		var out genai.GenerateContentResponse
		_ = json.Unmarshal([]byte(`{"candidates":[{"content":{"parts":[{"text":"DESC_OK"}]}}]}`), &out)
		return &out, nil
	}
	t.Cleanup(func() { genaiGenerateContentHook = oldGen })

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	cc := NewChatController(cs)

	r := gin.New()
	r.GET("/chat/describe/:id", cc.Describe)

	req := httptest.NewRequest(http.MethodGet, "/chat/describe/1", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"id":1`) || !strings.Contains(res.Body.String(), `"answer":"DESC_OK"`) {
		t.Fatalf("unexpected body: %s", res.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChatController_Describe_ServiceError500_RowNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, mock, cleanup := newMockDBChat(t)
	defer cleanup()

	// no rows => service returns "row not found"
	mock.ExpectQuery(`(?i)select.*from.*file_data.*where.*id.*limit`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}))

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	cc := NewChatController(cs)

	r := gin.New()
	r.GET("/chat/describe/:id", cc.Describe)

	req := httptest.NewRequest(http.MethodGet, "/chat/describe/99", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "row not found") {
		t.Fatalf("expected row not found, got body=%s", res.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

