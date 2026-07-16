package honour

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type mockService struct {
	resp     *TodayResponse
	err      error
	runErr   error
	runCalls int
}

func (m *mockService) GetTodayByFilename(filename string) (*TodayResponse, error) {
	return m.resp, m.err
}

func (m *mockService) RunDailyHonours() error {
	m.runCalls++
	return m.runErr
}

func setupRouter(svc ServiceAPI) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/file/honour", (&Controller{Service: svc}).GetToday)
	return r
}

func TestControllerGetTodayMissingFilename(t *testing.T) {
	r := setupRouter(&mockService{})
	req := httptest.NewRequest(http.MethodGet, "/api/file/honour", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d", w.Code)
	}
}

func TestControllerGetTodayNotFound(t *testing.T) {
	r := setupRouter(&mockService{err: gorm.ErrRecordNotFound})
	req := httptest.NewRequest(http.MethodGet, "/api/file/honour?filename=survivors.csv", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 got %d", w.Code)
	}
}

func TestControllerGetTodayInternalError(t *testing.T) {
	r := setupRouter(&mockService{err: errors.New("boom")})
	req := httptest.NewRequest(http.MethodGet, "/api/file/honour?filename=survivors.csv", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 got %d", w.Code)
	}
}

func TestControllerGetTodaySuccess(t *testing.T) {
	r := setupRouter(&mockService{resp: &TodayResponse{
		Available:   true,
		FileID:      7,
		FileName:    "survivors.csv",
		FileVersion: 2,
		SourceRowID: 14,
		CycleNumber: 1,
		Date:        "2026-07-14",
		HonourText:  "Honour text",
	}})
	req := httptest.NewRequest(http.MethodGet, "/api/file/honour?filename=survivors.csv", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body=%s", w.Code, w.Body.String())
	}

	var body TodayResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !body.Available || body.SourceRowID != 14 || body.HonourText != "Honour text" {
		t.Fatalf("unexpected body: %#v", body)
	}
}

func TestControllerRunDailySuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &mockService{}
	r := gin.New()
	r.POST("/api/internal/jobs/honour/run", (&Controller{Service: svc}).RunDaily)

	req := httptest.NewRequest(http.MethodPost, "/api/internal/jobs/honour/run", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body=%s", w.Code, w.Body.String())
	}
	if svc.runCalls != 1 {
		t.Fatalf("expected RunDailyHonours once, got %d", svc.runCalls)
	}
}

func TestControllerRunDailyFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &mockService{runErr: errors.New("boom")}
	r := gin.New()
	r.POST("/api/internal/jobs/honour/run", (&Controller{Service: svc}).RunDaily)

	req := httptest.NewRequest(http.MethodPost, "/api/internal/jobs/honour/run", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 got %d body=%s", w.Code, w.Body.String())
	}
}
