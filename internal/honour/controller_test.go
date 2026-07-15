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
	resp *TodayResponse
	err  error
}

func (m *mockService) GetTodayByFilename(filename string) (*TodayResponse, error) {
	return m.resp, m.err
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
