package logocontent

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gorm.io/gorm"
)

type mockLogoContentService struct {
	data        []byte
	contentType string
	filename    string
	err         error
	receivedID  uint
}

func (m *mockLogoContentService) GetHTMLByFileID(fileID uint) ([]byte, string, string, error) {
	m.receivedID = fileID
	return m.data, m.contentType, m.filename, m.err
}

func TestLogoContentController_GetHTMLByFileID_Unauthorized(t *testing.T) {
	r := setupRouterForController(&mockLogoContentService{})

	req := httptest.NewRequest(http.MethodGet, "/api/logo-content/1", nil)
	w := doReq(r, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestLogoContentController_GetHTMLByFileID_InvalidID(t *testing.T) {
	r := setupRouterForController(&mockLogoContentService{})

	req := httptest.NewRequest(http.MethodGet, "/api/logo-content/abc", nil)
	req.Header.Set("Authorization", "Bearer test")
	w := doReq(r, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "valid file id is required") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestLogoContentController_GetHTMLByFileID_NotFound(t *testing.T) {
	svc := &mockLogoContentService{err: gorm.ErrRecordNotFound}
	r := setupRouterForController(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/logo-content/12", nil)
	req.Header.Set("Authorization", "Bearer test")
	w := doReq(r, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	if svc.receivedID != 12 {
		t.Fatalf("expected file id 12, got %d", svc.receivedID)
	}
}

func TestLogoContentController_GetHTMLByFileID_ServiceError(t *testing.T) {
	svc := &mockLogoContentService{err: errors.New("gcs failed")}
	r := setupRouterForController(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/logo-content/7", nil)
	req.Header.Set("Authorization", "Bearer test")
	w := doReq(r, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "gcs failed") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestLogoContentController_GetHTMLByFileID_Success(t *testing.T) {
	svc := &mockLogoContentService{
		data:        []byte("<html><body>Coroner</body></html>"),
		contentType: "text/html; charset=utf-8",
		filename:    `../evil"page.html`,
	}
	r := setupRouterForController(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/logo-content/9", nil)
	req.Header.Set("Authorization", "Bearer test")
	w := doReq(r, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if svc.receivedID != 9 {
		t.Fatalf("expected file id 9, got %d", svc.receivedID)
	}
	if body := w.Body.String(); body != "<html><body>Coroner</body></html>" {
		t.Fatalf("unexpected body: %s", body)
	}
	if got := w.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("expected html content type, got %q", got)
	}
	if got := w.Header().Get("Content-Disposition"); !strings.Contains(got, `inline; filename=".._evilpage.html"`) {
		t.Fatalf("unexpected content disposition: %q", got)
	}
	if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("expected nosniff, got %q", got)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected no-store, got %q", got)
	}
	if got := w.Header().Get("Content-Security-Policy"); !strings.Contains(got, "script-src 'none'") {
		t.Fatalf("unexpected csp: %q", got)
	}
}
