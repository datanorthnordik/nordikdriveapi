package logocontent

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterRoutes_AddsRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	RegisterRoutes(r, &mockLogoContentService{})

	req := httptest.NewRequest(http.MethodGet, "/api/logo-content/1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected auth middleware to protect route, got %d", w.Code)
	}
}
