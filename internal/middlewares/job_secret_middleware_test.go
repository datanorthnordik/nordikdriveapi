package middlewares

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func newJobSecretRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(JobSecretMiddleware())
	r.POST("/internal", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return r
}

func TestJobSecretMiddlewareMissingConfig(t *testing.T) {
	_ = os.Unsetenv("HONOUR_JOB_SECRET")
	r := newJobSecretRouter()

	req := httptest.NewRequest(http.MethodPost, "/internal", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestJobSecretMiddlewareRejectsInvalidSecret(t *testing.T) {
	_ = os.Setenv("HONOUR_JOB_SECRET", "expected-secret")
	t.Cleanup(func() { _ = os.Unsetenv("HONOUR_JOB_SECRET") })
	r := newJobSecretRouter()

	req := httptest.NewRequest(http.MethodPost, "/internal", nil)
	req.Header.Set(HonourJobSecretHeader, "wrong-secret")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid honour job secret") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestJobSecretMiddlewareAllowsValidSecret(t *testing.T) {
	_ = os.Setenv("HONOUR_JOB_SECRET", "expected-secret")
	t.Cleanup(func() { _ = os.Unsetenv("HONOUR_JOB_SECRET") })
	r := newJobSecretRouter()

	req := httptest.NewRequest(http.MethodPost, "/internal", nil)
	req.Header.Set(HonourJobSecretHeader, "expected-secret")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
}
