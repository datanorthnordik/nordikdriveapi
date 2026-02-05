// internal/middlewares/auth_middleware_test.go
package middlewares

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// -------------------------
// helpers
// -------------------------

func setJWTSecretEnv(t *testing.T, secret string) {
	t.Helper()

	// Your config.LoadConfig() likely reads env; set the common ones.
	// Add/adjust if your config uses a different env var name.
	_ = os.Setenv("JWT_SECRET", secret)
	_ = os.Setenv("JWTSECRET", secret)
	_ = os.Setenv("JWT_SECRET_KEY", secret)
	_ = os.Setenv("JWTKEY", secret)
	_ = os.Setenv("ACCESS_TOKEN_SECRET", secret)

	t.Cleanup(func() {
		_ = os.Unsetenv("JWT_SECRET")
		_ = os.Unsetenv("JWTSECRET")
		_ = os.Unsetenv("JWT_SECRET_KEY")
		_ = os.Unsetenv("JWTKEY")
		_ = os.Unsetenv("ACCESS_TOKEN_SECRET")
	})
}

func newTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(AuthMiddleware())
	r.GET("/ok", func(c *gin.Context) {
		uid, _ := c.Get("userID")
		comms, _ := c.Get("communities")
		c.JSON(200, gin.H{
			"userID":       uid,
			"communities":  comms,
			"reached_next": true,
		})
	})
	return r
}

func signHS256(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}
	return s
}

// Creates a JWT string with payload that is NOT a MapClaims (so token.Claims.(jwt.MapClaims) would panic),
// but we avoid panics by ensuring AuthMiddleware rejects it first (e.g., invalid signature).
func makeNonMapClaimsTokenString(t *testing.T) string {
	t.Helper()
	// Build a 3-part token with HS256 header and arbitrary payload, but no valid signature.
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"user_id":1}`))
	return header + "." + payload + ".invalidsig"
}

func doReq(r *gin.Engine, token string, setCookie bool) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	if setCookie {
		req.AddCookie(&http.Cookie{Name: "access_token", Value: token})
	}
	r.ServeHTTP(w, req)
	return w
}

// -------------------------
// tests
// -------------------------

func TestAuthMiddleware_MissingCookie_401(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	w := doReq(r, "", false)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Missing access token") {
		t.Fatalf("expected Missing access token, got %s", w.Body.String())
	}
}

func TestAuthMiddleware_InvalidToken_401(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	// invalid token string -> jwt.Parse will error
	w := doReq(r, "not-a-jwt", true)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Invalid or expired token") {
		t.Fatalf("expected Invalid or expired token, got %s", w.Body.String())
	}
}

func TestAuthMiddleware_WrongSecret_401(t *testing.T) {
	setJWTSecretEnv(t, "server-secret")
	r := newTestRouter()

	// sign with different secret so signature verification fails
	token := signHS256(t, "other-secret", jwt.MapClaims{
		"user_id": 1,
		"exp":     time.Now().Add(time.Hour).Unix(),
	})

	w := doReq(r, token, true)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Invalid or expired token") {
		t.Fatalf("expected Invalid or expired token, got %s", w.Body.String())
	}
}

func TestAuthMiddleware_ExpiredToken_401(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	token := signHS256(t, "test-secret", jwt.MapClaims{
		"user_id": 1,
		"exp":     time.Now().Add(-time.Hour).Unix(),
	})

	w := doReq(r, token, true)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Invalid or expired token") {
		t.Fatalf("expected Invalid or expired token, got %s", w.Body.String())
	}
}

func TestAuthMiddleware_UserID_Float64_OK_CommunitiesParsed(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	token := signHS256(t, "test-secret", jwt.MapClaims{
		"user_id":     float64(42),
		"communities": []any{"c1", "", "c2", 123}, // only non-empty strings should remain
		"exp":         time.Now().Add(time.Hour).Unix(),
	})

	w := doReq(r, token, true)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}

	if body["reached_next"] != true {
		t.Fatalf("expected reached_next true, got %#v", body["reached_next"])
	}

	// userID is stored as float64 in gin context; JSON also returns float64
	if body["userID"] != float64(42) {
		t.Fatalf("expected userID 42, got %#v", body["userID"])
	}

	// communities should be ["c1","c2"]
	comms, ok := body["communities"].([]any)
	if !ok {
		t.Fatalf("expected communities array, got %#v", body["communities"])
	}
	if len(comms) != 2 || comms[0] != "c1" || comms[1] != "c2" {
		t.Fatalf("unexpected communities: %#v", comms)
	}
}

func TestAuthMiddleware_UserID_String_OK(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	token := signHS256(t, "test-secret", jwt.MapClaims{
		"user_id": "123",
		"exp":     time.Now().Add(time.Hour).Unix(),
	})

	w := doReq(r, token, true)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)

	if body["userID"] != float64(123) {
		t.Fatalf("expected userID 123, got %#v", body["userID"])
	}
}

func TestAuthMiddleware_UserID_String_ParseFail_401(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	token := signHS256(t, "test-secret", jwt.MapClaims{
		"user_id": "abc",
		"exp":     time.Now().Add(time.Hour).Unix(),
	})

	w := doReq(r, token, true)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid user ID") {
		t.Fatalf("expected invalid user ID, got %s", w.Body.String())
	}
}

func TestAuthMiddleware_UserID_UnsupportedType_401(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	token := signHS256(t, "test-secret", jwt.MapClaims{
		"user_id": []any{1}, // unsupported type
		"exp":     time.Now().Add(time.Hour).Unix(),
	})

	w := doReq(r, token, true)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid user ID") {
		t.Fatalf("expected invalid user ID, got %s", w.Body.String())
	}
}

func TestAuthMiddleware_Communities_MissingOrWrongType_DefaultsEmpty(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	// 1) communities missing => []
	token1 := signHS256(t, "test-secret", jwt.MapClaims{
		"user_id": 1,
		"exp":     time.Now().Add(time.Hour).Unix(),
	})
	w1 := doReq(r, token1, true)
	if w1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w1.Code, w1.Body.String())
	}
	var body1 map[string]any
	_ = json.Unmarshal(w1.Body.Bytes(), &body1)
	if comms, ok := body1["communities"].([]any); !ok || len(comms) != 0 {
		t.Fatalf("expected empty communities, got %#v", body1["communities"])
	}

	// 2) communities wrong type => []
	token2 := signHS256(t, "test-secret", jwt.MapClaims{
		"user_id":     1,
		"communities": "c1,c2",
		"exp":         time.Now().Add(time.Hour).Unix(),
	})
	w2 := doReq(r, token2, true)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w2.Code, w2.Body.String())
	}
	var body2 map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &body2)
	if comms, ok := body2["communities"].([]any); !ok || len(comms) != 0 {
		t.Fatalf("expected empty communities, got %#v", body2["communities"])
	}
}

func TestAuthMiddleware_MalformedButJWTLikeString_401(t *testing.T) {
	setJWTSecretEnv(t, "test-secret")
	r := newTestRouter()

	// This token looks like a JWT but has invalid signature; Parse should fail => 401
	token := makeNonMapClaimsTokenString(t)

	w := doReq(r, token, true)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Invalid or expired token") {
		t.Fatalf("expected Invalid or expired token, got %s", w.Body.String())
	}
}
