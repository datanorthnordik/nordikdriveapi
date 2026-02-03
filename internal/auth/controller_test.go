package auth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
)

func decodeJWTPayload(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("invalid jwt: %q", token)
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode jwt payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(b, &claims); err != nil {
		t.Fatalf("unmarshal jwt payload: %v", err)
	}
	return claims
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case json.Number:
		i, _ := x.Int64()
		return i
	default:
		return 0
	}
}

func TestLogin_BadRequest_InvalidJSON(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserFn: func(email string) (*Auth, error) { return nil, nil },
		},
		LS: &mockLogService{},
	}
	r := setupLoginRouter(ac)

	w := postJSON(r, "/login", []byte(`{"email":`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestLogin_BadRequest_ValidationError(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserFn: func(email string) (*Auth, error) { return nil, nil },
		},
		LS: &mockLogService{},
	}
	r := setupLoginRouter(ac)

	w := postJSON(r, "/login", []byte(`{"email":"not-an-email","password":"x","rememberMe":false}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestLogin_Unauthorized_UserNotFound(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserFn: func(email string) (*Auth, error) { return nil, assertErr("not found") },
		},
		LS: &mockLogService{},
	}
	r := setupLoginRouter(ac)

	w := postJSON(r, "/login", []byte(`{"email":"missing@test.com","password":"x","rememberMe":false}`))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Oops! We couldn’t log you in")
}

func TestLogin_Unauthorized_WrongPassword(t *testing.T) {
	u := &Auth{
		ID:        1,
		Email:     "user@test.com",
		Password:  hashPassword(t, "correct-password"),
		FirstName: "A",
		LastName:  "B",
		Role:      "User",
		Community: pq.StringArray{"c1"},
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserFn: func(email string) (*Auth, error) { return u, nil },
		},
		LS: &mockLogService{},
	}
	r := setupLoginRouter(ac)

	w := postJSON(r, "/login", []byte(`{"email":"user@test.com","password":"wrong","rememberMe":false}`))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Oops! We couldn’t log you in")
}

func TestLogin_OK_SetsCookies_AndJWTExp_RememberFalse(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret")

	u := &Auth{
		ID:        7,
		Email:     "ok@test.com",
		Password:  hashPassword(t, "correct-password"),
		FirstName: "Athul",
		LastName:  "N",
		Role:      "User",
		Community: pq.StringArray{"c1", "c2"},
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserFn: func(email string) (*Auth, error) { return u, nil },
		},
		LS: &mockLogService{},
	}
	r := setupLoginRouter(ac)

	start := time.Now()
	w := postJSON(r, "/login", []byte(`{"email":"ok@test.com","password":"correct-password","rememberMe":false}`))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	resp := w.Result()

	accessHeader, ok := cookieHeader(resp, "access_token")
	if !ok {
		t.Fatalf("missing access_token header")
	}
	refreshHeader, ok := cookieHeader(resp, "refresh_token")
	if !ok {
		t.Fatalf("missing refresh_token header")
	}

	requireContains(t, accessHeader, "HttpOnly")
	requireContains(t, accessHeader, "Secure")
	requireContains(t, accessHeader, "SameSite=None")

	requireContains(t, refreshHeader, "HttpOnly")
	requireContains(t, refreshHeader, "Secure")
	requireContains(t, refreshHeader, "SameSite=None")

	accessToken := cookieValue(resp, "access_token")
	refreshToken := cookieValue(resp, "refresh_token")
	if accessToken == "" || refreshToken == "" {
		t.Fatalf("missing cookie values: access=%q refresh=%q", accessToken, refreshToken)
	}

	accessClaims := decodeJWTPayload(t, accessToken)
	refreshClaims := decodeJWTPayload(t, refreshToken)

	if toInt64(accessClaims["user_id"]) != int64(u.ID) {
		t.Fatalf("access user_id mismatch: got=%v want=%v", accessClaims["user_id"], u.ID)
	}

	accessExp := time.Unix(toInt64(accessClaims["exp"]), 0)
	refreshExp := time.Unix(toInt64(refreshClaims["exp"]), 0)

	if accessExp.Before(start.Add(14*time.Minute)) || accessExp.After(start.Add(16*time.Minute)) {
		t.Fatalf("access exp out of range: %v start=%v", accessExp, start)
	}

	if refreshExp.Before(start.Add(23*time.Hour+55*time.Minute)) || refreshExp.After(start.Add(24*time.Hour+5*time.Minute)) {
		t.Fatalf("refresh exp out of range: %v start=%v", refreshExp, start)
	}
}

func TestLogin_OK_RememberMe_ExtendsRefreshExp(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret")

	u := &Auth{
		ID:        9,
		Email:     "remember@test.com",
		Password:  hashPassword(t, "correct-password"),
		FirstName: "X",
		LastName:  "Y",
		Role:      "User",
		Community: pq.StringArray{"c1"},
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserFn: func(email string) (*Auth, error) { return u, nil },
		},
		LS: &mockLogService{},
	}
	r := setupLoginRouter(ac)

	start := time.Now()
	w := postJSON(r, "/login", []byte(`{"email":"remember@test.com","password":"correct-password","rememberMe":true}`))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	resp := w.Result()
	refreshToken := cookieValue(resp, "refresh_token")
	if refreshToken == "" {
		t.Fatalf("missing refresh_token")
	}

	claims := decodeJWTPayload(t, refreshToken)
	refreshExp := time.Unix(toInt64(claims["exp"]), 0)

	min := start.Add(30*24*time.Hour - 10*time.Minute)
	max := start.Add(30*24*time.Hour + 10*time.Minute)
	if refreshExp.Before(min) || refreshExp.After(max) {
		t.Fatalf("refresh exp out of range: got=%v expected [%v, %v]", refreshExp, min, max)
	}
}
