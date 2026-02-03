package auth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"nordik-drive-api/internal/logs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
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
	r := setupAuthRouter(ac)

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
	r := setupAuthRouter(ac)

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
	r := setupAuthRouter(ac)

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
	r := setupAuthRouter(ac)

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
	r := setupAuthRouter(ac)

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
	r := setupAuthRouter(ac)

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

func TestSignUp_BadRequest_InvalidJSON(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			CreateUserFn: func(user Auth) (*Auth, error) { return &user, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/signup", []byte(`{"firstname":`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSignUp_BadRequest_ValidationError(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			CreateUserFn: func(user Auth) (*Auth, error) { return &user, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/signup", []byte(`{"firstname":"A","lastname":"B","email":"not-an-email","password":"123456"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSignUp_InternalServerError_HashFails(t *testing.T) {
	prev := hash
	hash = func(_ string) (string, error) { return "", assertErr("hash failed") }
	t.Cleanup(func() { hash = prev })

	ac := &AuthController{
		AuthService: &mockAuthService{
			CreateUserFn: func(user Auth) (*Auth, error) { return &user, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/signup", []byte(`{"firstname":"A","lastname":"B","email":"a@b.com","password":"123456"}`))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSignUp_InternalServerError_CreateUserFails(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			CreateUserFn: func(user Auth) (*Auth, error) { return nil, assertErr("db failed") },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/signup", []byte(`{"firstname":"A","lastname":"B","email":"a@b.com","password":"123456"}`))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSignUp_Created_NoCommunity_LogOK(t *testing.T) {
	var logCalled bool

	ac := &AuthController{
		AuthService: &mockAuthService{
			CreateUserFn: func(user Auth) (*Auth, error) {
				if user.FirstName != "A" || user.LastName != "B" || user.Email != "a@b.com" {
					t.Fatalf("unexpected user: %+v", user)
				}
				if user.Password == "" || user.Password == "123456" {
					t.Fatalf("password not hashed")
				}
				if len(user.Community) != 0 {
					t.Fatalf("expected empty community, got: %#v", user.Community)
				}
				user.ID = 10
				return &user, nil
			},
		},
		LS: &mockLogService{
			LogFn: func(entry logs.SystemLog, payload any) error {
				logCalled = true
				if entry.Action != "SIGNUP" || entry.Service != "auth" {
					t.Fatalf("unexpected log entry: %+v", entry)
				}
				return nil
			},
		},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/signup", []byte(`{"firstname":"A","lastname":"B","email":"a@b.com","password":"123456"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if !logCalled {
		t.Fatalf("expected log to be called")
	}

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v body=%s", err, w.Body.String())
	}
	if out["message"] != "User created successfully" {
		t.Fatalf("unexpected message: %v", out["message"])
	}
}

func TestSignUp_Created_WithCommunity_LogFails(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			CreateUserFn: func(user Auth) (*Auth, error) {
				if user.Email != "c@d.com" {
					t.Fatalf("unexpected email: %s", user.Email)
				}
				if user.Password == "" || user.Password == "123456" {
					t.Fatalf("password not hashed")
				}
				if len(user.Community) != 2 || user.Community[0] != "c1" || user.Community[1] != "c2" {
					t.Fatalf("unexpected community: %#v", user.Community)
				}
				user.ID = 11
				return &user, nil
			},
		},
		LS: &mockLogService{
			LogFn: func(_ logs.SystemLog, _ any) error { return assertErr("log failed") },
		},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/signup", []byte(`{"firstname":"C","lastname":"D","email":"c@d.com","password":"123456","community":["c1","c2"]}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v", err)
	}

	userObj, ok := out["user"].(map[string]any)
	if !ok {
		t.Fatalf("missing user object in response")
	}
	if userObj["email"] != "c@d.com" {
		t.Fatalf("unexpected response email: %v", userObj["email"])
	}
	_, _ = pq.StringArray{}, userObj["community"]
}

func TestLogout_OK_ClearsCookies_AndReturnsMessage(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReq(r, http.MethodPost, "/logout")
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
	requireContains(t, accessHeader, "Max-Age=0")

	requireContains(t, refreshHeader, "HttpOnly")
	requireContains(t, refreshHeader, "Secure")
	requireContains(t, refreshHeader, "SameSite=None")
	requireContains(t, refreshHeader, "Max-Age=0")

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if out["message"] != "Logged out" {
		t.Fatalf("unexpected message: %v", out["message"])
	}
}

func TestMe_Unauthorized_MissingAccessToken(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret")
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReq(r, http.MethodGet, "/me")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Missing access token")
}

func TestMe_Unauthorized_InvalidOrExpiredToken(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret")
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReq(r, http.MethodGet, "/me", &http.Cookie{Name: "access_token", Value: "not-a-jwt"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Invalid or expired token")
}

func TestMe_Unauthorized_UserNotFound(t *testing.T) {
	secret := "test-secret"
	os.Setenv("JWT_SECRET", secret)
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserByIDFn: func(id int) (*Auth, error) { return nil, assertErr("not found") },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	access := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": 7,
		"exp":     time.Now().Add(10 * time.Minute).Unix(),
	})
	accessStr, err := access.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	w := doReq(r, http.MethodGet, "/me", &http.Cookie{Name: "access_token", Value: accessStr})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "User not found")
}

func TestMe_OK_ReturnsUser(t *testing.T) {
	secret := "test-secret"
	os.Setenv("JWT_SECRET", secret)
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	u := &Auth{
		ID:        7,
		FirstName: "Athul",
		LastName:  "N",
		Email:     "athul@test.com",
		Role:      "User",
		Community: pq.StringArray{"c1", "c2"},
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserByIDFn: func(id int) (*Auth, error) {
				if id != 7 {
					t.Fatalf("expected id=7, got %d", id)
				}
				return u, nil
			},
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	access := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": 7,
		"exp":     time.Now().Add(10 * time.Minute).Unix(),
	})
	accessStr, err := access.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	w := doReq(r, http.MethodGet, "/me", &http.Cookie{Name: "access_token", Value: accessStr})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v", err)
	}

	userObj, ok := out["user"].(map[string]any)
	if !ok {
		t.Fatalf("missing user object: %s", w.Body.String())
	}
	if userObj["email"] != "athul@test.com" {
		t.Fatalf("unexpected email: %v", userObj["email"])
	}
}

func TestRefresh_Unauthorized_MissingRefreshToken(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret")
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReq(r, http.MethodPost, "/refresh")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Missing refresh token")
}

func TestRefresh_Unauthorized_InvalidRefreshToken(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret")
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReq(r, http.MethodPost, "/refresh", &http.Cookie{Name: "refresh_token", Value: "not-a-jwt"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Invalid refresh token")
}

func TestRefresh_OK_WithCommunitiesArray(t *testing.T) {
	secret := "test-secret"
	os.Setenv("JWT_SECRET", secret)
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	refresh := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id":     9,
		"communities": []any{"c1", "c2"},
		"exp":         time.Now().Add(10 * time.Minute).Unix(),
	})
	refreshStr, err := refresh.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign refresh: %v", err)
	}

	start := time.Now()
	w := doReq(r, http.MethodPost, "/refresh", &http.Cookie{Name: "refresh_token", Value: refreshStr})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	resp := w.Result()
	accessHeader, ok := cookieHeader(resp, "access_token")
	if !ok {
		t.Fatalf("missing access_token header")
	}
	requireContains(t, accessHeader, "HttpOnly")
	requireContains(t, accessHeader, "Secure")
	requireContains(t, accessHeader, "SameSite=None")

	accessToken := cookieValue(resp, "access_token")
	if accessToken == "" {
		t.Fatalf("missing access_token value")
	}

	claims := decodeJWTPayload(t, accessToken)

	if toInt64(claims["user_id"]) != 9 {
		t.Fatalf("unexpected user_id: %v", claims["user_id"])
	}

	raw := claims["communities"]
	arr, ok := raw.([]any)
	if !ok || len(arr) != 2 || arr[0] != "c1" || arr[1] != "c2" {
		t.Fatalf("unexpected communities: %#v", raw)
	}

	exp := time.Unix(toInt64(claims["exp"]), 0)
	if exp.Before(start.Add(14*time.Minute)) || exp.After(start.Add(16*time.Minute)) {
		t.Fatalf("access exp out of range: %v start=%v", exp, start)
	}
}

func TestRefresh_OK_CommunitiesArray_WithNonStringElement(t *testing.T) {
	secret := "test-secret"
	os.Setenv("JWT_SECRET", secret)
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	refresh := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id":     9,
		"communities": []any{"c1", 123},
		"exp":         time.Now().Add(10 * time.Minute).Unix(),
	})
	refreshStr, err := refresh.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign refresh: %v", err)
	}

	w := doReq(r, http.MethodPost, "/refresh", &http.Cookie{Name: "refresh_token", Value: refreshStr})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	resp := w.Result()
	accessToken := cookieValue(resp, "access_token")
	if accessToken == "" {
		t.Fatalf("missing access_token value")
	}

	claims := decodeJWTPayload(t, accessToken)
	raw := claims["communities"]
	arr, ok := raw.([]any)

	if !ok || len(arr) != 1 || arr[0] != "c1" {
		t.Fatalf("expected only [c1], got: %#v", raw)
	}
}

func TestRefresh_OK_CommunitiesNonArray_ResultsNilClaim(t *testing.T) {
	secret := "test-secret"
	os.Setenv("JWT_SECRET", secret)
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	refresh := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id":     9,
		"communities": "c1",
		"exp":         time.Now().Add(10 * time.Minute).Unix(),
	})
	refreshStr, err := refresh.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign refresh: %v", err)
	}

	w := doReq(r, http.MethodPost, "/refresh", &http.Cookie{Name: "refresh_token", Value: refreshStr})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	resp := w.Result()
	accessToken := cookieValue(resp, "access_token")
	if accessToken == "" {
		t.Fatalf("missing access_token value")
	}

	claims := decodeJWTPayload(t, accessToken)
	if _, exists := claims["communities"]; !exists || claims["communities"] != nil {
		t.Fatalf("expected communities=nil, got: %#v", claims["communities"])
	}
}

func TestRefresh_OK_CommunitiesMissing_ResultsNilClaim(t *testing.T) {
	secret := "test-secret"
	os.Setenv("JWT_SECRET", secret)
	t.Cleanup(func() { _ = os.Unsetenv("JWT_SECRET") })

	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	refresh := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": 9,
		"exp":     time.Now().Add(10 * time.Minute).Unix(),
	})
	refreshStr, err := refresh.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign refresh: %v", err)
	}

	w := doReq(r, http.MethodPost, "/refresh", &http.Cookie{Name: "refresh_token", Value: refreshStr})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	resp := w.Result()
	accessToken := cookieValue(resp, "access_token")
	if accessToken == "" {
		t.Fatalf("missing access_token value")
	}

	claims := decodeJWTPayload(t, accessToken)
	if _, exists := claims["communities"]; !exists || claims["communities"] != nil {
		t.Fatalf("expected communities=nil, got: %#v", claims["communities"])
	}
}

func TestGetUsers_Unauthorized_UserIDNotFound(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetAllUsersFn: func() ([]Auth, error) { return nil, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReqWithHeader(r, http.MethodGet, "/users", "", "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "user ID not found")
}

func TestGetUsers_Unauthorized_InvalidUserID(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetAllUsersFn: func() ([]Auth, error) { return nil, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReqWithHeader(r, http.MethodGet, "/users", "X-UserID", "abc")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "invalid user ID")
}

func TestGetUsers_InternalServerError_ServiceFails(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetAllUsersFn: func() ([]Auth, error) { return nil, assertErr("db failed") },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReqWithHeader(r, http.MethodGet, "/users", "X-UserID", "1")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "db failed")
}

func TestGetUsers_OK_ReturnsUsers(t *testing.T) {
	users := []Auth{
		{ID: 1, FirstName: "A", LastName: "B", Email: "a@b.com", Role: "User"},
		{ID: 2, FirstName: "C", LastName: "D", Email: "c@d.com", Role: "User"},
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetAllUsersFn: func() ([]Auth, error) { return users, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := doReqWithHeader(r, http.MethodGet, "/users", "X-UserID", "1")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if out["message"] != "Fetched all users successfully" {
		t.Fatalf("unexpected message: %v", out["message"])
	}

	arr, ok := out["users"].([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("unexpected users: %#v", out["users"])
	}
}

func TestVerifyPassword_BadRequest_InvalidJSON(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/verify-password", []byte(`{"password":`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestVerifyPassword_Unauthorized_UserIDNotFound(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSONWithHeader(r, "/verify-password", []byte(`{"password":"x"}`), "", "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "user ID not found")
}

func TestVerifyPassword_Unauthorized_InvalidUserID(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{},
		LS:          &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSONWithHeader(r, "/verify-password", []byte(`{"password":"x"}`), "X-UserID", "abc")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "invalid user ID")
}

func TestVerifyPassword_Unauthorized_UserNotFound(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserByIDFn: func(id int) (*Auth, error) { return nil, assertErr("not found") },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSONWithHeader(r, "/verify-password", []byte(`{"password":"x"}`), "X-UserID", "1")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Invalid credentials")
}

func TestVerifyPassword_BadRequest_WrongPassword(t *testing.T) {
	u := &Auth{
		ID:       1,
		Email:    "u@b.com",
		Password: hashPassword(t, "correct"),
		Role:     "User",
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserByIDFn: func(id int) (*Auth, error) { return u, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSONWithHeader(r, "/verify-password", []byte(`{"password":"wrong"}`), "X-UserID", "1")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "Invalid credentials")
}

func TestVerifyPassword_OK_LogSuccess(t *testing.T) {
	u := &Auth{
		ID:       1,
		Email:    "u@b.com",
		Password: hashPassword(t, "correct"),
		Role:     "User",
	}

	var logCalled bool
	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserByIDFn: func(id int) (*Auth, error) { return u, nil },
		},
		LS: &mockLogService{
			LogFn: func(entry logs.SystemLog, payload any) error {
				logCalled = true
				if entry.Action != "PASSWORD_VERIFICATION" || entry.Service != "auth" {
					t.Fatalf("unexpected log entry: %+v", entry)
				}
				return nil
			},
		},
	}
	r := setupAuthRouter(ac)

	w := postJSONWithHeader(r, "/verify-password", []byte(`{"password":"correct"}`), "X-UserID", "1")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !logCalled {
		t.Fatalf("expected log to be called")
	}

	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if out["message"] != "Password verified successfully" {
		t.Fatalf("unexpected message: %v", out["message"])
	}
	data, ok := out["data"].(map[string]any)
	if !ok || data["match"] != true {
		t.Fatalf("unexpected data: %#v", out["data"])
	}
}

func TestVerifyPassword_OK_LogFailsStillOK(t *testing.T) {
	u := &Auth{
		ID:       1,
		Email:    "u@b.com",
		Password: hashPassword(t, "correct"),
		Role:     "User",
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			GetUserByIDFn: func(id int) (*Auth, error) { return u, nil },
		},
		LS: &mockLogService{
			LogFn: func(_ logs.SystemLog, _ any) error { return assertErr("log failed") },
		},
	}
	r := setupAuthRouter(ac)

	w := postJSONWithHeader(r, "/verify-password", []byte(`{"password":"correct"}`), "X-UserID", "1")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSendOTP_BadRequest_InvalidJSON(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			SendOTPFn: func(email string) (*Auth, string, error) { return &Auth{}, "123456", nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/send-otp", []byte(`{"email":`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSendOTP_BadRequest_ValidationError(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			SendOTPFn: func(email string) (*Auth, string, error) { return &Auth{}, "123456", nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/send-otp", []byte(`{"email":"not-an-email"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSendOTP_BadRequest_ServiceReturnsError(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			SendOTPFn: func(email string) (*Auth, string, error) {
				if email != "a@b.com" {
					t.Fatalf("unexpected email: %s", email)
				}
				return nil, "", assertErr("user not found")
			},
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/send-otp", []byte(`{"email":"a@b.com"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "user not found")
}

func TestSendOTP_BadRequest_UserNil(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			SendOTPFn: func(email string) (*Auth, string, error) { return nil, "123456", nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/send-otp", []byte(`{"email":"a@b.com"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "user not found")
}

func TestSendOTP_OK_LogSuccess(t *testing.T) {
	var logCalled bool

	u := &Auth{
		ID:        5,
		Email:     "ok@b.com",
		Community: pq.StringArray{"c1"},
	}

	ac := &AuthController{
		AuthService: &mockAuthService{
			SendOTPFn: func(email string) (*Auth, string, error) {
				if email != "ok@b.com" {
					t.Fatalf("unexpected email: %s", email)
				}
				return u, "123456", nil
			},
		},
		LS: &mockLogService{
			LogFn: func(entry logs.SystemLog, payload any) error {
				logCalled = true
				if entry.Action != "SEND_OTP" || entry.Service != "auth" {
					t.Fatalf("unexpected log: %+v", entry)
				}
				return nil
			},
		},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/send-otp", []byte(`{"email":"ok@b.com"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !logCalled {
		t.Fatalf("expected log to be called")
	}
	requireContains(t, w.Body.String(), "OTP sent successfully")
}

func TestSendOTP_OK_LogFailsStillOK(t *testing.T) {
	u := &Auth{ID: 6, Email: "ok2@b.com"}

	ac := &AuthController{
		AuthService: &mockAuthService{
			SendOTPFn: func(email string) (*Auth, string, error) { return u, "123456", nil },
		},
		LS: &mockLogService{
			LogFn: func(_ logs.SystemLog, _ any) error { return assertErr("log failed") },
		},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/send-otp", []byte(`{"email":"ok2@b.com"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestResetPassword_BadRequest_InvalidJSON(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			ResetPasswordFn: func(email, code, newPassword string) (*Auth, error) { return &Auth{}, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/reset-password", []byte(`{"email":`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestResetPassword_BadRequest_ValidationError(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			ResetPasswordFn: func(email, code, newPassword string) (*Auth, error) { return &Auth{}, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	// OTP required + password min=6 enforced by binding tags
	w := postJSON(r, "/reset-password", []byte(`{"email":"a@b.com","otp":"","password":"123"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestResetPassword_BadRequest_ServiceReturnsError(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			ResetPasswordFn: func(email, code, newPassword string) (*Auth, error) {
				if email != "a@b.com" || code != "111111" || newPassword != "123456" {
					t.Fatalf("unexpected args: %s %s %s", email, code, newPassword)
				}
				return nil, assertErr("invalid OTP")
			},
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/reset-password", []byte(`{"email":"a@b.com","otp":"111111","password":"123456"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "invalid OTP")
}

func TestResetPassword_BadRequest_UserNil(t *testing.T) {
	ac := &AuthController{
		AuthService: &mockAuthService{
			ResetPasswordFn: func(email, code, newPassword string) (*Auth, error) { return nil, nil },
		},
		LS: &mockLogService{},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/reset-password", []byte(`{"email":"a@b.com","otp":"111111","password":"123456"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	requireContains(t, w.Body.String(), "user not found")
}

func TestResetPassword_OK_LogSuccess(t *testing.T) {
	var logCalled bool

	u := &Auth{ID: 9, Email: "a@b.com", Community: pq.StringArray{"c1"}}

	ac := &AuthController{
		AuthService: &mockAuthService{
			ResetPasswordFn: func(email, code, newPassword string) (*Auth, error) {
				return u, nil
			},
		},
		LS: &mockLogService{
			LogFn: func(entry logs.SystemLog, payload any) error {
				logCalled = true
				if entry.Action != "RESET_PASSWORD" || entry.Service != "auth" {
					t.Fatalf("unexpected log: %+v", entry)
				}
				return nil
			},
		},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/reset-password", []byte(`{"email":"a@b.com","otp":"111111","password":"123456"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !logCalled {
		t.Fatalf("expected log to be called")
	}
	requireContains(t, w.Body.String(), "Password reset successfully")
}

func TestResetPassword_OK_LogFailsStillOK(t *testing.T) {
	u := &Auth{ID: 10, Email: "a@b.com"}

	ac := &AuthController{
		AuthService: &mockAuthService{
			ResetPasswordFn: func(email, code, newPassword string) (*Auth, error) { return u, nil },
		},
		LS: &mockLogService{
			LogFn: func(_ logs.SystemLog, _ any) error { return assertErr("log failed") },
		},
	}
	r := setupAuthRouter(ac)

	w := postJSON(r, "/reset-password", []byte(`{"email":"a@b.com","otp":"111111","password":"123456"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}
