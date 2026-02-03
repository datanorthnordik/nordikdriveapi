package auth

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"nordik-drive-api/internal/logs"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

type mockAuthService struct {
	CreateUserFn    func(user Auth) (*Auth, error)
	GetUserFn       func(email string) (*Auth, error)
	GetUserByIDFn   func(id int) (*Auth, error)
	GetAllUsersFn   func() ([]Auth, error)
	SendOTPFn       func(email string) (*Auth, string, error)
	ResetPasswordFn func(email, code, newPassword string) (*Auth, error)
}

func (m *mockAuthService) CreateUser(user Auth) (*Auth, error) {
	if m.CreateUserFn == nil {
		return nil, assertErr("CreateUser not implemented")
	}
	return m.CreateUserFn(user)
}

func (m *mockAuthService) GetUser(email string) (*Auth, error) {
	if m.GetUserFn == nil {
		return nil, assertErr("GetUser not implemented")
	}
	return m.GetUserFn(email)
}

func (m *mockAuthService) GetUserByID(id int) (*Auth, error) {
	if m.GetUserByIDFn == nil {
		return nil, assertErr("GetUserByID not implemented")
	}
	return m.GetUserByIDFn(id)
}

func (m *mockAuthService) GetAllUsers() ([]Auth, error) {
	if m.GetAllUsersFn == nil {
		return nil, assertErr("GetAllUsers not implemented")
	}
	return m.GetAllUsersFn()
}

func (m *mockAuthService) SendOTP(email string) (*Auth, string, error) {
	if m.SendOTPFn == nil {
		return nil, "", assertErr("SendOTP not implemented")
	}
	return m.SendOTPFn(email)
}

func (m *mockAuthService) ResetPassword(email, code, newPassword string) (*Auth, error) {
	if m.ResetPasswordFn == nil {
		return nil, assertErr("ResetPassword not implemented")
	}
	return m.ResetPasswordFn(email, code, newPassword)
}

type mockLogService struct {
	LogFn func(entry logs.SystemLog, payload any) error
}

func (m *mockLogService) Log(entry logs.SystemLog, payload any) error {
	if m.LogFn == nil {
		return nil
	}
	return m.LogFn(entry, payload)
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func setupLoginRouter(ac *AuthController) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/login", ac.Login)
	return r
}

func postJSON(r http.Handler, path string, body []byte) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func requireContains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected %q to contain %q", s, sub)
	}
}

func hashPassword(t *testing.T, plain string) string {
	t.Helper()
	b, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return string(b)
}

func cookieHeader(resp *http.Response, name string) (string, bool) {
	prefix := name + "="
	for _, h := range resp.Header.Values("Set-Cookie") {
		if strings.HasPrefix(h, prefix) {
			return h, true
		}
	}
	return "", false
}

func cookieValue(resp *http.Response, name string) string {
	for _, c := range resp.Cookies() {
		if c.Name == name {
			return c.Value
		}
	}
	return ""
}
