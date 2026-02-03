package auth

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func setupAuthRouter(ac *AuthController) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	r.Use(func(c *gin.Context) {
		if v := c.GetHeader("X-UserID"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				c.Set("userID", f)
			} else {
				c.Set("userID", v)
			}
		}
		c.Next()
	})

	r.POST("/login", ac.Login)
	r.POST("/signup", ac.SignUp)

	r.POST("/logout", ac.Logout)
	r.GET("/me", ac.Me)
	r.POST("/refresh", ac.Refresh)

	r.GET("/users", ac.GetUsers)
	r.POST("/verify-password", ac.VerifyPassword)

	r.POST("/send-otp", ac.SendOTP)
	r.POST("/reset-password", ac.ResetPassword)

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

func doReq(r http.Handler, method, path string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	r.ServeHTTP(w, req)
	return w
}

func postJSONWithHeader(r http.Handler, path string, body []byte, key, value string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(key, value)
	}
	r.ServeHTTP(w, req)
	return w
}

func doReqWithHeader(r http.Handler, method, path, key, value string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	if key != "" {
		req.Header.Set(key, value)
	}
	r.ServeHTTP(w, req)
	return w
}
