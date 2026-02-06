package role

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"nordik-drive-api/internal/auth"
)

// --------------------
// Stub implementing your new interface RoleServiceAPI
// --------------------
type roleServiceStub struct {
	getRoleByUserFn    func(userid int) ([]auth.UserRole, error)
	getAllRolesFn      func(uniqueRoles []string) ([]Role, error)
	getRolesByUserIDFn func(userid int) ([]auth.UserRole, error)
}

func (s *roleServiceStub) GetRoleByUser(userid int) ([]auth.UserRole, error) {
	return s.getRoleByUserFn(userid)
}
func (s *roleServiceStub) GetAllRoles(uniqueRoles []string) ([]Role, error) {
	return s.getAllRolesFn(uniqueRoles)
}
func (s *roleServiceStub) GetRolesByUserId(userid int) ([]auth.UserRole, error) {
	return s.getRolesByUserIDFn(userid)
}

// --------------------
// Helpers
// --------------------
func setupRouterWithUserID(handler gin.HandlerFunc, setUser any, set bool) (*gin.Engine, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.GET("/x", func(c *gin.Context) {
		if set {
			c.Set("userID", setUser)
		}
		handler(c)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)
	return r, w
}

func decodeJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("json unmarshal: %v body=%s", err, string(body))
	}
	return m
}

// --------------------
// Tests: GetAllRoles
// --------------------
func TestRoleController_GetAllRoles_401_UserIDMissing(t *testing.T) {
	rc := &RoleController{RoleService: &roleServiceStub{}}

	_, w := setupRouterWithUserID(rc.GetAllRoles, nil, false)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoleController_GetAllRoles_401_UserIDInvalidType(t *testing.T) {
	rc := &RoleController{RoleService: &roleServiceStub{}}

	_, w := setupRouterWithUserID(rc.GetAllRoles, "not-float64", true)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoleController_GetAllRoles_500_WhenGetRoleByUserFails(t *testing.T) {
	stub := &roleServiceStub{
		getRoleByUserFn: func(int) ([]auth.UserRole, error) {
			return nil, errors.New("db fail")
		},
		getAllRolesFn: func([]string) ([]Role, error) {
			t.Fatalf("GetAllRoles should not be called when GetRoleByUser fails")
			return nil, nil
		},
	}
	rc := &RoleController{RoleService: stub}

	_, w := setupRouterWithUserID(rc.GetAllRoles, float64(10), true)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoleController_GetAllRoles_500_WhenGetAllRolesFails(t *testing.T) {
	stub := &roleServiceStub{
		getRoleByUserFn: func(int) ([]auth.UserRole, error) {
			// include duplicates to ensure dedupe still happens before call
			return []auth.UserRole{
				{UserID: 10, Role: "Admin"},
				{UserID: 10, Role: "User"},
				{UserID: 10, Role: "User"},
			}, nil
		},
		getAllRolesFn: func(unique []string) ([]Role, error) {
			// Unique should contain Admin + User (order not guaranteed)
			if len(unique) != 2 {
				return nil, errors.New("expected 2 unique roles")
			}
			return nil, errors.New("roles query fail")
		},
	}
	rc := &RoleController{RoleService: stub}

	_, w := setupRouterWithUserID(rc.GetAllRoles, float64(10), true)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoleController_GetAllRoles_200_Success_DedupesRoles(t *testing.T) {
	var capturedUnique []string

	stub := &roleServiceStub{
		getRoleByUserFn: func(int) ([]auth.UserRole, error) {
			return []auth.UserRole{
				{UserID: 10, Role: "Admin"},
				{UserID: 10, Role: "User"},
				{UserID: 10, Role: "User"}, // duplicate
			}, nil
		},
		getAllRolesFn: func(unique []string) ([]Role, error) {
			capturedUnique = append([]string{}, unique...)
			return []Role{
				{ID: 3, Role: "Reviewer"},
			}, nil
		},
	}
	rc := &RoleController{RoleService: stub}

	_, w := setupRouterWithUserID(rc.GetAllRoles, float64(10), true)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}

	// verify dedupe happened (order not guaranteed)
	if len(capturedUnique) != 2 {
		t.Fatalf("expected 2 unique roles passed to service, got %#v", capturedUnique)
	}

	resp := decodeJSON(t, w.Body.Bytes())
	if resp["message"] != "Roles fetched successfully" {
		t.Fatalf("unexpected message: %#v", resp["message"])
	}

	rolesAny, ok := resp["roles"].([]any)
	if !ok || len(rolesAny) != 1 {
		t.Fatalf("expected roles array len=1, got %#v", resp["roles"])
	}
}

// --------------------
// Tests: GetRolesByUserId
// --------------------
func TestRoleController_GetRolesByUserId_401_UserIDMissing(t *testing.T) {
	rc := &RoleController{RoleService: &roleServiceStub{}}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/x", rc.GetRolesByUserId)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoleController_GetRolesByUserId_401_UserIDInvalidType(t *testing.T) {
	stub := &roleServiceStub{}
	rc := &RoleController{RoleService: stub}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/x", func(c *gin.Context) {
		c.Set("userID", "nope")
		rc.GetRolesByUserId(c)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoleController_GetRolesByUserId_500_WhenServiceFails(t *testing.T) {
	stub := &roleServiceStub{
		getRolesByUserIDFn: func(int) ([]auth.UserRole, error) {
			return nil, errors.New("db fail")
		},
	}
	rc := &RoleController{RoleService: stub}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/x", func(c *gin.Context) {
		c.Set("userID", float64(10))
		rc.GetRolesByUserId(c)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestRoleController_GetRolesByUserId_200_Success(t *testing.T) {
	stub := &roleServiceStub{
		getRolesByUserIDFn: func(int) ([]auth.UserRole, error) {
			return []auth.UserRole{
				{UserID: 10, Role: "Admin"},
				{UserID: 10, Role: "User"},
			}, nil
		},
	}
	rc := &RoleController{RoleService: stub}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/x", func(c *gin.Context) {
		c.Set("userID", float64(10))
		rc.GetRolesByUserId(c)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}

	resp := decodeJSON(t, w.Body.Bytes())
	if resp["message"] != "Roles fetched successfully" {
		t.Fatalf("unexpected message: %#v", resp["message"])
	}
	if _, ok := resp["roles"]; !ok {
		t.Fatalf("expected roles key present")
	}
}
