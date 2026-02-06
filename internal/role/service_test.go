package role

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"nordik-drive-api/internal/auth"
)

// --------------------
// DB helper
// --------------------
func newRoleTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	// Make sure these exist:
	// - role.Role table
	// - auth.UserRole table
	if err := db.AutoMigrate(&Role{}, &auth.UserRole{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return db
}

func seedRoles(t *testing.T, db *gorm.DB) {
	t.Helper()

	roles := []Role{
		{Role: "Admin", Priority: 1, CanUpload: true, CanView: true, CanApprove: true, CanApproveAll: true},
		{Role: "User", Priority: 2, CanUpload: false, CanView: true, CanApprove: false, CanApproveAll: false},
		{Role: "Reviewer", Priority: 3, CanUpload: false, CanView: true, CanApprove: true, CanApproveAll: false},
	}
	if err := db.Create(&roles).Error; err != nil {
		t.Fatalf("seed roles: %v", err)
	}
}

func seedUserRoles(t *testing.T, db *gorm.DB, userID uint) {
	t.Helper()

	urs := []auth.UserRole{
		{UserID: int(userID), Role: "Admin"},
		{UserID: int(userID), Role: "User"},
		{UserID: int(userID), Role: "User"}, // duplicate to ensure controller dedupe works later
	}
	if err := db.Create(&urs).Error; err != nil {
		t.Fatalf("seed user_roles: %v", err)
	}
}

func TestRoleService_GetAllRoles_EmptyUnique_ReturnsEmpty(t *testing.T) {
	db := newRoleTestDB(t)
	svc := &RoleService{DB: db}

	out, err := svc.GetAllRoles(nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty, got %#v", out)
	}
}

func TestRoleService_GetAllRoles_FiltersNotIn(t *testing.T) {
	db := newRoleTestDB(t)
	seedRoles(t, db)
	svc := &RoleService{DB: db}

	out, err := svc.GetAllRoles([]string{"Admin", "User"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 1 || out[0].Role != "Reviewer" {
		t.Fatalf("expected [Reviewer], got %#v", out)
	}
}

func TestRoleService_GetAllRoles_DBError(t *testing.T) {
	db := newRoleTestDB(t)
	svc := &RoleService{DB: db}

	if err := db.Migrator().DropTable(&Role{}); err != nil {
		t.Fatalf("drop role table: %v", err)
	}

	_, err := svc.GetAllRoles([]string{"Admin"})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestRoleService_GetRoleByUser_OK_CurrentBehavior(t *testing.T) {
	// NOTE: your current service uses First(&roles) with a slice.
	// That returns only ONE record. This test matches that behavior.
	db := newRoleTestDB(t)
	seedUserRoles(t, db, 10)
	svc := &RoleService{DB: db}

	out, err := svc.GetRoleByUser(10)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 because First() is used, got %d: %#v", len(out), out)
	}
	if out[0].UserID != 10 {
		t.Fatalf("expected user 10, got %#v", out[0])
	}
}

func TestRoleService_GetRoleByUser_NotFound(t *testing.T) {
	db := newRoleTestDB(t)
	svc := &RoleService{DB: db}

	_, err := svc.GetRoleByUser(999999)
	if err == nil {
		t.Fatalf("expected not found error")
	}
}

func TestRoleService_GetRoleByUser_DBError(t *testing.T) {
	db := newRoleTestDB(t)
	svc := &RoleService{DB: db}

	if err := db.Migrator().DropTable(&auth.UserRole{}); err != nil {
		t.Fatalf("drop user_role table: %v", err)
	}

	_, err := svc.GetRoleByUser(10)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestRoleService_GetRolesByUserId_OK(t *testing.T) {
	db := newRoleTestDB(t)
	seedUserRoles(t, db, 10)
	svc := &RoleService{DB: db}

	out, err := svc.GetRolesByUserId(10)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 rows, got %d: %#v", len(out), out)
	}
}

func TestRoleService_GetRolesByUserId_DBError(t *testing.T) {
	db := newRoleTestDB(t)
	svc := &RoleService{DB: db}

	if err := db.Migrator().DropTable(&auth.UserRole{}); err != nil {
		t.Fatalf("drop user_role table: %v", err)
	}

	_, err := svc.GetRolesByUserId(10)
	if err == nil {
		t.Fatalf("expected error")
	}
}
