package auth

import (
	"errors"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if err := db.AutoMigrate(&Auth{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}

	return db
}

func TestAuthService_GetUser_ReturnsUser(t *testing.T) {
	db := newTestDB(t)

	seed := Auth{
		FirstName: "Athul",
		LastName:  "N",
		Email:     "a@b.com",
		Password:  "hashed",
		Role:      "User",
		Community: pq.StringArray{"c1", "c2"},
	}

	if err := db.Create(&seed).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	svc := &AuthService{DB: db}

	u, err := svc.GetUser("a@b.com")
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if u.Email != "a@b.com" {
		t.Fatalf("expected email a@b.com, got %s", u.Email)
	}
	if u.FirstName != "Athul" || u.LastName != "N" {
		t.Fatalf("unexpected name: %s %s", u.FirstName, u.LastName)
	}
	if len(u.Community) != 2 || u.Community[0] != "c1" || u.Community[1] != "c2" {
		t.Fatalf("unexpected communities: %#v", u.Community)
	}
}

func TestAuthService_GetUser_NotFound(t *testing.T) {
	db := newTestDB(t)
	svc := &AuthService{DB: db}

	_, err := svc.GetUser("missing@b.com")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound, got: %v", err)
	}
}

func TestAuthService_GetUser_DBBroken(t *testing.T) {
	db := newTestDB(t)

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	svc := &AuthService{DB: db}

	_, err = svc.GetUser("a@b.com")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
