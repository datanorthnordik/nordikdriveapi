package auth

import (
	"errors"
	"fmt"
	"net/smtp"
	"nordik-drive-api/config"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/glebarez/sqlite"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	// Unique DB per test name so data doesn't leak across tests
	name := strings.NewReplacer("/", "_", "\\", "_", " ", "_", ":", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", name)

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if err := db.AutoMigrate(&Auth{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}

	sqlDB, err := db.DB()
	if err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
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

func newMockGormPostgres(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}

	return db, mock
}

func TestAuthService_CreateUser_SetsDefaultRole_WhenEmpty(t *testing.T) {
	db, mock := newMockGormPostgres(t)

	mock.ExpectQuery(`INSERT INTO "users"`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	svc := &AuthService{DB: db}

	u := Auth{Email: "a@b.com", Password: "hashed", Role: ""}
	created, err := svc.CreateUser(u)
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if created.Role != "User" {
		t.Fatalf("expected Role=User, got %s", created.Role)
	}
	if created.ID == 0 {
		t.Fatalf("expected ID to be set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestAuthService_CreateUser_DoesNotOverrideRole_WhenProvided(t *testing.T) {
	db, mock := newMockGormPostgres(t)

	mock.ExpectQuery(`INSERT INTO "users"`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(2))

	svc := &AuthService{DB: db}

	u := Auth{Email: "admin@b.com", Password: "hashed", Role: "Admin"}
	created, err := svc.CreateUser(u)
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if created.Role != "Admin" {
		t.Fatalf("expected Role=Admin, got %s", created.Role)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestAuthService_CreateUser_UniqueViolation_ReturnsFriendlyMessage(t *testing.T) {
	db, mock := newMockGormPostgres(t)

	mock.ExpectQuery(`INSERT INTO "users"`).
		WillReturnError(errors.New(`duplicate key value violates unique constraint "users_email_key"`))

	svc := &AuthService{DB: db}

	_, err := svc.CreateUser(Auth{Email: "a@b.com", Password: "hashed"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "An account with this email or phone number already exists. Please log in or use different details." {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestAuthService_CreateUser_OtherDBError_ReturnsOriginal(t *testing.T) {
	db, mock := newMockGormPostgres(t)

	mock.ExpectQuery(`INSERT INTO "users"`).
		WillReturnError(errors.New("some db error"))

	svc := &AuthService{DB: db}

	_, err := svc.CreateUser(Auth{Email: "a@b.com", Password: "hashed"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "some db error" {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestAuthService_GetUserByID_ReturnsUser(t *testing.T) {
	db := newTestDB(t)

	seed := Auth{
		FirstName: "A",
		LastName:  "B",
		Email:     "id@b.com",
		Password:  "hashed",
		Role:      "User",
	}

	if err := db.Create(&seed).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	svc := &AuthService{DB: db}

	u, err := svc.GetUserByID(seed.ID)
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if u.ID != seed.ID {
		t.Fatalf("expected id %d, got %d", seed.ID, u.ID)
	}
	if u.Email != "id@b.com" {
		t.Fatalf("expected email id@b.com, got %s", u.Email)
	}
}

func TestAuthService_GetUserByID_NotFound(t *testing.T) {
	db := newTestDB(t)
	svc := &AuthService{DB: db}

	_, err := svc.GetUserByID(999999)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound, got: %v", err)
	}
}

func TestAuthService_GetUserByID_DBBroken(t *testing.T) {
	db := newTestDB(t)

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	svc := &AuthService{DB: db}

	_, err = svc.GetUserByID(1)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestAuthService_GetAllUsers_ReturnsOnlyRoleUser(t *testing.T) {
	db := newTestDB(t)

	seed := []Auth{
		{FirstName: "U1", LastName: "L1", Email: "u1@b.com", Password: "x", Role: "User"},
		{FirstName: "U2", LastName: "L2", Email: "u2@b.com", Password: "x", Role: "User"},
		{FirstName: "A1", LastName: "L3", Email: "a1@b.com", Password: "x", Role: "Admin"},
	}

	for i := range seed {
		if err := db.Create(&seed[i]).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	svc := &AuthService{DB: db}

	users, err := svc.GetAllUsers()
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d: %#v", len(users), users)
	}
	for _, u := range users {
		if u.Role != "User" {
			t.Fatalf("expected only Role=User, got: %s", u.Role)
		}
	}
}

func TestAuthService_GetAllUsers_Empty(t *testing.T) {
	db := newTestDB(t)
	svc := &AuthService{DB: db}

	users, err := svc.GetAllUsers()
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("expected 0 users, got %d", len(users))
	}
}

func TestAuthService_GetAllUsers_DBBroken(t *testing.T) {
	db := newTestDB(t)

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	svc := &AuthService{DB: db}

	_, err = svc.GetAllUsers()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestAuthService_ResetPassword_InvalidOTP(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&OTP{}); err != nil {
		t.Fatalf("automigrate otp: %v", err)
	}

	svc := &AuthService{DB: db}

	_, err := svc.ResetPassword("a@b.com", "111111", "123456")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "invalid OTP" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthService_ResetPassword_UserNotFound(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&OTP{}); err != nil {
		t.Fatalf("automigrate otp: %v", err)
	}

	if err := db.Create(&OTP{Email: "a@b.com", Code: "111111", CreatedAt: time.Now()}).Error; err != nil {
		t.Fatalf("seed otp: %v", err)
	}

	svc := &AuthService{DB: db}

	_, err := svc.ResetPassword("a@b.com", "111111", "123456")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "user not found" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthService_ResetPassword_OTPExpired(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&OTP{}); err != nil {
		t.Fatalf("automigrate otp: %v", err)
	}

	if err := db.Create(&Auth{Email: "a@b.com", Password: "old", Role: "User"}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}
	old := time.Now().Add(-11 * time.Minute)
	if err := db.Create(&OTP{Email: "a@b.com", Code: "111111", CreatedAt: old}).Error; err != nil {
		t.Fatalf("seed otp: %v", err)
	}

	svc := &AuthService{DB: db}

	_, err := svc.ResetPassword("a@b.com", "111111", "123456")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "OTP expired" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthService_ResetPassword_OK_UpdatesPassword(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&OTP{}); err != nil {
		t.Fatalf("automigrate otp: %v", err)
	}

	if err := db.Create(&Auth{Email: "a@b.com", Password: "old", Role: "User"}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := db.Create(&OTP{Email: "a@b.com", Code: "111111", CreatedAt: time.Now()}).Error; err != nil {
		t.Fatalf("seed otp: %v", err)
	}

	svc := &AuthService{DB: db}

	_, err := svc.ResetPassword("a@b.com", "111111", "123456")
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}

	var updated Auth
	if err := db.Where("email = ?", "a@b.com").First(&updated).Error; err != nil {
		t.Fatalf("fetch updated user: %v", err)
	}
	if updated.Password == "old" || updated.Password == "" {
		t.Fatalf("expected password updated & hashed, got: %q", updated.Password)
	}
}

func TestAuthService_SendOTP_UserNotFound(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&OTP{}); err != nil {
		t.Fatalf("automigrate otp: %v", err)
	}

	svc := &AuthService{
		DB:  db,
		CFG: &config.Config{GmailUser: "from@test.com", GmailPass: "pass"},
	}

	_, _, err := svc.SendOTP("missing@b.com")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "user not found" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthService_SendOTP_OK_CreatesOTP_AndSendsMail(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&OTP{}); err != nil {
		t.Fatalf("automigrate otp: %v", err)
	}

	if err := db.Create(&Auth{Email: "a@b.com", Password: "x", Role: "User"}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	prev := sendMail
	t.Cleanup(func() { sendMail = prev })

	var sentMsg []byte
	sendMail = func(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
		sentMsg = msg
		if addr != "smtp.gmail.com:587" {
			t.Fatalf("unexpected addr: %s", addr)
		}
		if from != "from@test.com" {
			t.Fatalf("unexpected from: %s", from)
		}
		if len(to) != 1 || to[0] != "a@b.com" {
			t.Fatalf("unexpected to: %#v", to)
		}
		return nil
	}

	svc := &AuthService{
		DB:  db,
		CFG: &config.Config{GmailUser: "from@test.com", GmailPass: "pass"},
	}

	user, otp, err := svc.SendOTP("a@b.com")
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if user == nil || user.Email != "a@b.com" {
		t.Fatalf("unexpected user: %+v", user)
	}

	// returned otp must be 6 digits
	if matched, _ := regexp.MatchString(`^\d{6}$`, otp); !matched {
		t.Fatalf("expected 6-digit otp, got: %q", otp)
	}

	// ensure email body contains same otp
	if !strings.Contains(string(sentMsg), otp) {
		t.Fatalf("expected email to contain otp %q, got msg=%s", otp, string(sentMsg))
	}

	// ensure saved in DB
	var saved OTP
	if err := db.Where("email = ?", "a@b.com").Order("created_at desc").First(&saved).Error; err != nil {
		t.Fatalf("expected otp record: %v", err)
	}
	if saved.Code != otp {
		t.Fatalf("otp mismatch: saved=%q returned=%q", saved.Code, otp)
	}
}

func TestAuthService_SendOTP_SaveOTPDBError(t *testing.T) {
	db := newTestDB(t)

	// Do not migrate OTP table, so Create(&record) fails before sendMail
	if err := db.AutoMigrate(&Auth{}); err != nil {
		t.Fatalf("automigrate auth: %v", err)
	}
	if err := db.Create(&Auth{Email: "a@b.com", Password: "x", Role: "User"}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	svc := &AuthService{
		DB:  db,
		CFG: &config.Config{GmailUser: "from@test.com", GmailPass: "pass"},
	}

	_, _, err := svc.SendOTP("a@b.com")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestAuthService_SendOTP_SendMailFails_ReturnsFriendlyError(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&OTP{}); err != nil {
		t.Fatalf("automigrate otp: %v", err)
	}

	if err := db.Create(&Auth{Email: "a@b.com", Password: "x", Role: "User"}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	prev := sendMail
	t.Cleanup(func() { sendMail = prev })

	sendMail = func(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
		return assertErr("smtp down")
	}

	svc := &AuthService{
		DB:  db,
		CFG: &config.Config{GmailUser: "from@test.com", GmailPass: "pass"},
	}

	_, _, err := svc.SendOTP("a@b.com")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "failed to send OTP email" {
		t.Fatalf("unexpected error: %v", err)
	}
}
