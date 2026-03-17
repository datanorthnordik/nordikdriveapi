package formsubmission

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var testDBSeq uint64
var testSubmissionSeq uint64

func newTestService(t *testing.T) *FormSubmissionService {
	t.Helper()

	id := atomic.AddUint64(&testDBSeq, 1)
	dsn := fmt.Sprintf("file:formsubmission_test_%d?mode=memory&cache=shared", id)

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	if err := db.AutoMigrate(
		&FormSubmissionUserRef{},
		&FormSubmission{},
		&FormSubmissionDetail{},
		&FormSubmissionUpload{},
		&FormFileMapping{},
	); err != nil {
		t.Fatalf("migrate db: %v", err)
	}

	users := []FormSubmissionUserRef{
		{ID: 1, Email: "user1@example.com", FirstName: "User", LastName: "One"},
		{ID: 2, Email: "user2@example.com", FirstName: "User", LastName: "Two"},
		{ID: 5, Email: "user5@example.com", FirstName: "User", LastName: "Five"},
		{ID: 7, Email: "user7@example.com", FirstName: "User", LastName: "Seven"},
		{ID: 8, Email: "user8@example.com", FirstName: "User", LastName: "Eight"},
		{ID: 9, Email: "user9@example.com", FirstName: "User", LastName: "Nine"},
		{ID: 42, Email: "user42@example.com", FirstName: "User", LastName: "Forty-Two"},
		{ID: 99, Email: "reviewer@example.com", FirstName: "Reviewer", LastName: "One"},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("seed users: %v", err)
	}

	t.Cleanup(func() { _ = sqlDB.Close() })

	return &FormSubmissionService{DB: db}
}

func breakDB(t *testing.T, db *gorm.DB) {
	t.Helper()

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()
}

func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }
func boolPtr(v bool) *bool    { return &v }
func strPtr(v string) *string { return &v }

func newGinRouter(userID interface{}, register func(r *gin.Engine)) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	if userID != nil {
		r.Use(func(c *gin.Context) {
			c.Set("userID", userID)
			c.Next()
		})
	}

	register(r)
	return r
}

func doReq(r http.Handler, method, target string, body io.Reader) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, body)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	return rr
}

func doJSONReq(r http.Handler, method, target string, body interface{}) *httptest.ResponseRecorder {
	if body == nil {
		return doReq(r, method, target, nil)
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		panic(err)
	}

	return doReq(r, method, target, &buf)
}

func mustDecode[T any](t *testing.T, rr *httptest.ResponseRecorder) T {
	t.Helper()

	var out T
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode body: %v body=%s", err, rr.Body.String())
	}
	return out
}

func assertStatus(t *testing.T, rr *httptest.ResponseRecorder, want int) {
	t.Helper()

	if rr.Code != want {
		t.Fatalf("expected status %d, got %d, body=%s", want, rr.Code, rr.Body.String())
	}
}

func assertErrorContains(t *testing.T, rr *httptest.ResponseRecorder, wantCode int, want string) {
	t.Helper()

	assertStatus(t, rr, wantCode)

	var m map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode error body: %v", err)
	}

	got, _ := m["error"].(string)
	if !strings.Contains(got, want) {
		t.Fatalf("expected error containing %q, got %q", want, got)
	}
}

func seedSubmissionWithDetailAndUpload(t *testing.T, svc *FormSubmissionService) (FormSubmission, FormSubmissionDetail, FormSubmissionUpload) {
	t.Helper()

	n := atomic.AddUint64(&testSubmissionSeq, 1)

	createdBy := 1
	sub := FormSubmission{
		FileID:      int64(1000 + n),
		RowID:       int64(2000 + n),
		FileName:    fmt.Sprintf("sheet_%d.xlsx", n),
		FormKey:     fmt.Sprintf("boarding_%d", n),
		FormLabel:   "Boarding",
		CreatedByID: &createdBy,
		Status:      ReviewStatusPending,
	}
	if err := svc.DB.Create(&sub).Error; err != nil {
		t.Fatalf("create submission: %v", err)
	}

	detail := FormSubmissionDetail{
		SubmissionID: sub.ID,
		DetailKey:    "passport_no",
		DetailLabel:  "Passport Number",
		FieldType:    "text",
		ValueJSON:    []byte(`"A12345"`),
	}
	if err := svc.DB.Create(&detail).Error; err != nil {
		t.Fatalf("create detail: %v", err)
	}

	upload := FormSubmissionUpload{
		SubmissionID:    sub.ID,
		DetailID:        detail.ID,
		UploadType:      "document",
		FileName:        fmt.Sprintf("passport_%d.pdf", n),
		MimeType:        "application/pdf",
		FileURL:         fmt.Sprintf("gs://bucket/path/passport_%d.pdf", n),
		FileSizeBytes:   100,
		Status:          UploadReviewStatusPending,
		ReviewerComment: "",
	}
	if err := svc.DB.Create(&upload).Error; err != nil {
		t.Fatalf("create upload: %v", err)
	}

	return sub, detail, upload
}

type errReadCloser struct {
	readErr  error
	closeErr error
}

func (e *errReadCloser) Read(p []byte) (int, error) { return 0, e.readErr }
func (e *errReadCloser) Close() error               { return e.closeErr }

type staticReadCloser struct {
	data     []byte
	readDone bool
	closeErr error
}

func (s *staticReadCloser) Read(p []byte) (int, error) {
	if s.readDone {
		return 0, io.EOF
	}
	s.readDone = true
	n := copy(p, s.data)
	return n, io.EOF
}

func (s *staticReadCloser) Close() error { return s.closeErr }

func fixedTime() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}
