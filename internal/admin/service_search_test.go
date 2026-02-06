package admin

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func ptr(s string) *string { return &s }

func newMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}

	dial := postgres.New(postgres.Config{
		Conn:                 sqlDB,
		PreferSimpleProtocol: true,
	})
	gdb, err := gorm.Open(dial, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}

	return gdb, mock, sqlDB
}

// ✅ GORM-safe COUNT DISTINCT matcher for r.request_id (handles quotes + optional parentheses)
const countDistinctRequestID = `(?i)select\s+count\s*\(\s*distinct\s*\(?\s*"?r"?\."?request_id"?\s*\)?\s*\)\s+from\s+file_edit_request`

func TestAdminService_searchChanges_NoDetailFilters_AggByFile(t *testing.T) {
	db, mock, sqlDB := newMockDB(t)
	defer sqlDB.Close()

	as := &AdminService{DB: db}

	// 1) totalChanges
	mock.ExpectQuery(`(?i)select\s+count\(\*\).*file_edit_request_details`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(2)))

	// 2) totalRequests ✅ FIXED
	mock.ExpectQuery(countDistinctRequestID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))

	// 3) aggregations by file
	mock.ExpectQuery(`(?i)coalesce\(f\.filename`).
		WillReturnRows(sqlmock.NewRows([]string{"key", "count"}).AddRow("fileA.xlsx", int64(1)))

	// 4) page rows
	createdAt := time.Now()
	mock.ExpectQuery(`(?i)dc\.change_count`).
		WillReturnRows(sqlmock.NewRows([]string{
			"request_id", "status", "file_id", "file_name",
			"firstname", "lastname",
			"community", "uploader_community",
			"requested_by", "approved_by",
			"consent", "change_count", "created_at",
		}).AddRow(
			uint(10), "PENDING", uint(5), "fileA.xlsx",
			"fn", "ln",
			[]byte("{c1,c2}"), []byte("{u1}"),
			"Req User", "App User",
			true, int64(2), createdAt,
		))

	resp, err := as.SearchFileEditRequests(AdminFileEditSearchRequest{
		Mode:     ModeChanges,
		Page:     1,
		PageSize: 20,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.TotalRows != 1 || resp.TotalChanges != 2 {
		t.Fatalf("unexpected totals: %+v", resp)
	}
	if len(resp.Aggregations.ByFile) != 1 || resp.Aggregations.ByFile[0].Key != "fileA.xlsx" {
		t.Fatalf("expected ByFile aggs, got %+v", resp.Aggregations)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestAdminService_searchChanges_WithDetailFilters_AggByField(t *testing.T) {
	db, mock, sqlDB := newMockDB(t)
	defer sqlDB.Close()

	as := &AdminService{DB: db}

	mock.ExpectQuery(`(?i)select\s+count\(\*\).*file_edit_request_details`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(3)))

	// totalRequests ✅ FIXED
	mock.ExpectQuery(countDistinctRequestID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(2)))

	// hasFileFilter => agg by field
	mock.ExpectQuery(`(?i)d\.field_name\s+as\s+key`).
		WillReturnRows(sqlmock.NewRows([]string{"key", "count"}).
			AddRow("firstname", int64(2)).
			AddRow("lastname", int64(1)))

	mock.ExpectQuery(`(?i)dc\.change_count`).
		WillReturnRows(sqlmock.NewRows([]string{
			"request_id", "status", "file_id", "file_name",
			"firstname", "lastname",
			"community", "uploader_community",
			"requested_by", "approved_by",
			"consent", "change_count", "created_at",
		}).AddRow(
			uint(11), "APPROVED", uint(9), "fileB.xlsx",
			"A", "B",
			[]byte("{}"), []byte("{}"),
			"Req", "App",
			false, int64(3), time.Now(),
		))

	resp, err := as.SearchFileEditRequests(AdminFileEditSearchRequest{
		Mode: ModeChanges,
		Clauses: []Clause{
			{Field: "file_id", Op: OpEQ, Value: ptr("9")},             // file filter
			{Field: "field_key", Op: OpCONTAINS, Value: ptr("first")}, // detail filter
		},
		Page:     1,
		PageSize: 20,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(resp.Aggregations.ByField) == 0 {
		t.Fatalf("expected ByField aggs, got %+v", resp.Aggregations)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestAdminService_GetFileEditRequestDetails_OK(t *testing.T) {
	db, mock, sqlDB := newMockDB(t)
	defer sqlDB.Close()

	as := &AdminService{DB: db}

	mock.ExpectQuery(`(?i)file_edit_request_details`).
		WillReturnRows(sqlmock.NewRows([]string{"field_key", "old_value", "new_value"}).
			AddRow("firstname", "Old", "New").
			AddRow("age", "", "30"))

	out, err := as.GetFileEditRequestDetails(123)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 2 || out[0].FieldKey != "firstname" {
		t.Fatalf("unexpected out: %#v", out)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestAdminService_loadMediaRows_InvalidDocType(t *testing.T) {
	as := &AdminService{DB: nil}
	_, err := as.loadMediaRows([]uint{1}, "weird", nil)
	if err == nil {
		t.Fatalf("expected error")
	}
}
