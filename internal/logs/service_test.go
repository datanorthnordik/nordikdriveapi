package logs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newMockGorm(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}

	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 db,
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true, // ✅ IMPORTANT (removes BEGIN/COMMIT)
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	cleanup := func() { _ = db.Close() }
	return gdb, mock, cleanup
}

func mustJSONPtr(t *testing.T, v any) *string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	s := string(b)
	return &s
}

func TestLogService_Log_Inserts(t *testing.T) {
	t.Run("metadata nil", func(t *testing.T) {
		db, mock, cleanup := newMockGorm(t)
		defer cleanup()

		ls := &LogService{DB: db}

		mock.ExpectQuery(`INSERT INTO "logs"`).
			WithArgs(
				sqlmock.AnyArg(), // level
				sqlmock.AnyArg(), // service
				sqlmock.AnyArg(), // user_id
				sqlmock.AnyArg(), // action
				sqlmock.AnyArg(), // message
				sqlmock.AnyArg(), // metadata
				sqlmock.AnyArg(), // created_at
				sqlmock.AnyArg(), // communities
				sqlmock.AnyArg(), // filename
			).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

		err := ls.Log(SystemLog{
			Level:       "info",
			Service:     "file",
			UserID:      ptrUint(7),
			Action:      "upload",
			Message:     "ok",
			Filename:    ptrStr("a.csv"),
			Communities: pq.StringArray{"c1", "c2"},
		}, nil)

		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("expectations: %v", err)
		}
	})

	t.Run("metadata json", func(t *testing.T) {
		db, mock, cleanup := newMockGorm(t)
		defer cleanup()

		ls := &LogService{DB: db}

		mock.ExpectQuery(`INSERT INTO "logs"`).
			WithArgs(
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
			).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(2))

		err := ls.Log(SystemLog{
			Level:       "error",
			Service:     "auth",
			Action:      "login",
			Message:     "fail",
			Communities: pq.StringArray{},
		}, map[string]any{"ip": "127.0.0.1"})

		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("expectations: %v", err)
		}
	})

	t.Run("metadata marshal fails (ignored)", func(t *testing.T) {
		db, mock, cleanup := newMockGorm(t)
		defer cleanup()

		ls := &LogService{DB: db}

		mock.ExpectQuery(`INSERT INTO "logs"`).
			WithArgs(
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
			).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(3))

		// json.Marshal on func fails; your code ignores error and inserts metadata as NULL.
		err := ls.Log(SystemLog{
			Level:   "info",
			Service: "svc",
			Action:  "act",
			Message: "msg",
		}, func() {})

		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("expectations: %v", err)
		}
	})
}

func TestLogService_GetLogs_InvalidDateRange_ReturnsError(t *testing.T) {
	db, mock, cleanup := newMockGorm(t)
	defer cleanup()
	_ = mock // no db calls expected

	ls := &LogService{DB: db}

	start := "bad-date"
	_, _, _, _, err := ls.GetLogs(LogFilterInput{
		StartDate: &start,
		Page:      1,
		PageSize:  10,
	})
	if err == nil {
		t.Fatalf("expected error for invalid date")
	}
}

func TestLogService_GetLogs_CountError_ReturnsError(t *testing.T) {
	db, mock, cleanup := newMockGorm(t)
	defer cleanup()

	ls := &LogService{DB: db}

	// Any count(*) query returns error
	mock.ExpectQuery(`SELECT count\(\*\)`).
		WillReturnError(errors.New("count failed"))

	_, _, _, _, err := ls.GetLogs(LogFilterInput{Page: 1, PageSize: 10})
	if err == nil || err.Error() != "count failed" {
		t.Fatalf("expected count failed, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestLogService_GetLogs_HappyPath_WithAggregates(t *testing.T) {
	db, mock, cleanup := newMockGorm(t)
	defer cleanup()

	ls := &LogService{DB: db}

	// 1) total count
	mock.ExpectQuery(`SELECT count\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	// 2) paged rows scan
	cols := []string{
		"id", "level", "service", "user_id", "action", "message",
		"filename", "communities", "metadata", "created_at",
		"firstname", "lastname",
	}
	now := time.Now()

	mock.ExpectQuery(`SELECT logs\.\*, a\.firstname as firstname, a\.lastname as lastname`).
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow(
				1, "info", "file", sql.NullInt64{Int64: 10, Valid: true}, "upload", "ok",
				"a.csv", []byte(`{c1,c2}`), mustJSONPtr(t, map[string]any{"k": "v"}), now,
				"John", "Doe",
			).
			AddRow(
				2, "error", "auth", sql.NullInt64{Valid: false}, "login", "fail",
				nil, []byte(`{}`), nil, now.Add(-time.Minute),
				"", "",
			))

	// 3) aggregates: ByFilename
	mock.ExpectQuery(`COALESCE\(NULLIF\(TRIM\(x\.filename\), ''\), 'No filename'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"label", "count"}).
			AddRow("a.csv", 2).
			AddRow("No filename", 1))

	// 4) aggregates: ByPerson
	mock.ExpectQuery(`CASE\s+WHEN\s+\(COALESCE\(x\.firstname,''\) = '' AND COALESCE\(x\.lastname,''\) = ''\)`).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "firstname", "lastname", "label", "count"}).
			AddRow(sql.NullInt64{Int64: 10, Valid: true}, "John", "Doe", "John Doe", 2).
			AddRow(sql.NullInt64{Valid: false}, "", "", "Unknown", 1))

	// 5) aggregates: ByCommunity (unnest)
	mock.ExpectQuery(`JOIN LATERAL unnest\(x\.communities\) AS c ON TRUE`).
		WillReturnRows(sqlmock.NewRows([]string{"label", "count"}).
			AddRow("c1", 2).
			AddRow("c2", 1))

	// 6) aggregates: No community
	mock.ExpectQuery(`array_length\(x\.communities, 1\)`).
		WillReturnRows(sqlmock.NewRows([]string{"label", "count"}).
			AddRow("No community", 1))

	rows, aggs, total, totalPages, err := ls.GetLogs(LogFilterInput{
		// page/page_size default branches are tested elsewhere; set explicit here
		Page:     1,
		PageSize: 2,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if total != 3 {
		t.Fatalf("expected total=3 got %d", total)
	}
	if totalPages != 2 { // ceil(3/2)=2
		t.Fatalf("expected totalPages=2 got %d", totalPages)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows got %d", len(rows))
	}

	// aggregates checks
	if len(aggs.ByFilename) == 0 || aggs.ByFilename[0].Label != "a.csv" {
		t.Fatalf("unexpected ByFilename: %#v", aggs.ByFilename)
	}
	if len(aggs.ByPerson) == 0 || aggs.ByPerson[0].Label == "" {
		t.Fatalf("unexpected ByPerson: %#v", aggs.ByPerson)
	}
	if len(aggs.ByCommunity) == 0 {
		t.Fatalf("unexpected ByCommunity: %#v", aggs.ByCommunity)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func ptrStr(s string) *string { return &s }
func ptrUint(u uint) *uint    { return &u }
