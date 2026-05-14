package admin

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/xuri/excelize/v2"
)

func Test_collectAllRequestIDs_PaginatesAndDedupes(t *testing.T) {
	as := &AdminService{}

	calls := 0
	as.searchHook = func(req AdminFileEditSearchRequest) (*AdminSearchResponse, error) {
		calls++
		if req.Page == 1 {
			return &AdminSearchResponse{
				Message:    "success",
				Page:       1,
				PageSize:   200,
				TotalPages: 2,
				TotalRows:  2,
				Data: []map[string]any{
					{"request_id": 2},
					{"request_id": 1},
				},
			}, nil
		}
		return &AdminSearchResponse{
			Message:    "success",
			Page:       2,
			PageSize:   200,
			TotalPages: 2,
			TotalRows:  2,
			Data: []map[string]any{
				{"request_id": 2}, // dup
			},
		}, nil
	}

	ids, err := as.collectAllRequestIDs(ModeChanges, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 pages, got %d", calls)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("expected [1 2], got %#v", ids)
	}
}

func Test_DownloadUpdates_NoMatches_CSV(t *testing.T) {
	as := &AdminService{}
	as.searchHook = func(req AdminFileEditSearchRequest) (*AdminSearchResponse, error) {
		return &AdminSearchResponse{Message: "success", Page: 1, PageSize: 200, TotalPages: 1, Data: []any{}}, nil
	}

	ct, name, out, err := as.DownloadUpdates(ModeChanges, nil, "csv")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(ct, "text/csv") || name != "updates.csv" {
		t.Fatalf("unexpected ct/name: %q %q", ct, name)
	}
	s := string(out)
	if !strings.Contains(s, "file_name") || !strings.Contains(s, "changed_columns") {
		t.Fatalf("unexpected csv: %s", s)
	}
}

func Test_DownloadUpdates_NoMatches_XLSX(t *testing.T) {
	as := &AdminService{}
	as.searchHook = func(req AdminFileEditSearchRequest) (*AdminSearchResponse, error) {
		return &AdminSearchResponse{Message: "success", Page: 1, PageSize: 200, TotalPages: 1, Data: []any{}}, nil
	}

	ct, name, out, err := as.DownloadUpdates(ModeChanges, nil, "excel")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(ct, "spreadsheetml") || name != "updates.xlsx" {
		t.Fatalf("unexpected ct/name: %q %q", ct, name)
	}

	f, err := excelize.OpenReader(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("xlsx open: %v", err)
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) != 1 || sheets[0] != "Updates" {
		t.Fatalf("unexpected sheets: %#v", sheets)
	}
}

func Test_DownloadUpdates_SkipsRejectedDetailFields(t *testing.T) {
	db, mock, sqlDB := newMockDB(t)
	defer sqlDB.Close()

	as := &AdminService{DB: db}
	as.searchHook = func(req AdminFileEditSearchRequest) (*AdminSearchResponse, error) {
		return &AdminSearchResponse{
			Message:    "success",
			Page:       1,
			PageSize:   200,
			TotalPages: 1,
			TotalRows:  1,
			Data: []map[string]any{
				{"request_id": 10},
			},
		}, nil
	}

	createdAt := time.Now()

	mock.ExpectQuery(`(?i)from\s+"?file_edit_request_details"?`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "request_id", "file_id", "filename", "row_id", "field_name",
			"old_value", "new_value", "status", "reviewer_comment", "created_at",
		}).
			AddRow(uint(1), uint(10), uint(7), "sheet.xlsx", 12, "Name", "Old Name", "New Name", "approved", "", createdAt).
			AddRow(uint(2), uint(10), uint(7), "sheet.xlsx", 12, "Secret", "Original Secret", "Rejected Secret", "rejected", "do not use", createdAt))

	mock.ExpectQuery(`(?i)from\s+"?file"?`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "filename", "columns_order"}).
			AddRow(uint(7), "sheet.xlsx", []byte(`["Name","Secret"]`)))

	mock.ExpectQuery(`(?i)coalesce\(max\(version\),\s*0\)`).
		WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(3))

	mock.ExpectQuery(`(?i)select\s+id,\s*file_id,\s*version,\s*row_data\s+from\s+"?file_data"?`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "file_id", "version", "row_data"}).
			AddRow(uint(12), uint(7), 3, []byte(`{"Name":"Old Name","Secret":"Original Secret"}`)))

	ct, _, out, err := as.DownloadUpdates(ModeChanges, nil, "csv")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(ct, "text/csv") {
		t.Fatalf("unexpected content type: %q", ct)
	}

	rows, err := csv.NewReader(bytes.NewReader(out)).ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected header + 1 row, got %#v", rows)
	}

	header := rows[0]
	record := rows[1]
	idx := func(col string) int {
		for i, name := range header {
			if name == col {
				return i
			}
		}
		return -1
	}

	nameIdx := idx("Name")
	secretIdx := idx("Secret")
	changedIdx := idx("changed_columns")
	if nameIdx < 0 || secretIdx < 0 || changedIdx < 0 {
		t.Fatalf("missing expected columns in header: %#v", header)
	}

	if record[nameIdx] != "New Name" {
		t.Fatalf("expected approved field to be exported, got %q", record[nameIdx])
	}
	if record[secretIdx] != "Original Secret" {
		t.Fatalf("expected rejected field to keep base value, got %q", record[secretIdx])
	}
	if record[changedIdx] != "Name" {
		t.Fatalf("expected changed_columns to exclude rejected field, got %q", record[changedIdx])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
