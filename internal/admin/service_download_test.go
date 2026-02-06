package admin

import (
	"bytes"
	"strings"
	"testing"

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
