package logs

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestLogController_GetLogs_BindError_400(t *testing.T) {
	gin.SetMode(gin.TestMode)

	lc := &LogController{LogService: &LogService{DB: &gorm.DB{}}} // DB not used (bind fails first)
	r := gin.New()
	r.POST("/logs", lc.GetLogs)

	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(`{bad json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d body=%s", w.Code, w.Body.String())
	}
}

func TestLogController_GetLogs_ServiceError_500(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, mock, cleanup := newMockGorm(t)
	defer cleanup()

	ls := &LogService{DB: db}
	lc := &LogController{LogService: ls}

	// service error: count fails
	mock.ExpectQuery(`SELECT count\(\*\)`).
		WillReturnError(assertErr("boom"))

	r := gin.New()
	r.POST("/logs", lc.GetLogs)

	body := `{"page":1,"page_size":10}`
	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 got %d body=%s", w.Code, w.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestLogController_GetLogs_OK_200(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, mock, cleanup := newMockGorm(t)
	defer cleanup()

	ls := &LogService{DB: db}
	lc := &LogController{LogService: ls}

	// count
	mock.ExpectQuery(`SELECT count\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// logs rows
	now := time.Now()
	mock.ExpectQuery(`SELECT logs\.\*, a\.firstname as firstname, a\.lastname as lastname`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "level", "service", "user_id", "action", "message",
			"filename", "communities", "metadata", "created_at",
			"firstname", "lastname",
		}).AddRow(
			1, "info", "svc", nil, "act", "msg",
			"f.csv", []byte(`{c1}`), nil, now,
			"A", "B",
		))

	// aggregates (4 queries)
	mock.ExpectQuery(`COALESCE\(NULLIF\(TRIM\(x\.filename\), ''\), 'No filename'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"label", "count"}).AddRow("f.csv", 1))
	mock.ExpectQuery(`CASE\s+WHEN\s+\(COALESCE\(x\.firstname,''\) = '' AND COALESCE\(x\.lastname,''\) = ''\)`).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "firstname", "lastname", "label", "count"}).AddRow(nil, "A", "B", "A B", 1))
	mock.ExpectQuery(`JOIN LATERAL unnest\(x\.communities\) AS c ON TRUE`).
		WillReturnRows(sqlmock.NewRows([]string{"label", "count"}).AddRow("c1", 1))
	mock.ExpectQuery(`array_length\(x\.communities, 1\)`).
		WillReturnRows(sqlmock.NewRows([]string{"label", "count"})) // none

	r := gin.New()
	r.POST("/logs", lc.GetLogs)

	body := `{"page":2,"page_size":10}`
	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal resp: %v body=%s", err, w.Body.String())
	}

	if resp["page"].(float64) != 2 {
		t.Fatalf("expected page=2 got %#v", resp["page"])
	}
	if resp["page_size"].(float64) != 10 {
		t.Fatalf("expected page_size=10 got %#v", resp["page_size"])
	}
	if _, ok := resp["aggregates"]; !ok {
		t.Fatalf("expected aggregates in response, got keys=%v", keys(resp))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
