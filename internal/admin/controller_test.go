package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

type errTest string

func (e errTest) Error() string { return string(e) }

type fakeAdminService struct {
	lastSearchReq AdminFileEditSearchRequest
	lastDetailsID uint
	lastDLMode    Mode
	lastDLClauses []Clause
	lastDLFormat  string
	lastMediaReq  AdminDownloadMediaRequest

	searchResp  *AdminSearchResponse
	searchErr   error
	detailsResp []AdminChangeDetailRow
	detailsErr  error

	dlCT    string
	dlName  string
	dlBytes []byte
	dlErr   error

	streamWriteData []byte
	streamErr       error
}

func (f *fakeAdminService) SearchFileEditRequests(req AdminFileEditSearchRequest) (*AdminSearchResponse, error) {
	f.lastSearchReq = req
	return f.searchResp, f.searchErr
}
func (f *fakeAdminService) GetFileEditRequestDetails(id uint) ([]AdminChangeDetailRow, error) {
	f.lastDetailsID = id
	return f.detailsResp, f.detailsErr
}
func (f *fakeAdminService) DownloadUpdates(mode Mode, clauses []Clause, format string) (string, string, []byte, error) {
	f.lastDLMode = mode
	f.lastDLClauses = clauses
	f.lastDLFormat = format
	return f.dlCT, f.dlName, f.dlBytes, f.dlErr
}
func (f *fakeAdminService) StreamMediaZip(ctx context.Context, out io.Writer, req AdminDownloadMediaRequest) error {
	f.lastMediaReq = req
	if len(f.streamWriteData) > 0 {
		_, _ = out.Write(f.streamWriteData)
	}
	return f.streamErr
}

func newTestRouter(ctrl *AdminController) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/admin", ctrl.SearchFileEditRequests)
	r.POST("/api/admin/details", ctrl.GetFileEditRequestDetails)
	r.GET("/api/admin/details/:request_id", ctrl.GetFileEditRequestDetailsByParam)
	r.POST("/api/admin/download", ctrl.DownloadUpdates)
	r.POST("/api/admin/download_files", ctrl.DownloadMediaZip)
	return r
}

func doJSON(r http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestAdminController_SearchFileEditRequests_BadJSON(t *testing.T) {
	f := &fakeAdminService{}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	req := httptest.NewRequest("POST", "/api/admin", strings.NewReader("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d, body=%s", w.Code, w.Body.String())
	}
}

func TestAdminController_SearchFileEditRequests_Defaults(t *testing.T) {
	f := &fakeAdminService{
		searchResp: &AdminSearchResponse{Message: "success", Page: 1, PageSize: 20, TotalPages: 1, TotalRows: 0, Data: []AdminChangeRow{}},
	}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "POST", "/api/admin", AdminFileEditSearchRequest{Mode: ModeChanges, Page: 0, PageSize: 0})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", w.Code, w.Body.String())
	}
	if f.lastSearchReq.Page != 1 || f.lastSearchReq.PageSize != 20 {
		t.Fatalf("expected Page=1 PageSize=20, got Page=%d PageSize=%d", f.lastSearchReq.Page, f.lastSearchReq.PageSize)
	}
}

func TestAdminController_SearchFileEditRequests_PageSizeCapped(t *testing.T) {
	f := &fakeAdminService{searchResp: &AdminSearchResponse{Message: "success", Page: 1, PageSize: 20, TotalPages: 1, TotalRows: 0, Data: []AdminChangeRow{}}}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	_ = doJSON(r, "POST", "/api/admin", AdminFileEditSearchRequest{Mode: ModeChanges, Page: 1, PageSize: 999})
	if f.lastSearchReq.PageSize != 20 {
		t.Fatalf("expected PageSize=20, got %d", f.lastSearchReq.PageSize)
	}
}

func TestAdminController_SearchFileEditRequests_ServiceError(t *testing.T) {
	f := &fakeAdminService{searchErr: errTest("boom")}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "POST", "/api/admin", AdminFileEditSearchRequest{Mode: ModeChanges, Page: 1, PageSize: 20})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d, body=%s", w.Code, w.Body.String())
	}
}

func TestAdminController_GetFileEditRequestDetails_MissingID(t *testing.T) {
	f := &fakeAdminService{}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "POST", "/api/admin/details", AdminDetailsRequest{RequestID: 0})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d, body=%s", w.Code, w.Body.String())
	}
}

func TestAdminController_GetFileEditRequestDetails_OK(t *testing.T) {
	f := &fakeAdminService{
		detailsResp: []AdminChangeDetailRow{{FieldKey: "a", OldValue: "x", NewValue: "y"}, {FieldKey: "b", OldValue: "", NewValue: "1"}},
	}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "POST", "/api/admin/details", AdminDetailsRequest{RequestID: 123})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", w.Code, w.Body.String())
	}
	if f.lastDetailsID != 123 {
		t.Fatalf("expected 123, got %d", f.lastDetailsID)
	}
}

func TestAdminController_GetFileEditRequestDetailsByParam_Invalid(t *testing.T) {
	f := &fakeAdminService{}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "GET", "/api/admin/details/abc", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d, body=%s", w.Code, w.Body.String())
	}
}

func TestAdminController_DownloadUpdates_DefaultFormatExcel(t *testing.T) {
	f := &fakeAdminService{
		dlCT:    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		dlName:  "updates.xlsx",
		dlBytes: []byte{1, 2, 3},
	}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "POST", "/api/admin/download", AdminDownloadRequest{Mode: ModeChanges})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", w.Code, w.Body.String())
	}
	if f.lastDLFormat != "excel" {
		t.Fatalf("expected excel, got %q", f.lastDLFormat)
	}
	if !bytes.Equal(w.Body.Bytes(), f.dlBytes) {
		t.Fatalf("expected body bytes match")
	}
}

func TestAdminController_DownloadMediaZip_RejectsNoFilters(t *testing.T) {
	f := &fakeAdminService{}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "POST", "/api/admin/download_files", AdminDownloadMediaRequest{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminController_DownloadMediaZip_StreamsAndSetsHeaders(t *testing.T) {
	f := &fakeAdminService{streamWriteData: []byte("ZIPDATA")}
	ctrl := &AdminController{AdminService: f}
	r := newTestRouter(ctrl)

	w := doJSON(r, "POST", "/api/admin/download_files", AdminDownloadMediaRequest{RequestIDs: []uint{0, 2, 2, 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Content-Type") != "application/zip" {
		t.Fatalf("expected application/zip, got %q", w.Header().Get("Content-Type"))
	}
	if w.Body.String() != "ZIPDATA" {
		t.Fatalf("expected streamed data, got %q", w.Body.String())
	}
}
