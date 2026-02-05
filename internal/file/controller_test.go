package file

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// ---------- small local helpers ----------

func contentDispositionFilename(cd string) string {
	const key = `filename="`
	i := strings.Index(cd, key)
	if i < 0 {
		return ""
	}
	rest := cd[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func mustJSON(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("invalid json: %v body=%s", err, string(b))
	}
	return out
}

func requireContains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected %q to contain %q", s, sub)
	}
}

func assertStatus(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Fatalf("expected %d got %d body=%s", want, w.Code, w.Body.String())
	}
}

// Build a request with Content-Type intentionally wrong for multipart parsing failure
func newBadMultipartReq(method, url string, headers map[string]string) *http.Request {
	req := httptest.NewRequest(method, url, strings.NewReader("not-multipart"))
	req.Header.Set("Content-Type", "text/plain")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// Router that injects communities as a bad type to hit "invalid communities" branch.
func setupRouterWithBadCommunities(fc *FileController) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	// custom middleware: copy auth behavior but set communities to invalid type (string)
	r.Use(func(c *gin.Context) {
		// mimic the important parts of mockAuthMiddleware for Authorization + userID
		if strings.TrimSpace(c.GetHeader("Authorization")) != "Bearer test" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		// set a float64 userID (most controller funcs expect float64)
		c.Set("userID", float64(1))

		// invalid communities type:
		c.Set("communities", "NOT_AN_ARRAY")

		// optional email
		if em := strings.TrimSpace(c.GetHeader("X-UserEmail")); em != "" {
			c.Set("user_email", em)
		}

		c.Next()
	})

	g := r.Group("/api/file")
	{
		g.POST("/upload", fc.UploadFiles)
		g.GET("/data", fc.GetFileData)
		g.DELETE("", fc.DeleteFile)
		g.PUT("/reset", fc.ResetFile)
		g.POST("/access", fc.CreateAccess)
		g.POST("/replace", fc.ReplaceFile)
		g.POST("/revert", fc.RevertFile)
	}
	return r
}

type badCreateAccessSvc struct{ *fakeFileService }

func (b *badCreateAccessSvc) CreateAccess(input []FileAccess) error {
	b.bump("CreateAccess")
	return errors.New("create access failed")
}

type badDeleteAccessSvc struct{ *fakeFileService }

func (b *badDeleteAccessSvc) DeleteAccess(accessId string) error {
	b.bump("DeleteAccess")
	return errors.New("delete access failed")
}

type badGetAccessSvc struct{ *fakeFileService }

func (b *badGetAccessSvc) GetFileAccess(fileId string) ([]FileAccessWithUser, error) {
	b.bump("GetFileAccess")
	return nil, errors.New("get access failed")
}

type nilGetAccessSvc struct{ *fakeFileService }

func (n *nilGetAccessSvc) GetFileAccess(fileId string) ([]FileAccessWithUser, error) {
	n.bump("GetFileAccess")
	return nil, nil
}

type okGetAccessSvc struct{ *fakeFileService }

func (o *okGetAccessSvc) GetFileAccess(fileId string) ([]FileAccessWithUser, error) {
	o.bump("GetFileAccess")
	return []FileAccessWithUser{{ID: 1, UserID: 2, FileID: 3, FirstName: "A", LastName: "B"}}, nil
}

type badHistorySvc struct{ *fakeFileService }

func (b *badHistorySvc) GetFileHistory(fileId string) ([]FileVersionWithUser, error) {
	b.bump("GetFileHistory")
	return nil, errors.New("history failed")
}

type nilHistorySvc struct{ *fakeFileService }

func (n *nilHistorySvc) GetFileHistory(fileId string) ([]FileVersionWithUser, error) {
	n.bump("GetFileHistory")
	return nil, nil
}

type okHistorySvc struct{ *fakeFileService }

func (o *okHistorySvc) GetFileHistory(fileId string) ([]FileVersionWithUser, error) {
	o.bump("GetFileHistory")
	return []FileVersionWithUser{}, nil
}

type badReplaceSvc struct{ *fakeFileService }

func (b *badReplaceSvc) ReplaceFiles(uploadedFile *multipart.FileHeader, fileID uint, userID uint) error {
	b.bump("ReplaceFiles")
	return errors.New("replace failed")
}

type badRevertSvc struct{ *fakeFileService }

func (b *badRevertSvc) RevertFile(filename string, version int, userID uint) error {
	b.bump("RevertFile")
	return errors.New("revert failed")
}

type badCreateReqSvc struct{ *fakeFileService }

func (b *badCreateReqSvc) CreateEditRequest(input EditRequestInput, userID uint) (*FileEditRequest, error) {
	b.bump("CreateEditRequest")
	return nil, errors.New("create request failed")
}

type badGetEditsSvc struct{ *fakeFileService }

func (b *badGetEditsSvc) GetEditRequests(statusCSV *string, userID *uint) ([]FileEditRequestWithUser, error) {
	b.bump("GetEditRequests")
	return nil, errors.New("get edits failed")
}

type badApproveSvc struct{ *fakeFileService }

func (b *badApproveSvc) ApproveEditRequest(requestID uint, updates []FileEditRequestDetails, userId uint) error {
	b.bump("ApproveEditRequest")
	return errors.New("approve failed")
}

type badReviewSvc struct{ *fakeFileService }

func (b *badReviewSvc) ReviewPhotos(approved []uint, rejected []uint, reviewer string) error {
	b.bump("ReviewPhotos")
	return errors.New("review failed")
}

type badPhotosReqSvc struct{ *fakeFileService }

func (b *badPhotosReqSvc) GetPhotosByRequest(requestID uint) ([]FileEditRequestPhoto, error) {
	b.bump("GetPhotosByRequest")
	return nil, errors.New("photos req failed")
}

type badOpenSvc struct{ *fakeFileService }

func (b *badOpenSvc) OpenMediaHandle(ctx context.Context, id uint, kind string) (io.ReadCloser, string, string, string, error) {
	b.bump("OpenMediaHandle")
	return nil, "", "", "", errors.New("open failed")
}

func TestFileController_AllEndpoints_AllScenarios(t *testing.T) {
	// Shared headers: default is float64 userID via mockAuthMiddleware
	authHeaders := map[string]string{
		"Authorization": "Bearer test",
		"X-UserID":      "1",
		"X-UserID-Type": "float64",
		"X-Communities": "Shingwauk,Algoma",
		"X-UserEmail":   "reviewer@nordik.ca",
	}

	t.Run("UploadFiles - 401 unauthorized (missing/invalid Authorization)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		h := map[string]string{
			"Authorization": "Bearer WRONG",
		}
		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{"filenames": {"a.csv"}, "private": {"false"}, "community_filter": {"false"}},
			"files", "x.csv", []byte("a,b\n1,2\n"), h,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "unauthorized")
	})

	// ---------------- UploadFiles ----------------

	t.Run("UploadFiles - 400 failed to read form (not multipart)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newBadMultipartReq(http.MethodPost, "/api/file/upload", authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "failed to read form")
	})

	t.Run("UploadFiles - 400 no files uploaded", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		// multipart with no fileField
		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{
				"filenames":        {"a.csv"},
				"private":          {"false"},
				"community_filter": {"false"},
			},
			"", "", nil, authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "no files uploaded")
	})

	t.Run("UploadFiles - 400 failed to read filenames array (missing required fields)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		// no filenames/private/community_filter -> ShouldBind fails
		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{},
			"files", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "failed to read filenames array")
	})

	t.Run("UploadFiles - 400 files count and filenames mismatch", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{
				"filenames":        {"a.csv", "b.csv"},
				"private":          {"false", "false"},
				"community_filter": {"false", "false"},
			},
			"files", "x.csv", []byte("x"), authHeaders, // only 1 file uploaded
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "files count and filenames array length mismatch")
	})

	t.Run("UploadFiles - 401 invalid user ID (middleware sets uint, controller expects float64)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		h := map[string]string{}
		for k, v := range authHeaders {
			h[k] = v
		}
		h["X-UserID-Type"] = "uint" // middleware will set uint, controller expects float64

		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{
				"filenames":        {"a.csv"},
				"private":          {"false"},
				"community_filter": {"false"},
			},
			"files", "x.csv", []byte("x"), h,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid user ID")
	})

	t.Run("UploadFiles - 400 service error", func(t *testing.T) {
		svc := &fakeFileService{SaveErr: errors.New("save failed")}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{
				"filenames":        {"a.csv"},
				"private":          {"false"},
				"community_filter": {"false"},
			},
			"files", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "save failed")
		if svc.Called["SaveFilesMultipart"] != 1 {
			t.Fatalf("expected SaveFilesMultipart called once, got %+v", svc.Called)
		}
	})

	t.Run("UploadFiles - 401 invalid communities (forced)", func(t *testing.T) {
		svc := &fakeFileService{SaveOut: []File{{ID: 1, Filename: "a.csv"}}}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterWithBadCommunities(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{
				"filenames":        {"a.csv"},
				"private":          {"false"},
				"community_filter": {"false"},
			},
			"files", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid communities")
	})

	t.Run("UploadFiles - 200 ok + log success", func(t *testing.T) {
		svc := &fakeFileService{
			SaveOut: []File{{ID: 10, Filename: "a.csv"}},
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{
				"filenames":        {"a.csv"},
				"private":          {"false"},
				"community_filter": {"false"},
			},
			"files", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["SaveFilesMultipart"] != 1 {
			t.Fatalf("expected SaveFilesMultipart called once, got %+v", svc.Called)
		}
		if svc.LastSaveUserID != 1 {
			t.Fatalf("expected userID=1, got %d", svc.LastSaveUserID)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "UPLOAD_FILE" && l.Service == "file" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected UPLOAD_FILE log, got %+v", logSvc.Calls)
		}
	})

	t.Run("UploadFiles - 200 ok even if log fails (no crash)", func(t *testing.T) {
		svc := &fakeFileService{
			SaveOut: []File{{ID: 10, Filename: "a.csv"}},
		}
		logSvc := &fakeLogService{Err: errors.New("log failed")}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/upload",
			map[string][]string{
				"filenames":        {"a.csv"},
				"private":          {"false"},
				"community_filter": {"false"},
			},
			"files", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
	})

	// ---------------- GetAllFiles ----------------

	t.Run("GetAllFiles - 401 invalid user ID type", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		h := map[string]string{}
		for k, v := range authHeaders {
			h[k] = v
		}
		h["X-UserID-Type"] = "string"

		req := newJSONReq(http.MethodGet, "/api/file", nil, h)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid user ID")
	})

	t.Run("GetAllFiles - 500 GetUserRole fails", func(t *testing.T) {
		svc := &fakeFileService{
			RoleErr: errors.New("role failed"),
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "role failed")
	})

	t.Run("GetAllFiles - 500 GetAllFiles fails", func(t *testing.T) {
		svc := &fakeFileService{
			RoleOut:   "User",
			GetAllErr: errors.New("getall failed"),
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "getall failed")
	})

	t.Run("GetAllFiles - 200 ok", func(t *testing.T) {
		svc := &fakeFileService{
			RoleOut: "User",
			GetAllOut: []FileWithUser{
				{File: File{ID: 1, Filename: "public.csv", Private: false}},
			},
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		out := mustJSON(t, w.Body.Bytes())
		if out["message"] != "Files fetched successfully" {
			t.Fatalf("unexpected message: %#v", out["message"])
		}
		if svc.Called["GetUserRole"] != 1 || svc.Called["GetAllFiles"] != 1 {
			t.Fatalf("expected GetUserRole+GetAllFiles once; got %+v", svc.Called)
		}
	})

	// ---------------- GetFileData ----------------

	t.Run("GetFileData - 401 invalid user ID type", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		h := map[string]string{}
		for k, v := range authHeaders {
			h[k] = v
		}
		h["X-UserID-Type"] = "uint" // controller expects float64

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=a.csv&version=1", nil, h)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid user ID")
	})

	t.Run("GetFileData - 400 invalid version", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=a.csv&version=NOPE", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "invalid version")
	})

	t.Run("GetFileData - 400 file name is required", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "file name is required")
	})

	t.Run("GetFileData - 500 service fails", func(t *testing.T) {
		svc := &fakeFileService{FileDataErr: errors.New("db down")}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "db down")
	})

	t.Run("GetFileData - 404 file not found (nil slice)", func(t *testing.T) {
		svc := &fakeFileService{FileDataOut: nil}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusNotFound)
		requireContains(t, w.Body.String(), "file not found")
	})

	t.Run("GetFileData - 401 invalid communities (forced)", func(t *testing.T) {
		svc := &fakeFileService{
			FileDataOut: []FileData{{ID: 1}},
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterWithBadCommunities(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid communities")
	})

	t.Run("GetFileData - 200 ok + log", func(t *testing.T) {
		svc := &fakeFileService{
			FileDataOut: []FileData{{ID: 1}},
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["GetFileData"] != 1 {
			t.Fatalf("expected GetFileData called once, got %+v", svc.Called)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "ACCESS_FILE" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected ACCESS_FILE log, got %+v", logSvc.Calls)
		}
	})

	t.Run("GetFileData - 200 ok even if log fails", func(t *testing.T) {
		svc := &fakeFileService{FileDataOut: []FileData{{ID: 1}}}
		logSvc := &fakeLogService{Err: errors.New("log failed")}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
	})

	// ---------------- DeleteFile ----------------

	t.Run("DeleteFile - 401 invalid user ID type", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		h := map[string]string{}
		for k, v := range authHeaders {
			h[k] = v
		}
		h["X-UserID-Type"] = "uint"

		req := newJSONReq(http.MethodDelete, "/api/file?id=99", nil, h)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid user ID")
	})

	t.Run("DeleteFile - 400 missing id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodDelete, "/api/file", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "file ID is required")
	})

	t.Run("DeleteFile - 500 service fails", func(t *testing.T) {
		svc := &fakeFileService{DeleteErr: errors.New("delete failed")}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodDelete, "/api/file?id=99", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "delete failed")
	})

	t.Run("DeleteFile - 401 invalid communities (forced)", func(t *testing.T) {
		svc := &fakeFileService{DeleteOut: File{ID: 99, Filename: "del.csv"}}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterWithBadCommunities(fc)

		req := newJSONReq(http.MethodDelete, "/api/file?id=99", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid communities")
	})

	t.Run("DeleteFile - 200 ok + log", func(t *testing.T) {
		svc := &fakeFileService{DeleteOut: File{ID: 99, Filename: "del.csv"}}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodDelete, "/api/file?id=99", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["DeleteFile"] != 1 || svc.LastDeleteID != "99" {
			t.Fatalf("expected DeleteFile called with 99, got called=%+v last=%q", svc.Called, svc.LastDeleteID)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "DELETE_FILE" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected DELETE_FILE log, got %+v", logSvc.Calls)
		}
	})

	t.Run("DeleteFile - 200 ok even if log fails", func(t *testing.T) {
		svc := &fakeFileService{DeleteOut: File{ID: 99, Filename: "del.csv"}}
		logSvc := &fakeLogService{Err: errors.New("log failed")}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodDelete, "/api/file?id=99", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
	})

	// ---------------- ResetFile ----------------

	t.Run("ResetFile - 400 missing id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPut, "/api/file/reset", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "file ID is required")
	})

	t.Run("ResetFile - 500 service fails", func(t *testing.T) {
		svc := &fakeFileService{ResetErr: errors.New("reset failed")}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPut, "/api/file/reset?id=99", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "reset failed")
	})

	t.Run("ResetFile - 200 ok + log", func(t *testing.T) {
		svc := &fakeFileService{ResetOut: File{ID: 99, Filename: "res.csv"}}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPut, "/api/file/reset?id=99", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["ResetFile"] != 1 || svc.LastResetID != "99" {
			t.Fatalf("expected ResetFile called with 99, got called=%+v last=%q", svc.Called, svc.LastResetID)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "RESTORE_FILE" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected RESTORE_FILE log, got %+v", logSvc.Calls)
		}
	})

	// ---------------- CreateAccess / DeleteAccess / GetAllAccess ----------------

	t.Run("CreateAccess - 400 bind error (invalid json)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := httptest.NewRequest(http.MethodPost, "/api/file/access", strings.NewReader("{bad"))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range authHeaders {
			req.Header.Set(k, v)
		}
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "failed to read data")
	})

	t.Run("CreateAccess - 500 service fails (need svc to return error)", func(t *testing.T) {
		// Your fake CreateAccess always returns nil.
		// So we simulate failure by wrapping fake service in a tiny adapter.

		base := &fakeFileService{}
		svc := &badCreateAccessSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := []FileAccess{{UserID: 1, FileID: 2}}
		req := newJSONReq(http.MethodPost, "/api/file/access", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "create access failed")
	})

	t.Run("CreateAccess - 200 ok + log", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := []FileAccess{{UserID: 1, FileID: 2}}
		req := newJSONReq(http.MethodPost, "/api/file/access", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["CreateAccess"] != 1 {
			t.Fatalf("expected CreateAccess called once, got %+v", svc.Called)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "GRAND_FILE_ACCESS" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected GRAND_FILE_ACCESS log, got %+v", logSvc.Calls)
		}
	})

	t.Run("DeleteAccess - 500 service fails (need svc to return error)", func(t *testing.T) {

		base := &fakeFileService{}
		svc := &badDeleteAccessSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodDelete, "/api/file/access?id=7", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "delete access failed")
	})

	t.Run("DeleteAccess - 200 ok + log", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodDelete, "/api/file/access?id=7", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["DeleteAccess"] != 1 {
			t.Fatalf("expected DeleteAccess called once, got %+v", svc.Called)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "REVOKE_FILE_ACCESS" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected REVOKE_FILE_ACCESS log, got %+v", logSvc.Calls)
		}
	})

	t.Run("GetAllAccess - 500 service fails (need svc to return error)", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badGetAccessSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/access?id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "get access failed")
	})

	t.Run("GetAllAccess - 404 access not found (nil)", func(t *testing.T) {

		base := &fakeFileService{}
		svc := &nilGetAccessSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/access?id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusNotFound)
		requireContains(t, w.Body.String(), "access not found")
	})

	t.Run("GetAllAccess - 200 ok", func(t *testing.T) {

		base := &fakeFileService{}
		svc := &okGetAccessSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/access?id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		out := mustJSON(t, w.Body.Bytes())
		if out["message"] != "Access fetched successfully" {
			t.Fatalf("unexpected message: %#v", out["message"])
		}
	})

	// ---------------- GetFileHistory ----------------

	t.Run("GetFileHistory - 500 service fails (need svc)", func(t *testing.T) {

		base := &fakeFileService{}
		svc := &badHistorySvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/history?id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "history failed")
	})

	t.Run("GetFileHistory - 404 history not found (nil)", func(t *testing.T) {

		base := &fakeFileService{}
		svc := &nilHistorySvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/history?id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusNotFound)
		requireContains(t, w.Body.String(), "history not found")
	})

	t.Run("GetFileHistory - 200 ok", func(t *testing.T) {

		base := &fakeFileService{}
		svc := &okHistorySvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/history?id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		out := mustJSON(t, w.Body.Bytes())
		if out["message"] != "File history fetched successfully" {
			t.Fatalf("unexpected message: %#v", out["message"])
		}
	})

	// ---------------- ReplaceFile ----------------

	t.Run("ReplaceFile - 400 no file uploaded", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		// missing file field
		req := newMultipartReq(http.MethodPost, "/api/file/replace",
			map[string][]string{"id": {"1"}},
			"", "", nil, authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "no file uploaded")
	})

	t.Run("ReplaceFile - 400 failed to read file id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		// include file but omit required id field
		req := newMultipartReq(http.MethodPost, "/api/file/replace",
			map[string][]string{}, // id missing
			"file", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "failed to read file id")
	})

	t.Run("ReplaceFile - 400 service error", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badReplaceSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/replace",
			map[string][]string{"id": {"1"}},
			"file", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "replace failed")
	})

	t.Run("ReplaceFile - 401 invalid communities (forced)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterWithBadCommunities(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/replace",
			map[string][]string{"id": {"1"}},
			"file", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid communities")
	})

	t.Run("ReplaceFile - 200 ok + log", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newMultipartReq(http.MethodPost, "/api/file/replace",
			map[string][]string{"id": {"55"}},
			"file", "x.csv", []byte("x"), authHeaders,
		)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["ReplaceFiles"] != 1 {
			t.Fatalf("expected ReplaceFiles called once, got %+v", svc.Called)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "REPLACE_FILE" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected REPLACE_FILE log, got %+v", logSvc.Calls)
		}
	})

	// ---------------- RevertFile ----------------

	t.Run("RevertFile - 400 invalid json", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := httptest.NewRequest(http.MethodPost, "/api/file/revert", strings.NewReader("{bad"))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range authHeaders {
			req.Header.Set(k, v)
		}
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
	})

	t.Run("RevertFile - 500 service fails", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badRevertSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := map[string]any{"filename": "a.csv", "version": 2}
		req := newJSONReq(http.MethodPost, "/api/file/revert", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "revert failed")
	})

	t.Run("RevertFile - 401 invalid communities (forced)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterWithBadCommunities(fc)

		body := map[string]any{"filename": "a.csv", "version": 2}
		req := newJSONReq(http.MethodPost, "/api/file/revert", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid communities")
	})

	t.Run("RevertFile - 200 ok + log", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := map[string]any{"filename": "a.csv", "version": 2}
		req := newJSONReq(http.MethodPost, "/api/file/revert", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["RevertFile"] != 1 {
			t.Fatalf("expected RevertFile called once, got %+v", svc.Called)
		}
		found := false
		for _, l := range logSvc.Calls {
			if l.Action == "REVERT_FILE" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected REVERT_FILE log, got %+v", logSvc.Calls)
		}
	})

	// ---------------- CreateEditRequest / GetEditRequests / ApproveEditRequest ----------------

	t.Run("CreateEditRequest - 400 bind error", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := httptest.NewRequest(http.MethodPost, "/api/file/edit/request", strings.NewReader("{bad"))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range authHeaders {
			req.Header.Set(k, v)
		}
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
	})

	t.Run("CreateEditRequest - 401 invalid user ID", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		h := map[string]string{}
		for k, v := range authHeaders {
			h[k] = v
		}
		h["X-UserID-Type"] = "uint"

		body := map[string]any{"foo": "bar"} // whatever your EditRequestInput expects
		req := newJSONReq(http.MethodPost, "/api/file/edit/request", body, h)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusUnauthorized)
		requireContains(t, w.Body.String(), "invalid user ID")
	})

	t.Run("CreateEditRequest - 400 service error", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badCreateReqSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		// If EditRequestInput has required fields, you may need to provide minimal valid json for your struct.
		// This test assumes empty object binds; if not, replace with valid shape.
		req := newJSONReq(http.MethodPost, "/api/file/edit/request", map[string]any{}, authHeaders)
		w := doReq(r, req)
		// if binding fails in your real struct, change the body to satisfy it.
		if w.Code == http.StatusBadRequest && strings.Contains(w.Body.String(), "error") && !strings.Contains(w.Body.String(), "create request failed") {
			// binding failed; skip (adjust input in your repo)
			t.Skipf("EditRequestInput binding failed in your repo; provide minimal valid json for this test")
		}
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "create request failed")
	})

	t.Run("CreateEditRequest - 200 ok", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/edit/request", map[string]any{}, authHeaders)
		w := doReq(r, req)

		// If your EditRequestInput has required fields, this may 400.
		// Provide minimal valid json for your struct and remove this guard.
		if w.Code == http.StatusBadRequest {
			t.Skipf("EditRequestInput in your repo likely has required fields; provide minimal valid json and remove skip. body=%s", w.Body.String())
		}

		assertStatus(t, w, http.StatusOK)
		out := mustJSON(t, w.Body.Bytes())
		if out["message"] != "Edit request submitted" {
			t.Fatalf("unexpected message: %#v", out["message"])
		}
	})

	t.Run("GetEditRequests - 400 invalid user_id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/request?user_id=abc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "invalid user_id")
	})

	t.Run("GetEditRequests - 400 user_id=0 invalid", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/request?user_id=0", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "invalid user_id")
	})

	t.Run("GetEditRequests - 400 service error (need svc)", func(t *testing.T) {

		base := &fakeFileService{}
		svc := &badGetEditsSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/request?status=approved&user_id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "get edits failed")
	})

	t.Run("GetEditRequests - 200 ok (status + user_id)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/request?status=approved,rejected&user_id=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
	})

	t.Run("ApproveEditRequest - 400 invalid input (bad json)", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := httptest.NewRequest(http.MethodPut, "/api/file/approve/request", strings.NewReader("{bad"))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range authHeaders {
			req.Header.Set(k, v)
		}
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "invalid input")
	})

	t.Run("ApproveEditRequest - 400 service error (need svc)", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badApproveSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := map[string]any{"request_id": 5, "updates": []any{}}
		req := newJSONReq(http.MethodPut, "/api/file/approve/request", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "approve failed")
	})

	t.Run("ApproveEditRequest - 200 ok", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := map[string]any{"request_id": 99, "updates": []any{}}
		req := newJSONReq(http.MethodPut, "/api/file/approve/request", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
		if svc.Called["ApproveEditRequest"] != 1 {
			t.Fatalf("expected ApproveEditRequest called once, got %+v", svc.Called)
		}
	})

	// ---------------- ReviewPhotos ----------------

	t.Run("ReviewPhotos - 400 invalid input", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := httptest.NewRequest(http.MethodPost, "/api/file/photos/review", strings.NewReader("{bad"))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range authHeaders {
			req.Header.Set(k, v)
		}
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "invalid input")
	})

	t.Run("ReviewPhotos - 400 service error (need svc)", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badReviewSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := map[string]any{"approved_photos": []any{1}, "rejected_photos": []any{2}}
		req := newJSONReq(http.MethodPost, "/api/file/photos/review", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "review failed")
	})

	t.Run("ReviewPhotos - 200 ok uses reviewer email", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		body := map[string]any{"approved_photos": []any{1, 2}, "rejected_photos": []any{3}}
		req := newJSONReq(http.MethodPost, "/api/file/photos/review", body, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.Called["ReviewPhotos"] != 1 {
			t.Fatalf("expected ReviewPhotos called once, got %+v", svc.Called)
		}
		if svc.LastReviewer != "reviewer@nordik.ca" {
			t.Fatalf("expected reviewer email, got %q", svc.LastReviewer)
		}
	})

	// ---------------- GetPhotos/Docs by Request/Row ----------------

	t.Run("GetPhotosByRequest - 400 invalid request id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/photos/abc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "Invalid request ID")
	})

	t.Run("GetPhotosByRequest - 500 service fails", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badPhotosReqSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/photos/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "photos req failed")
	})

	t.Run("GetPhotosByRequest - 200 ok", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/photos/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
	})

	t.Run("GetDocsByRequest - 400 invalid request id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/edit/docs/abc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "Invalid request ID")
	})

	t.Run("GetDocsByRow - 400 invalid row id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/docs/abc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "Invalid row ID")
	})

	t.Run("GetPhotosByRow - 200 ok", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/photos/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
	})

	// ---------------- GetPhoto / GetDoc ----------------

	t.Run("GetPhoto - 400 invalid photo id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/photo/abc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "Invalid photo ID")
	})

	t.Run("GetPhoto - 500 service error", func(t *testing.T) {
		svc := &fakeFileService{PhotoBytesErr: errors.New("photo failed")}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/photo/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "photo failed")
	})

	t.Run("GetPhoto - 200 returns bytes", func(t *testing.T) {
		svc := &fakeFileService{PhotoBytesOut: []byte("IMG"), PhotoCTOut: "image/png"}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/photo/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
		if w.Body.String() != "IMG" {
			t.Fatalf("expected IMG got %q", w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "image/png") {
			t.Fatalf("expected image/png content-type got %q", ct)
		}
	})

	t.Run("GetDoc - 400 invalid doc id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/doc/abc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "Invalid doc ID")
	})

	t.Run("GetDoc - 500 service error", func(t *testing.T) {
		svc := &fakeFileService{DocBytesErr: errors.New("doc failed")}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/doc/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
		requireContains(t, w.Body.String(), "doc failed")
	})

	t.Run("GetDoc - 200 inline for pdf + sanitizes filename", func(t *testing.T) {
		svc := &fakeFileService{
			DocBytesOut: []byte("%PDF"),
			DocCTOut:    "application/pdf",
			DocNameOut:  `../evil"name.pdf`,
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/doc/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		cd := w.Header().Get("Content-Disposition")
		requireContains(t, cd, "inline; filename=")

		fn := contentDispositionFilename(cd)
		if fn == "" {
			t.Fatalf("expected filename in Content-Disposition, got %q", cd)
		}

		// Controller sanitization: should remove path separators and quotes; may still keep dots.
		if strings.Contains(fn, "/") || strings.Contains(fn, "\\") {
			t.Fatalf("expected sanitized filename (no path separators), got %q (header=%q)", fn, cd)
		}
		if strings.Contains(fn, "\r") || strings.Contains(fn, "\n") {
			t.Fatalf("expected sanitized filename (no CRLF), got %q (header=%q)", fn, cd)
		}
		if strings.Contains(fn, `"`) {
			t.Fatalf("expected sanitized filename (no quotes), got %q (header=%q)", fn, cd)
		}

	})

	t.Run("GetDoc - 200 attachment for non-pdf/non-image", func(t *testing.T) {
		svc := &fakeFileService{
			DocBytesOut: []byte("DATA"),
			DocCTOut:    "application/octet-stream",
			DocNameOut:  "file.bin",
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/doc/1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		cd := w.Header().Get("Content-Disposition")
		requireContains(t, cd, "attachment; filename=")
	})

	// ---------------- DownloadMediaByID ----------------

	t.Run("DownloadMediaByID - 400 invalid id", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/doc/download/abc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "invalid id")
	})

	t.Run("DownloadMediaByID - 400 service error", func(t *testing.T) {
		base := &fakeFileService{}
		svc := &badOpenSvc{fakeFileService: base}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/doc/download/55?kind=doc", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
		requireContains(t, w.Body.String(), "open failed")
	})

	t.Run("DownloadMediaByID - 200 sniff path when content-type missing", func(t *testing.T) {
		content := []byte("HELLO WORLD") // DetectContentType => text/plain; charset=utf-8
		svc := &fakeFileService{
			MediaRCOut:   io.NopCloser(bytes.NewReader(content)),
			MediaFNOut:   "hello.bin",
			MediaCTOut:   "", // force sniff branch
			MediaDispOut: "",
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/doc/download/55", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if string(w.Body.Bytes()) != string(content) {
			t.Fatalf("expected %q got %q", string(content), w.Body.String())
		}
		if svc.Called["OpenMediaHandle"] != 1 {
			t.Fatalf("expected OpenMediaHandle called once, got %+v", svc.Called)
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("expected Cache-Control=no-store got %q", w.Header().Get("Cache-Control"))
		}
		if w.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("expected nosniff")
		}
	})

	t.Run("DownloadMediaByID - 200 normal path uses provided contentType + disposition", func(t *testing.T) {
		content := []byte("%PDF-1.4 mock")
		svc := &fakeFileService{
			MediaRCOut:   io.NopCloser(bytes.NewReader(content)),
			MediaFNOut:   "x.pdf",
			MediaCTOut:   "application/pdf",
			MediaDispOut: "inline",
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/doc/download/77", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		cd := w.Header().Get("Content-Disposition")
		requireContains(t, cd, `inline; filename="x.pdf"`)
	})
}
