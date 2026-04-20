package file

import (
	"errors"
	"net/http"
	"testing"
)

func TestFileController_NormalizedEndpoints(t *testing.T) {
	authHeaders := map[string]string{
		"Authorization": "Bearer test",
		"X-UserID":      "1",
		"X-UserID-Type": "float64",
		"X-Communities": "Shingwauk,Algoma",
	}

	t.Run("GetNormalizedFileData - 404 when file missing", func(t *testing.T) {
		svc := &fakeFileService{NormalizedDataOut: nil}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data/normalized?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusNotFound)
	})

	t.Run("GetNormalizedFileData - 200 returns rows", func(t *testing.T) {
		svc := &fakeFileService{
			NormalizedDataOut: []FileDataNormalized{{ID: 1, SourceRowID: 10, CanonicalCommunity: "garden river"}},
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodGet, "/api/file/data/normalized?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
		if svc.Called["GetNormalizedFileData"] != 1 {
			t.Fatalf("expected GetNormalizedFileData called once, got %+v", svc.Called)
		}
	})

	t.Run("SyncNormalizedFileData - 400 invalid version", func(t *testing.T) {
		svc := &fakeFileService{}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/data/normalized/sync?filename=a.csv&version=bad", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusBadRequest)
	})

	t.Run("SyncNormalizedFileData - 500 on service error", func(t *testing.T) {
		svc := &fakeFileService{NormalizedSyncErr: errors.New("sync failed")}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/data/normalized/sync?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusInternalServerError)
	})

	t.Run("SyncNormalizedFileData - 200 returns sync result", func(t *testing.T) {
		svc := &fakeFileService{
			NormalizedSyncOut: &NormalizationSyncResult{Processed: 2, Inserted: 1, Updated: 1},
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/data/normalized/sync?filename=a.csv&version=1", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)
		if svc.Called["SyncNormalizedFileData"] != 1 {
			t.Fatalf("expected SyncNormalizedFileData called once, got %+v", svc.Called)
		}
	})

	t.Run("SyncNormalizedFileData - 200 supports global sync with latest-version priority", func(t *testing.T) {
		svc := &fakeFileService{
			NormalizedSyncOut: &NormalizationSyncResult{Processed: 5, Inserted: 5},
		}
		logSvc := &fakeLogService{}
		fc := &FileController{FileService: svc, LogService: logSvc}
		r := setupRouterForController(fc)

		req := newJSONReq(http.MethodPost, "/api/file/data/normalized/sync", nil, authHeaders)
		w := doReq(r, req)
		assertStatus(t, w, http.StatusOK)

		if svc.LastNormalizedSyncFilename != "" || svc.LastNormalizedSyncVersion != 0 {
			t.Fatalf("expected global sync call, got filename=%q version=%d", svc.LastNormalizedSyncFilename, svc.LastNormalizedSyncVersion)
		}
	})
}
