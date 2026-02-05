package community

import (
	"net/http"
	"strings"
	"testing"
)

func TestCommunityController_GetAllCommunities_OK(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}
	r := setupCommunityRouter(svc)

	// seed
	if err := db.Create(&Community{Name: "c1", Approved: false}).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	w := getReq(r, "/api/communities")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var out map[string]any
	decodeJSON(t, w.Body.Bytes(), &out)

	if out["message"] != "Communities fetched successfully" {
		t.Fatalf("unexpected message: %v", out["message"])
	}
	comms, ok := out["communities"].([]any)
	if !ok {
		t.Fatalf("expected communities array, got: %#v", out["communities"])
	}
	if len(comms) != 1 {
		t.Fatalf("expected 1 community, got %d: %#v", len(comms), comms)
	}
}

func TestCommunityController_GetAllCommunities_InternalServerError_WhenDBClosed(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}
	r := setupCommunityRouter(svc)

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	w := getReq(r, "/api/communities")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCommunityController_AddCommunities_BadRequest_InvalidJSON(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}
	r := setupCommunityRouter(svc)

	w := postJSON(r, "/api/communities", []byte(`{"communities":`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCommunityController_AddCommunities_BadRequest_ValidationError(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}
	r := setupCommunityRouter(svc)

	// missing required "communities"
	w := postJSON(r, "/api/communities", []byte(`{}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCommunityController_AddCommunities_Created(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}
	r := setupCommunityRouter(svc)

	w := postJSON(r, "/api/communities", []byte(`{"communities":["X","Y"]}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	if !strings.Contains(w.Body.String(), "Communities added successfully") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}

	var rows []Community
	if err := db.Order("id asc").Find(&rows).Error; err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %#v", len(rows), rows)
	}
	if rows[0].Approved || rows[1].Approved {
		t.Fatalf("expected Approved=false for all, got %#v", rows)
	}
}

func TestCommunityController_AddCommunities_InternalServerError_WhenDBClosed(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}
	r := setupCommunityRouter(svc)

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	w := postJSON(r, "/api/communities", []byte(`{"communities":["X"]}`))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}
