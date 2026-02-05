package community

import (
	"testing"
)

func TestCommunityService_GetAllCommunities_Empty(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}

	got, err := svc.GetAllCommunities()
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if got == nil {
		t.Fatalf("expected empty slice, got nil")
	}
	if len(got) != 0 {
		t.Fatalf("expected 0, got %d: %#v", len(got), got)
	}
}

func TestCommunityService_GetAllCommunities_ReturnsRows(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}

	seed := []Community{
		{Name: "c1", Approved: false},
		{Name: "c2", Approved: true},
	}
	for i := range seed {
		if err := db.Create(&seed[i]).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	got, err := svc.GetAllCommunities()
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d: %#v", len(got), got)
	}
}

func TestCommunityService_GetAllCommunities_DBBroken_ReturnsError(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	_, err = svc.GetAllCommunities()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestCommunityService_AddCommunities_OK_CreatesApprovedFalse(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}

	err := svc.AddCommunities([]string{"Alpha", "Beta"})
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}

	var rows []Community
	if err := db.Order("id asc").Find(&rows).Error; err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %#v", len(rows), rows)
	}
	if rows[0].Name != "Alpha" || rows[1].Name != "Beta" {
		t.Fatalf("unexpected names: %#v", rows)
	}
	for _, c := range rows {
		if c.Approved != false {
			t.Fatalf("expected Approved=false, got %#v", c)
		}
	}
}

func TestCommunityService_AddCommunities_DBBroken_ReturnsError(t *testing.T) {
	db := newTestDB(t)
	svc := &CommunityService{DB: db}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	err = svc.AddCommunities([]string{"Alpha"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
