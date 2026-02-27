package lookup

import (
	"fmt"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func strPtr(s string) *string {
	return &s
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	// Unique in-memory DB per test to avoid cross-test contamination
	dsn := fmt.Sprintf("file:%d?mode=memory&cache=shared", time.Now().UnixNano())

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if err := db.AutoMigrate(&Province{}, &DaySchool{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}

	return db
}

func TestLookupService_GetAllProvinces_Empty(t *testing.T) {
	db := newTestDB(t)
	svc := &LookupService{DB: db}

	got, err := svc.GetAllProvinces()
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

func TestLookupService_GetAllProvinces_ReturnsRows(t *testing.T) {
	db := newTestDB(t)
	svc := &LookupService{DB: db}

	seed := []Province{
		{Name: "Ontario"},
		{Name: "Alberta"},
		{Name: "British Columbia"},
	}
	for i := range seed {
		if err := db.Create(&seed[i]).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	got, err := svc.GetAllProvinces()
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d: %#v", len(got), got)
	}

	if got[0].Name != "Alberta" {
		t.Fatalf("expected first Alberta, got %q", got[0].Name)
	}
	if got[1].Name != "British Columbia" {
		t.Fatalf("expected second British Columbia, got %q", got[1].Name)
	}
	if got[2].Name != "Ontario" {
		t.Fatalf("expected third Ontario, got %q", got[2].Name)
	}
}

func TestLookupService_GetAllProvinces_DBBroken_ReturnsError(t *testing.T) {
	db := newTestDB(t)
	svc := &LookupService{DB: db}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	_, err = svc.GetAllProvinces()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestLookupService_GetDaySchoolsByProvince_Empty(t *testing.T) {
	db := newTestDB(t)
	svc := &LookupService{DB: db}

	got, err := svc.GetDaySchoolsByProvince(1)
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

func TestLookupService_GetDaySchoolsByProvince_ReturnsFilteredRows(t *testing.T) {
	db := newTestDB(t)
	svc := &LookupService{DB: db}

	p1 := Province{Name: "Ontario"}
	p2 := Province{Name: "Alberta"}

	if err := db.Create(&p1).Error; err != nil {
		t.Fatalf("seed province1: %v", err)
	}
	if err := db.Create(&p2).Error; err != nil {
		t.Fatalf("seed province2: %v", err)
	}

	seed := []DaySchool{
		{
			ProvinceID:           p1.ID,
			SchoolName:           "Zeta School",
			NameVariants:         strPtr("Zeta\nSchool"),
			OpeningDate:          strPtr("1901"),
			ClosingDate:          strPtr("1950"),
			Location:             strPtr("Ontario"),
			ReligiousAffiliation: strPtr("Catholic"),
		},
		{
			ProvinceID:           p1.ID,
			SchoolName:           "Alpha School",
			NameVariants:         strPtr("Alpha"),
			OpeningDate:          strPtr("1890"),
			ClosingDate:          strPtr("1940"),
			Location:             strPtr("Ontario"),
			ReligiousAffiliation: strPtr("Anglican"),
		},
		{
			ProvinceID:           p2.ID,
			SchoolName:           "Other Province School",
			NameVariants:         strPtr("Other"),
			OpeningDate:          strPtr("1910"),
			ClosingDate:          strPtr("1960"),
			Location:             strPtr("Alberta"),
			ReligiousAffiliation: strPtr("United"),
		},
	}

	for i := range seed {
		if err := db.Create(&seed[i]).Error; err != nil {
			t.Fatalf("seed day school: %v", err)
		}
	}

	got, err := svc.GetDaySchoolsByProvince(p1.ID)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d: %#v", len(got), got)
	}

	if got[0].SchoolName != "Alpha School" {
		t.Fatalf("expected first Alpha School, got %q", got[0].SchoolName)
	}
	if got[1].SchoolName != "Zeta School" {
		t.Fatalf("expected second Zeta School, got %q", got[1].SchoolName)
	}

	if got[1].NameVariants == nil || *got[1].NameVariants != "Zeta\nSchool" {
		t.Fatalf("expected multiline name_variants to be preserved, got %#v", got[1].NameVariants)
	}
}

func TestLookupService_GetDaySchoolsByProvince_DBBroken_ReturnsError(t *testing.T) {
	db := newTestDB(t)
	svc := &LookupService{DB: db}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()

	_, err = svc.GetDaySchoolsByProvince(1)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
