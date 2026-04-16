package logocontent

import (
	"errors"
	"path"
	"testing"

	"gorm.io/gorm"
)

func TestLogoContentService_GetHTMLByFileID_NotFound(t *testing.T) {
	db := newTestDB(t)
	svc := &LogoContentService{DB: db}

	_, _, _, err := svc.GetHTMLByFileID(123)
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected gorm.ErrRecordNotFound, got %v", err)
	}
}

func TestLogoContentService_GetHTMLByFileID_InvalidGSURL(t *testing.T) {
	db := newTestDB(t)
	svc := &LogoContentService{DB: db}

	rec := FileLogoContent{
		FileID:   1,
		FileName: "coroner.csv",
		LogoURL:  "https://example.com/coroner.html",
	}
	if err := db.Create(&rec).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, _, _, err := svc.GetHTMLByFileID(1)
	if err == nil || err.Error() != "invalid gs url (must start with gs://): https://example.com/coroner.html" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogoContentService_GetHTMLByFileID_RejectsNonHTMLExtension(t *testing.T) {
	db := newTestDB(t)
	svc := &LogoContentService{DB: db}

	rec := FileLogoContent{
		FileID:   2,
		FileName: "coroner.csv",
		LogoURL:  "gs://bucket/logos_content/coroner.txt",
	}
	if err := db.Create(&rec).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, _, _, err := svc.GetHTMLByFileID(2)
	if err == nil || err.Error() != "logo url must point to an html file" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogoContentService_GetHTMLByFileID_RejectsNonHTMLContent(t *testing.T) {
	db := newTestDB(t)
	svc := &LogoContentService{DB: db}

	_, bucket := withFakeGCS(t)
	putGCSObject(t, bucket, "logos_content/coroner.html", []byte{0x89, 'P', 'N', 'G'}, "image/png")

	rec := FileLogoContent{
		FileID:   3,
		FileName: "coroner.csv",
		LogoURL:  "gs://" + bucket + "/logos_content/coroner.html",
	}
	if err := db.Create(&rec).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, _, _, err := svc.GetHTMLByFileID(3)
	if err == nil || err.Error() != "object is not html content" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogoContentService_GetHTMLByFileID_Success(t *testing.T) {
	db := newTestDB(t)
	svc := &LogoContentService{DB: db}

	_, bucket := withFakeGCS(t)
	objectPath := "logos_content/coroner.html"
	content := []byte("<html><body>Coroner</body></html>")
	putGCSObject(t, bucket, objectPath, content, "")

	rec := FileLogoContent{
		FileID:   4,
		FileName: "coroner.csv",
		LogoURL:  "gs://" + bucket + "/" + objectPath,
	}
	if err := db.Create(&rec).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	data, contentType, filename, err := svc.GetHTMLByFileID(4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("unexpected data: %q", string(data))
	}
	if contentType != "text/html; charset=utf-8" {
		t.Fatalf("unexpected content type: %q", contentType)
	}
	if filename != path.Base(objectPath) {
		t.Fatalf("expected filename %q, got %q", path.Base(objectPath), filename)
	}
}
