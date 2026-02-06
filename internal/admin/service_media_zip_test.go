package admin

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type fakeGCS struct {
	objects map[string]map[string]any
}

type fakeBucket struct {
	parent *fakeGCS
	bucket string
}
type fakeObj struct {
	parent *fakeGCS
	bucket string
	object string
}

func (c *fakeGCS) Bucket(name string) gcsBucket { return fakeBucket{parent: c, bucket: name} }
func (c *fakeGCS) Close() error                 { return nil }
func (b fakeBucket) Object(name string) gcsObject {
	return fakeObj{parent: b.parent, bucket: b.bucket, object: name}
}
func (o fakeObj) NewReader(ctx context.Context) (io.ReadCloser, error) {
	bm := o.parent.objects[o.bucket]
	if bm == nil {
		return nil, errors.New("bucket not found in fake")
	}
	v, ok := bm[o.object]
	if !ok {
		return nil, errors.New("object not found in fake: " + o.object)
	}
	switch x := v.(type) {
	case error:
		return nil, x
	case io.ReadCloser:
		return x, nil
	case []byte:
		return io.NopCloser(bytes.NewReader(x)), nil
	default:
		return nil, errors.New("bad fake type")
	}
}

type errReadCloser struct{}

func (e errReadCloser) Read(p []byte) (int, error) { return 0, errors.New("read fail") }
func (e errReadCloser) Close() error               { return nil }

func newMockDBForZip(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}

	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 sqlDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}

	cleanup := func() { _ = sqlDB.Close() }
	return gdb, mock, cleanup
}

func Test_StreamMediaZip_NoMatchingRequestsFound(t *testing.T) {
	as := &AdminService{}
	as.searchHook = func(req AdminFileEditSearchRequest) (*AdminSearchResponse, error) {
		return &AdminSearchResponse{Message: "success", Page: 1, PageSize: 200, TotalPages: 1, Data: []any{}}, nil
	}

	var buf bytes.Buffer
	err := as.StreamMediaZip(context.Background(), &buf, AdminDownloadMediaRequest{
		// empty request_ids => collectAllRequestIDs() => none => error
		DocumentType: "all",
	})
	if err == nil || !strings.Contains(err.Error(), "no matching requests found") {
		t.Fatalf("expected no matching requests found, got %v", err)
	}
}

func Test_StreamMediaZip_NoMediaFound(t *testing.T) {
	db, mock, cleanup := newMockDBForZip(t)
	defer cleanup()

	as := &AdminService{DB: db}

	mock.ExpectQuery(`(?i)file_edit_request_photos`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "request_id", "row_id", "photo_url", "file_name", "document_type", "document_category",
			"user_id", "user_first", "user_last",
		})) // no rows

	var buf bytes.Buffer
	err := as.StreamMediaZip(context.Background(), &buf, AdminDownloadMediaRequest{
		RequestIDs:   []uint{1},
		DocumentType: "all",
	})
	if err == nil || !strings.Contains(err.Error(), "no media found") {
		t.Fatalf("expected no media found, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func Test_StreamMediaZip_InvalidGSURL(t *testing.T) {
	// ✅ Prevent ADC / default credentials errors even if service uses storage.NewClient directly
	t.Setenv("STORAGE_EMULATOR_HOST", "http://127.0.0.1:1")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "test-project")

	db, mock, cleanup := newMockDBForZip(t)
	defer cleanup()

	as := &AdminService{DB: db}

	mock.ExpectQuery(`(?i)file_edit_request_photos`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "request_id", "row_id", "photo_url", "file_name", "document_type", "document_category",
			"user_id", "user_first", "user_last",
		}).AddRow(
			uint(1), uint(10), 5, "http://bad", "x.png", "photos", "",
			uint(7), "John", "Doe",
		))

	// ✅ If your service uses newGCSClientHook, avoid hitting real GCS client too
	old := newGCSClientHook
	newGCSClientHook = func(ctx context.Context) (gcsClient, error) {
		return &fakeGCS{objects: map[string]map[string]any{}}, nil
	}
	t.Cleanup(func() { newGCSClientHook = old })

	var buf bytes.Buffer
	err := as.StreamMediaZip(context.Background(), &buf, AdminDownloadMediaRequest{
		RequestIDs:   []uint{10},
		DocumentType: "all",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid gs url") {
		t.Fatalf("expected invalid gs url, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func Test_StreamMediaZip_MissingBucketAndEnv(t *testing.T) {
	db, mock, cleanup := newMockDBForZip(t)
	defer cleanup()

	as := &AdminService{DB: db}
	t.Setenv("BUCKET_NAME", "")

	mock.ExpectQuery(`(?i)file_edit_request_photos`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "request_id", "row_id", "photo_url", "file_name", "document_type", "document_category",
			"user_id", "user_first", "user_last",
		}).AddRow(
			uint(1), uint(10), 5, "gs:///obj.png", "", "photos", "",
			uint(7), "John", "Doe",
		))

	// avoid real GCS
	old := newGCSClientHook
	newGCSClientHook = func(ctx context.Context) (gcsClient, error) {
		return &fakeGCS{objects: map[string]map[string]any{}}, nil
	}
	t.Cleanup(func() { newGCSClientHook = old })

	var buf bytes.Buffer
	err := as.StreamMediaZip(context.Background(), &buf, AdminDownloadMediaRequest{
		RequestIDs:       []uint{10},
		DocumentType:     "all",
		CategorizeByUser: true,
		CategorizeByType: true,
	})
	if err == nil || !strings.Contains(err.Error(), "bucket name not found") {
		t.Fatalf("expected bucket name not found, got %v", err)
	}
}

func Test_StreamMediaZip_Success_ZipsTwoFiles(t *testing.T) {
	db, mock, cleanup := newMockDBForZip(t)
	defer cleanup()

	as := &AdminService{DB: db}
	t.Setenv("BUCKET_NAME", "mybucket")

	mock.ExpectQuery(`(?i)file_edit_request_photos`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "request_id", "row_id", "photo_url", "file_name", "document_type", "document_category",
			"user_id", "user_first", "user_last",
		}).AddRow(
			uint(2), uint(11), 3, "gs:///photos/a.jpg", "", "photos", "",
			uint(9), "Alice", "Z",
		).AddRow(
			uint(1), uint(10), 5, "gs:///docs/b.pdf", "", "document", "",
			uint(9), "Alice", "Z",
		))

	fake := &fakeGCS{
		objects: map[string]map[string]any{
			"mybucket": {
				path.Clean("photos/a.jpg"): []byte("AAA"),
				path.Clean("docs/b.pdf"):   []byte("BBB"),
			},
		},
	}
	old := newGCSClientHook
	newGCSClientHook = func(ctx context.Context) (gcsClient, error) { return fake, nil }
	t.Cleanup(func() { newGCSClientHook = old })

	var out bytes.Buffer
	err := as.StreamMediaZip(context.Background(), &out, AdminDownloadMediaRequest{
		RequestIDs:       []uint{10, 11},
		DocumentType:     "all",
		CategorizeByUser: true,
		CategorizeByType: true,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatalf("zip read: %v", err)
	}
	if len(zr.File) != 2 {
		t.Fatalf("expected 2 zip entries, got %d", len(zr.File))
	}

	names := []string{zr.File[0].Name, zr.File[1].Name}
	joined := strings.Join(names, " | ")
	if !strings.Contains(joined, "user_9_") || !strings.Contains(joined, "/photos/") || !strings.Contains(joined, "/documents/") {
		t.Fatalf("unexpected zip names: %v", names)
	}

	// content check
	got := map[string]string{}
	for _, f := range zr.File {
		rc, _ := f.Open()
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		got[f.Name] = string(b)
	}
	if !(strings.Contains(joined, "a.jpg") || strings.Contains(joined, "photos")) {
		// name contains req_..._a.jpg usually
	}
	// Ensure bytes exist
	foundAAA, foundBBB := false, false
	for _, v := range got {
		if v == "AAA" {
			foundAAA = true
		}
		if v == "BBB" {
			foundBBB = true
		}
	}
	if !foundAAA || !foundBBB {
		t.Fatalf("expected both AAA and BBB, got %#v", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func Test_StreamMediaZip_GCSReaderError(t *testing.T) {
	db, mock, cleanup := newMockDBForZip(t)
	defer cleanup()

	as := &AdminService{DB: db}
	t.Setenv("BUCKET_NAME", "mybucket")

	mock.ExpectQuery(`(?i)file_edit_request_photos`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "request_id", "row_id", "photo_url", "file_name", "document_type", "document_category",
			"user_id", "user_first", "user_last",
		}).AddRow(
			uint(1), uint(10), 5, "gs:///x.bin", "", "photos", "",
			uint(7), "John", "Doe",
		))

	fake := &fakeGCS{
		objects: map[string]map[string]any{
			"mybucket": {"x.bin": errors.New("newreader fail")},
		},
	}
	old := newGCSClientHook
	newGCSClientHook = func(ctx context.Context) (gcsClient, error) { return fake, nil }
	t.Cleanup(func() { newGCSClientHook = old })

	var out bytes.Buffer
	err := as.StreamMediaZip(context.Background(), &out, AdminDownloadMediaRequest{
		RequestIDs:   []uint{10},
		DocumentType: "all",
	})
	if err == nil || !strings.Contains(err.Error(), "newreader fail") {
		t.Fatalf("expected newreader fail, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func Test_StreamMediaZip_CopyError(t *testing.T) {
	db, mock, cleanup := newMockDBForZip(t)
	defer cleanup()

	as := &AdminService{DB: db}
	t.Setenv("BUCKET_NAME", "mybucket")

	mock.ExpectQuery(`(?i)file_edit_request_photos`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "request_id", "row_id", "photo_url", "file_name", "document_type", "document_category",
			"user_id", "user_first", "user_last",
		}).AddRow(
			uint(1), uint(10), 5, "gs:///badread.bin", "", "photos", "",
			uint(7), "John", "Doe",
		))

	fake := &fakeGCS{
		objects: map[string]map[string]any{
			"mybucket": {"badread.bin": errReadCloser{}},
		},
	}
	old := newGCSClientHook
	newGCSClientHook = func(ctx context.Context) (gcsClient, error) { return fake, nil }
	t.Cleanup(func() { newGCSClientHook = old })

	var out bytes.Buffer
	err := as.StreamMediaZip(context.Background(), &out, AdminDownloadMediaRequest{
		RequestIDs:   []uint{10},
		DocumentType: "all",
	})
	if err == nil || !strings.Contains(err.Error(), "read fail") {
		t.Fatalf("expected read fail, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}