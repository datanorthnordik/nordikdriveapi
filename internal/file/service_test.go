// internal/file/service_test.go
package file

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/glebarez/sqlite"
	"github.com/xuri/excelize/v2"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"nordik-drive-api/internal/util"
)

// -----------------------------------------------------------------------------
// Test DB helpers (sqlite in-memory, isolated per test)
// -----------------------------------------------------------------------------

var testDBSeq uint64

// --- Add this near the top of service_test.go ---

// UserForTest creates the exact "users" table your FileService queries/join against.
type UserForTest struct {
	ID        uint   `gorm:"primaryKey;column:id"`
	Email     string `gorm:"column:email"`
	Role      string `gorm:"column:role"`
	FirstName string `gorm:"column:firstname"`
	LastName  string `gorm:"column:lastname"`
}

func (UserForTest) TableName() string { return "users" }

func migrateTestSchema(t *testing.T, db *gorm.DB) {
	t.Helper()

	// optional but nice for sqlite tests
	_ = db.Exec("PRAGMA foreign_keys = ON").Error

	// IMPORTANT: migrate ALL tables your service touches
	if err := db.AutoMigrate(
		&File{},
		&FileVersion{},
		&FileData{},
		&FileAccess{},
		&FileEditRequest{},
		&FileEditRequestDetails{},
		&FileEditRequestPhoto{},
		&UserForTest{}, // creates "users"
	); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	id := atomic.AddUint64(&testDBSeq, 1)
	dsn := fmt.Sprintf("file:nordik_test_%d?mode=memory&cache=shared", id)

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	// ✅ CREATE TABLES HERE
	migrateTestSchema(t, db)

	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func breakDB(t *testing.T, db *gorm.DB) {
	t.Helper()
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	_ = sqlDB.Close()
}

// -----------------------------------------------------------------------------
// Multipart helpers
// -----------------------------------------------------------------------------

func fileHeaderFromBytes(t *testing.T, formField, filename string, content []byte) *multipart.FileHeader {
	t.Helper()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile(formField, filename)
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	_, _ = fw.Write(content)
	_ = w.Close()

	r := multipart.NewReader(bytes.NewReader(buf.Bytes()), w.Boundary())
	form, err := r.ReadForm(32 << 20)
	if err != nil {
		t.Fatalf("ReadForm: %v", err)
	}

	fhs := form.File[formField]
	if len(fhs) != 1 {
		t.Fatalf("expected 1 file header, got %d", len(fhs))
	}
	return fhs[0]
}

func csvBytes(headers []string, rows [][]string) []byte {
	var b strings.Builder
	b.WriteString(strings.Join(headers, ","))
	b.WriteString("\n")
	for _, r := range rows {
		b.WriteString(strings.Join(r, ","))
		b.WriteString("\n")
	}
	return []byte(b.String())
}

func xlsxBytesWithStyles(t *testing.T) []byte {
	t.Helper()

	f := excelize.NewFile()
	sheet := f.GetSheetName(0)

	// headers
	_ = f.SetCellValue(sheet, "A1", "Name")
	_ = f.SetCellValue(sheet, "B1", "Age")

	// data
	_ = f.SetCellValue(sheet, "A2", "John")
	_ = f.SetCellValue(sheet, "B2", "30")

	// style with fill color that maps in parseExcelReader:
	// "#FFFF00" => "FURTHER INVESTIGATION REQUIRED"
	styleID, err := f.NewStyle(&excelize.Style{
		Fill: excelize.Fill{
			Type:    "pattern",
			Pattern: 1,
			Color:   []string{"FFFF00"},
		},
	})
	if err != nil {
		t.Fatalf("NewStyle: %v", err)
	}

	// Apply style to A2 (has value => should append)
	if err := f.SetCellStyle(sheet, "A2", "A2", styleID); err != nil {
		t.Fatalf("SetCellStyle: %v", err)
	}

	// Apply style to B2 but set empty value (no append)
	_ = f.SetCellValue(sheet, "B2", "")
	if err := f.SetCellStyle(sheet, "B2", "B2", styleID); err != nil {
		t.Fatalf("SetCellStyle: %v", err)
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	return buf.Bytes()
}

// -----------------------------------------------------------------------------
// Fake GCS server helper (covers GetPhotoBytes/GetDocBytes/OpenMediaHandle/readFromGCS)
// -----------------------------------------------------------------------------

func withFakeGCS(t *testing.T) (*fakestorage.Server, string) {
	t.Helper()

	srv, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		Scheme: "http",
	})
	if err != nil {
		t.Fatalf("failed to start fake gcs: %v", err)
	}
	t.Cleanup(srv.Stop)

	bucket := "test-bucket"
	srv.CreateBucket(bucket)

	_ = os.Setenv("BUCKET_NAME", bucket)

	// ✅ CRITICAL: use srv.Client() so NewReader hits supported endpoints
	prev := newGCSClientHook
	newGCSClientHook = func(ctx context.Context) (*storage.Client, error) {
		return srv.Client(), nil
	}
	t.Cleanup(func() { newGCSClientHook = prev })

	return srv, bucket
}

// setFieldIfExists sets a struct field by name if it exists and is settable.
// It also tolerates slightly different fake-gcs-server versions (field names changed across versions).
func setFieldIfExists(rv reflect.Value, field string, val any) {
	f := rv.FieldByName(field)
	if !f.IsValid() || !f.CanSet() {
		return
	}

	v := reflect.ValueOf(val)

	// Handle []byte assignment into []uint8
	if f.Kind() == reflect.Slice && f.Type().Elem().Kind() == reflect.Uint8 && v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		f.SetBytes(v.Bytes())
		return
	}

	// Direct assign if types match or convertible
	if v.Type().AssignableTo(f.Type()) {
		f.Set(v)
		return
	}
	if v.Type().ConvertibleTo(f.Type()) {
		f.Set(v.Convert(f.Type()))
		return
	}
}

func putGCSObject(t *testing.T, srv *fakestorage.Server, bucket, name string, content []byte, contentType string) {
	t.Helper()
	_ = srv // keep signature

	ctx := context.Background()

	// ✅ IMPORTANT: use the same hook the service uses
	client, err := newGCSClientHook(ctx)
	if err != nil {
		t.Fatalf("newGCSClientHook: %v", err)
	}
	defer client.Close()

	w := client.Bucket(bucket).Object(name).NewWriter(ctx)
	if contentType != "" {
		w.ContentType = contentType
	}

	if _, err := w.Write(content); err != nil {
		_ = w.Close()
		t.Fatalf("write object %s/%s: %v", bucket, name, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer %s/%s: %v", bucket, name, err)
	}
}

// -----------------------------------------------------------------------------
// Unsafe helper to force *storage.Reader internal reader so we can fully cover ioReadAll
// storage.Reader has unexported fields; we set them via reflect+unsafe.
// -----------------------------------------------------------------------------

type errReadCloser struct {
	readFn  func([]byte) (int, error)
	closeFn func() error
}

func (e *errReadCloser) Read(p []byte) (int, error) { return e.readFn(p) }
func (e *errReadCloser) Close() error {
	if e.closeFn != nil {
		return e.closeFn()
	}
	return nil
}

func forceStorageReaderInner(t *testing.T, sr *storage.Reader, inner io.ReadCloser) {
	t.Helper()

	rv := reflect.ValueOf(sr).Elem()

	// Set ctx to a valid context to avoid nil issues in some versions
	if f := rv.FieldByName("ctx"); f.IsValid() {
		setUnexported(t, f, reflect.ValueOf(context.Background()))
	}

	// Prefer field named "reader"
	if f := rv.FieldByName("reader"); f.IsValid() {
		setUnexported(t, f, reflect.ValueOf(inner))
		return
	}

	// Fallback: set first io.ReadCloser field
	rcType := reflect.TypeOf((*io.ReadCloser)(nil)).Elem()
	for i := 0; i < rv.NumField(); i++ {
		ft := rv.Type().Field(i).Type
		if ft == rcType {
			setUnexported(t, rv.Field(i), reflect.ValueOf(inner))
			return
		}
	}

	t.Fatalf("could not find storage.Reader inner reader field")
}

func setUnexported(t *testing.T, field reflect.Value, val reflect.Value) {
	t.Helper()
	if !field.IsValid() {
		t.Fatalf("invalid field")
	}
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(val)
}

// -----------------------------------------------------------------------------
// normalizeColorHex / parseStatuses / parseGSURL tests
// -----------------------------------------------------------------------------

func TestNormalizeColorHex_AllBranches(t *testing.T) {
	if got := normalizeColorHex(""); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
	if got := normalizeColorHex("0xFFffff00"); got != "#FFFF00" {
		t.Fatalf("ARGB drop alpha expected #FFFF00, got %q", got)
	}
	if got := normalizeColorHex("#abc"); got != "#AABBCC" {
		t.Fatalf("3-digit expected #AABBCC, got %q", got)
	}
	if got := normalizeColorHex("12"); got != "#120000" {
		t.Fatalf("pad expected #120000, got %q", got)
	}
	if got := normalizeColorHex("A1B2C3D4E5"); got != "#A1B2C3" {
		t.Fatalf("trim expected #A1B2C3, got %q", got)
	}
	if got := normalizeColorHex("ffffff"); got != "#FFFFFF" {
		t.Fatalf("6-digit expected #FFFFFF, got %q", got)
	}
}

func TestParseStatuses_AllBranches(t *testing.T) {
	if got := parseStatuses("   "); got != nil {
		t.Fatalf("expected nil, got %#v", got)
	}
	got := parseStatuses(" Pending, approved, pending,  ,REJECTED ")
	if len(got) != 3 || got[0] != "pending" || got[1] != "approved" || got[2] != "rejected" {
		t.Fatalf("unexpected: %#v", got)
	}
	if got := parseStatuses(",,,   ,"); got != nil {
		t.Fatalf("expected nil for garbage, got %#v", got)
	}
}

func TestParseGSURL_AllBranches(t *testing.T) {
	if _, _, err := parseGSURL(""); err == nil {
		t.Fatalf("expected error")
	}
	if _, _, err := parseGSURL("http://x"); err == nil {
		t.Fatalf("expected error")
	}
	if _, _, err := parseGSURL("gs://bucketonly"); err == nil {
		t.Fatalf("expected error")
	}
	if _, _, err := parseGSURL("gs://bucket/"); err == nil {
		t.Fatalf("expected error")
	}

	// bucket may be empty but object path must exist (covers bucket-from-env branch in OpenMediaHandle/readFromGCS)
	b, obj, err := parseGSURL("gs:///obj.txt")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if b != "" || obj != "obj.txt" {
		t.Fatalf("unexpected parsed: bucket=%q obj=%q", b, obj)
	}

	b, obj, err = parseGSURL("gs://myb/folder/a.txt")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if b != "myb" || obj != "folder/a.txt" {
		t.Fatalf("unexpected parsed: bucket=%q obj=%q", b, obj)
	}
}

// -----------------------------------------------------------------------------
// parseCSVReader / parseExcelReader
// -----------------------------------------------------------------------------

func TestParseCSVReader_EmptyAndInvalidAndOK(t *testing.T) {
	// empty => error
	fh := fileHeaderFromBytes(t, "file", "a.csv", []byte(""))
	f, _ := fh.Open()
	defer f.Close()
	if _, _, err := parseCSVReader(f); err == nil {
		t.Fatalf("expected error for empty csv")
	}

	// invalid => error
	bad := []byte(`a,b` + "\n" + `"unterminated`)
	fh = fileHeaderFromBytes(t, "file", "b.csv", bad)
	f, _ = fh.Open()
	defer f.Close()
	if _, _, err := parseCSVReader(f); err == nil {
		t.Fatalf("expected error for invalid csv")
	}

	// ok
	ok := csvBytes([]string{"h1", "h2"}, [][]string{{"v1", "v2"}})
	fh = fileHeaderFromBytes(t, "file", "c.csv", ok)
	f, _ = fh.Open()
	defer f.Close()
	h, rows, err := parseCSVReader(f)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(h) != 2 || h[0] != "h1" || len(rows) != 1 || rows[0][0] != "v1" {
		t.Fatalf("unexpected parse: headers=%v rows=%v", h, rows)
	}
}

func TestParseExcelReader_InvalidAndOK_WithColorAppend(t *testing.T) {
	// invalid bytes => parse error
	fh := fileHeaderFromBytes(t, "file", "x.xlsx", []byte("notxlsx"))
	f, _ := fh.Open()
	defer f.Close()
	if _, _, err := parseExcelReader(f); err == nil {
		t.Fatalf("expected error")
	}

	// ok with style mapping
	xbytes := xlsxBytesWithStyles(t)
	fh = fileHeaderFromBytes(t, "file", "ok.xlsx", xbytes)
	f, _ = fh.Open()
	defer f.Close()

	headers, data, err := parseExcelReader(f)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(headers) != 2 || headers[0] != "Name" || headers[1] != "Age" {
		t.Fatalf("unexpected headers: %#v", headers)
	}
	if len(data) != 1 {
		t.Fatalf("expected 1 data row, got %d", len(data))
	}
	if !strings.Contains(data[0][0], "FURTHER INVESTIGATION REQUIRED") {
		t.Fatalf("expected appended source, got %q", data[0][0])
	}
	// B2 was empty, so no append
	if data[0][1] != "" {
		t.Fatalf("expected empty age, got %q", data[0][1])
	}
}

// -----------------------------------------------------------------------------
// SaveFilesMultipart
// -----------------------------------------------------------------------------

func TestFileService_SaveFilesMultipart_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// duplicate filename
	if err := db.Create(&File{Filename: "dup"}).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	fh := fileHeaderFromBytes(t, "file", "dup.csv", csvBytes([]string{"a"}, [][]string{{"1"}}))
	_, err := svc.SaveFilesMultipart([]*multipart.FileHeader{fh}, FileUploadInput{
		FileNames:       []string{"dup"},
		Private:         []bool{false},
		CommunityFilter: []bool{true},
	}, 1)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected duplicate error, got %v", err)
	}

	// unsupported extension
	fh = fileHeaderFromBytes(t, "file", "x.txt", []byte("hi"))
	_, err = svc.SaveFilesMultipart([]*multipart.FileHeader{fh}, FileUploadInput{
		FileNames:       []string{"x"},
		Private:         []bool{false},
		CommunityFilter: []bool{true},
	}, 1)
	if err == nil || !strings.Contains(err.Error(), "unsupported file type") {
		t.Fatalf("expected unsupported error, got %v", err)
	}

	// CSV parse error
	fh = fileHeaderFromBytes(t, "file", "bad.csv", []byte(`h1`+"\n"+`"unterminated`))
	_, err = svc.SaveFilesMultipart([]*multipart.FileHeader{fh}, FileUploadInput{
		FileNames:       []string{"bad"},
		Private:         []bool{false},
		CommunityFilter: []bool{true},
	}, 1)
	if err == nil {
		t.Fatalf("expected csv parse err")
	}

	// Excel parse error
	fh = fileHeaderFromBytes(t, "file", "bad.xlsx", []byte("notxlsx"))
	_, err = svc.SaveFilesMultipart([]*multipart.FileHeader{fh}, FileUploadInput{
		FileNames:       []string{"badx"},
		Private:         []bool{false},
		CommunityFilter: []bool{true},
	}, 1)
	if err == nil {
		t.Fatalf("expected excel parse err")
	}

	// success CSV (also covers: missing cells => empty string)
	fh = fileHeaderFromBytes(t, "file", "ok.csv", csvBytes(
		[]string{"c1", "c2", "c3"},
		[][]string{
			{"a", "b", ""},
			{"x", "y", "z"},
		},
	))

	out, err := svc.SaveFilesMultipart([]*multipart.FileHeader{fh}, FileUploadInput{
		FileNames:       []string{"okfile"},
		Private:         []bool{true},
		CommunityFilter: []bool{true},
	}, 99)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 1 || out[0].Filename != "okfile" || out[0].Version != 1 || out[0].Rows != 2 || out[0].InsertedBy != 99 {
		t.Fatalf("unexpected file out: %#v", out)
	}

	// verify file_version created
	var vers []FileVersion
	if err := db.Where("file_id = ?", out[0].ID).Find(&vers).Error; err != nil {
		t.Fatalf("fetch versions: %v", err)
	}
	if len(vers) != 1 || vers[0].Version != 1 {
		t.Fatalf("unexpected versions: %#v", vers)
	}

	// verify file_data created and has c3 filled for first row
	var rows []FileData
	if err := db.Where("file_id = ?", out[0].ID).Order("id asc").Find(&rows).Error; err != nil {
		t.Fatalf("fetch data: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	var m map[string]string
	_ = json.Unmarshal(rows[0].RowData, &m)
	if m["c3"] != "" || m["c1"] != "a" || m["c2"] != "b" {
		t.Fatalf("unexpected row map: %#v", m)
	}
}

// -----------------------------------------------------------------------------
// ReplaceFiles
// -----------------------------------------------------------------------------

func TestFileService_ReplaceFiles_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// not found
	fh := fileHeaderFromBytes(t, "file", "x.csv", csvBytes([]string{"a"}, [][]string{{"1"}}))
	if err := svc.ReplaceFiles(fh, 999999, 1); err == nil {
		t.Fatalf("expected not found")
	}

	// seed file
	f := File{Filename: "f1", Version: 1, Private: false, IsDelete: false}
	if err := db.Create(&f).Error; err != nil {
		t.Fatalf("seed file: %v", err)
	}

	// unsupported extension
	fh = fileHeaderFromBytes(t, "file", "x.txt", []byte("hi"))
	if err := svc.ReplaceFiles(fh, f.ID, 1); err == nil {
		t.Fatalf("expected unsupported")
	}

	// parse error
	fh = fileHeaderFromBytes(t, "file", "bad.csv", []byte(`h1`+"\n"+`"unterminated`))
	if err := svc.ReplaceFiles(fh, f.ID, 1); err == nil {
		t.Fatalf("expected parse error")
	}

	// force Save error by dropping table
	fh = fileHeaderFromBytes(t, "file", "ok.csv", csvBytes([]string{"h1"}, [][]string{{"v1"}}))
	if err := db.Migrator().DropTable(&File{}); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if err := svc.ReplaceFiles(fh, f.ID, 1); err == nil {
		t.Fatalf("expected save error")
	}

	// re-migrate and re-seed for success + other create errors
	db = newTestDB(t)
	svc = &FileService{DB: db}
	f = File{Filename: "f1", Version: 1, Private: false, IsDelete: false}
	_ = db.Create(&f).Error

	// Create file_version error
	fh = fileHeaderFromBytes(t, "file", "ok.csv", csvBytes([]string{"h1"}, [][]string{{"v1"}}))
	if err := db.Migrator().DropTable(&FileVersion{}); err != nil {
		t.Fatalf("drop file_version: %v", err)
	}
	if err := svc.ReplaceFiles(fh, f.ID, 2); err == nil {
		t.Fatalf("expected file_version insert error")
	}

	// re-migrate and test FileData insert error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	f = File{Filename: "f1", Version: 1, Private: false, IsDelete: false}
	_ = db.Create(&f).Error

	if err := db.Migrator().DropTable(&FileData{}); err != nil {
		t.Fatalf("drop file_data: %v", err)
	}
	fh = fileHeaderFromBytes(t, "file", "ok.csv", csvBytes([]string{"h1"}, [][]string{{"v1"}}))
	if err := svc.ReplaceFiles(fh, f.ID, 2); err == nil {
		t.Fatalf("expected file_data insert error")
	}

	// success
	db = newTestDB(t)
	svc = &FileService{DB: db}
	f = File{Filename: "f1", Version: 1, Private: true, IsDelete: false}
	_ = db.Create(&f).Error

	fh = fileHeaderFromBytes(t, "file", "ok.csv", csvBytes([]string{"h1", "h2"}, [][]string{{"v1", ""}}))
	if err := svc.ReplaceFiles(fh, f.ID, 77); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	var updated File
	if err := db.First(&updated, f.ID).Error; err != nil {
		t.Fatalf("fetch updated: %v", err)
	}
	if updated.Version != 2 || updated.Rows != 1 {
		t.Fatalf("unexpected updated file: %#v", updated)
	}

	var v []FileVersion
	_ = db.Where("file_id = ?", f.ID).Find(&v).Error
	if len(v) != 1 || v[0].Version != 2 || v[0].InsertedBy != 77 {
		t.Fatalf("unexpected version rows: %#v", v)
	}
}

// -----------------------------------------------------------------------------
// GetUserRole / GetAllFiles
// -----------------------------------------------------------------------------

func TestFileService_GetUserRole_OK_NotFound_DBBroken(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	u := UserForTest{Email: "a@b.com", Role: "Admin", FirstName: "A", LastName: "B"}
	if err := db.Create(&u).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	role, err := svc.GetUserRole(uint(u.ID))
	if err != nil || role != "Admin" {
		t.Fatalf("expected Admin, got %q err=%v", role, err)
	}

	_, err = svc.GetUserRole(999999)
	if err == nil {
		t.Fatalf("expected not found err")
	}

	breakDB(t, db)
	_, err = svc.GetUserRole(uint(u.ID))
	if err == nil {
		t.Fatalf("expected db error")
	}
}

func TestFileService_GetAllFiles_AdminAndUserAndDBError(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	u1 := UserForTest{Email: "u1@b.com", Role: "User", FirstName: "U1", LastName: "L1"}
	u2 := UserForTest{Email: "u2@b.com", Role: "User", FirstName: "U2", LastName: "L2"}

	_ = db.Create(&u1).Error
	_ = db.Create(&u2).Error

	// public file by u1
	fPub := File{Filename: "pub", InsertedBy: uint(u1.ID), Private: false, IsDelete: false}
	// private file by u1
	fPriv := File{Filename: "priv", InsertedBy: uint(u1.ID), Private: true, IsDelete: false}
	// deleted private with access should NOT show for user
	fPrivDel := File{Filename: "privdel", InsertedBy: uint(u1.ID), Private: true, IsDelete: true}
	_ = db.Create(&fPub).Error
	_ = db.Create(&fPriv).Error
	_ = db.Create(&fPrivDel).Error

	// grant access for u2 to priv + privdel
	_ = db.Create(&FileAccess{UserID: uint(u2.ID), FileID: fPriv.ID}).Error
	_ = db.Create(&FileAccess{UserID: uint(u2.ID), FileID: fPrivDel.ID}).Error

	// Admin -> all (3 files)
	all, err := svc.GetAllFiles(uint(u2.ID), "Admin")
	if err != nil || len(all) != 3 {
		t.Fatalf("admin expected 3, got %d err=%v %#v", len(all), err, all)
	}

	// User -> public + private with access AND not deleted
	userFiles, err := svc.GetAllFiles(uint(u2.ID), "User")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// should include pub + priv (NOT privdel)
	if len(userFiles) != 2 {
		t.Fatalf("expected 2, got %d: %#v", len(userFiles), userFiles)
	}

	breakDB(t, db)
	_, err = svc.GetAllFiles(uint(u2.ID), "Admin")
	if err == nil {
		t.Fatalf("expected db err")
	}
}

// -----------------------------------------------------------------------------
// GetFileData
// -----------------------------------------------------------------------------

func TestFileService_GetFileData_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// not found => nil,nil
	got, err := svc.GetFileData("missing", 1)
	if err != nil || got != nil {
		t.Fatalf("expected nil,nil got=%v err=%v", got, err)
	}

	// create file with invalid ColumnsOrder => unmarshal error
	f := File{Filename: "f1", IsDelete: false, ColumnsOrder: []byte(`not-json`)}
	_ = db.Create(&f).Error
	_, err = svc.GetFileData("f1", 1)
	if err == nil || !strings.Contains(err.Error(), "unmarshal columns order") {
		t.Fatalf("expected columns order error, got %v", err)
	}

	// valid columns order + invalid row json => row unmarshal error
	cols, _ := json.Marshal([]string{"b", "a", "c"})
	f2 := File{Filename: "f2", IsDelete: false, ColumnsOrder: cols}
	_ = db.Create(&f2).Error
	_ = db.Create(&FileData{
		FileID:  f2.ID,
		RowData: datatypes.JSON([]byte(`{bad json`)),
		Version: 1,
	}).Error
	_, err = svc.GetFileData("f2", 1)
	if err == nil || !strings.Contains(err.Error(), "unmarshal row") {
		t.Fatalf("expected row unmarshal error, got %v", err)
	}

	// success reorder + fill missing columns
	f3 := File{Filename: "f3", IsDelete: false, ColumnsOrder: cols}
	_ = db.Create(&f3).Error
	// row has keys out-of-order + missing "c"
	_ = db.Create(&FileData{
		FileID:  f3.ID,
		RowData: datatypes.JSON([]byte(`{"a":"1","b":"2"}`)),
		Version: 5,
	}).Error

	rows, err := svc.GetFileData("f3", 5)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var ordered map[string]any
	_ = json.Unmarshal(rows[0].RowData, &ordered)
	if ordered["b"] != "2" || ordered["a"] != "1" || ordered["c"] != "" {
		t.Fatalf("unexpected ordered row: %#v", ordered)
	}

	// Find error path: drop file_data table after file exists + columns ok
	if err := db.Migrator().DropTable(&FileData{}); err != nil {
		t.Fatalf("drop file_data: %v", err)
	}
	_, err = svc.GetFileData("f3", 5)
	if err == nil {
		t.Fatalf("expected find error")
	}
}

// -----------------------------------------------------------------------------
// DeleteFile / ResetFile
// -----------------------------------------------------------------------------

func TestFileService_DeleteAndReset_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// not found
	if _, err := svc.DeleteFile("999"); err == nil {
		t.Fatalf("expected err")
	}
	if _, err := svc.ResetFile("999"); err == nil {
		t.Fatalf("expected err")
	}

	f := File{Filename: "f1", IsDelete: false}
	_ = db.Create(&f).Error

	// update error by dropping table
	_ = db.Migrator().DropTable(&File{})
	if _, err := svc.DeleteFile(fmt.Sprintf("%d", f.ID)); err == nil {
		t.Fatalf("expected update err")
	}

	// success delete/reset
	db = newTestDB(t)
	svc = &FileService{DB: db}
	f = File{Filename: "f2", IsDelete: false}
	_ = db.Create(&f).Error

	_, err := svc.DeleteFile(fmt.Sprintf("%d", f.ID))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	var ff File
	_ = db.First(&ff, f.ID).Error
	if ff.IsDelete != true {
		t.Fatalf("expected deleted=true")
	}

	_, err = svc.ResetFile(fmt.Sprintf("%d", f.ID))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	_ = db.First(&ff, f.ID).Error
	if ff.IsDelete != false {
		t.Fatalf("expected deleted=false")
	}
}

// -----------------------------------------------------------------------------
// Access CRUD + history
// -----------------------------------------------------------------------------

func TestFileService_AccessCRUD_AndHistory(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	u := UserForTest{Email: "u@b.com", Role: "User", FirstName: "U", LastName: "L"}

	_ = db.Create(&u).Error

	f := File{Filename: "f1", InsertedBy: uint(u.ID)}
	_ = db.Create(&f).Error

	// CreateAccess ok
	if err := svc.CreateAccess([]FileAccess{{UserID: uint(u.ID), FileID: f.ID}}); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	// GetFileAccess ok
	out, err := svc.GetFileAccess(fmt.Sprintf("%d", f.ID))
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, got %d err=%v %#v", len(out), err, out)
	}

	// DeleteAccess not found
	if err := svc.DeleteAccess("999"); err == nil {
		t.Fatalf("expected err")
	}

	// DeleteAccess ok
	var acc FileAccess
	_ = db.Where("file_id = ? AND user_id = ?", f.ID, u.ID).First(&acc).Error
	if err := svc.DeleteAccess(fmt.Sprintf("%d", acc.ID)); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	var count int64
	_ = db.Model(&FileAccess{}).Where("id = ?", acc.ID).Count(&count).Error
	if count != 0 {
		t.Fatalf("expected deleted access row")
	}

	// DeleteAccess delete error: find then drop table
	_ = db.Create(&FileAccess{UserID: uint(u.ID), FileID: f.ID}).Error
	_ = db.Where("file_id = ? AND user_id = ?", f.ID, u.ID).First(&acc).Error
	_ = db.Migrator().DropTable(&FileAccess{})
	if err := svc.DeleteAccess(fmt.Sprintf("%d", acc.ID)); err == nil {
		t.Fatalf("expected delete error")
	}

	// CreateAccess error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	_ = db.Migrator().DropTable(&FileAccess{})
	if err := svc.CreateAccess([]FileAccess{{UserID: 1, FileID: 1}}); err == nil {
		t.Fatalf("expected create error")
	}

	// history
	db = newTestDB(t)
	svc = &FileService{DB: db}
	u = UserForTest{Email: "u2@b.com", Role: "User", FirstName: "A", LastName: "B"}
	_ = db.Create(&u).Error
	f = File{Filename: "f2", InsertedBy: uint(u.ID)}
	_ = db.Create(&f).Error
	_ = db.Create(&FileVersion{FileID: f.ID, Filename: f.Filename, InsertedBy: uint(u.ID), Version: 1, Rows: 10}).Error
	hist, err := svc.GetFileHistory(fmt.Sprintf("%d", f.ID))
	if err != nil || len(hist) != 1 {
		t.Fatalf("expected 1 history row, got %d err=%v", len(hist), err)
	}
	_ = db.Migrator().DropTable(&FileVersion{})
	_, err = svc.GetFileHistory(fmt.Sprintf("%d", f.ID))
	if err == nil {
		t.Fatalf("expected history query err")
	}
}

// -----------------------------------------------------------------------------
// RevertFile
// -----------------------------------------------------------------------------

func TestFileService_RevertFile_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// file not found
	if err := svc.RevertFile("missing", 1, 1); err == nil {
		t.Fatalf("expected err")
	}

	// seed file
	f := File{Filename: "f1", Version: 2, Rows: 2, Size: 1.0}
	_ = db.Create(&f).Error

	// target version not found
	if err := svc.RevertFile("f1", 99, 1); err == nil {
		t.Fatalf("expected target not found")
	}

	// seed target version + file_data
	_ = db.Create(&FileVersion{FileID: f.ID, Filename: "f1", Version: 1, Rows: 1, Size: 0.5, Private: false}).Error
	_ = db.Create(&FileData{FileID: f.ID, Version: 1, RowData: datatypes.JSON([]byte(`{"a":"1"}`))}).Error

	// update file error: drop file table after reading
	_ = db.Migrator().DropTable(&File{})
	if err := svc.RevertFile("f1", 1, 2); err == nil {
		t.Fatalf("expected update err")
	}

	// recreate for next errors
	db = newTestDB(t)
	svc = &FileService{DB: db}
	f = File{Filename: "f1", Version: 2, Rows: 2, Size: 1.0}
	_ = db.Create(&f).Error
	_ = db.Create(&FileVersion{FileID: f.ID, Filename: "f1", Version: 1, Rows: 1, Size: 0.5, Private: false}).Error
	_ = db.Create(&FileData{FileID: f.ID, Version: 1, RowData: datatypes.JSON([]byte(`{"a":"1"}`))}).Error

	// create new file_version error
	_ = db.Migrator().DropTable(&FileVersion{})
	if err := svc.RevertFile("f1", 1, 2); err == nil {
		t.Fatalf("expected create file_version err")
	}

	// find data rows error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	f = File{Filename: "f1", Version: 2, Rows: 2, Size: 1.0}
	_ = db.Create(&f).Error
	_ = db.Create(&FileVersion{FileID: f.ID, Filename: "f1", Version: 1, Rows: 1, Size: 0.5, Private: false}).Error
	_ = db.Create(&FileData{FileID: f.ID, Version: 1, RowData: datatypes.JSON([]byte(`{"a":"1"}`))}).Error
	_ = db.Migrator().DropTable(&FileData{})
	if err := svc.RevertFile("f1", 1, 2); err == nil {
		t.Fatalf("expected find data err")
	}

	// success
	db = newTestDB(t)
	svc = &FileService{DB: db}
	f = File{Filename: "f2", Version: 2, Rows: 2, Size: 1.0, Private: true}
	_ = db.Create(&f).Error
	_ = db.Create(&FileVersion{FileID: f.ID, Filename: "f2", Version: 2, Rows: 1, Size: 0.5, Private: true}).Error
	_ = db.Create(&FileData{FileID: f.ID, Version: 2, RowData: datatypes.JSON([]byte(`{"a":"1"}`))}).Error

	if err := svc.RevertFile("f2", 2, 99); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var updated File
	_ = db.Where("filename = ?", "f2").First(&updated).Error
	if updated.Version != 3 || updated.Rows != 1 || updated.Private != true {
		t.Fatalf("unexpected updated file: %#v", updated)
	}
	var newRows int64
	_ = db.Model(&FileData{}).Where("file_id = ? AND version = ?", f.ID, 3).Count(&newRows).Error
	if newRows != 1 {
		t.Fatalf("expected copied 1 row, got %d", newRows)
	}
}

// -----------------------------------------------------------------------------
// CreateEditRequest
// -----------------------------------------------------------------------------

func TestFileService_CreateEditRequest_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// Hook uploadToGCS to avoid real GCS and to capture object paths
	prevUpload := uploadToGCSHook
	t.Cleanup(func() { uploadToGCSHook = prevUpload })

	var uploadedPaths []string
	uploadToGCSHook = func(base64, bucket, objectPath string) (string, int64, error) {
		uploadedPaths = append(uploadedPaths, objectPath)
		return "gs://" + bucket + "/" + objectPath, 123, nil
	}

	// main request insert error
	_ = db.Migrator().DropTable(&FileEditRequest{})
	_, err := svc.CreateEditRequest(EditRequestInput{FirstName: "A", LastName: "B", FileID: 1, Filename: "f"}, 1)
	if err == nil {
		t.Fatalf("expected create request err")
	}

	// details insert error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	uploadToGCSHook = func(base64, bucket, objectPath string) (string, int64, error) {
		return "gs://" + bucket + "/" + objectPath, 123, nil
	}
	_ = db.Migrator().DropTable(&FileEditRequestDetails{})
	_, err = svc.CreateEditRequest(EditRequestInput{
		FirstName: "A", LastName: "B",
		FileID:   1,
		Filename: "f",
		Changes: map[string][]EditChangeInput{
			"block": {{RowID: 0, FieldName: "x", OldValue: "o", NewValue: "n"}},
		},
	}, 1)
	if err == nil {
		t.Fatalf("expected details insert err")
	}

	// upload error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	uploadToGCSHook = func(_, _, _ string) (string, int64, error) {
		return "", 0, errors.New("upload fail")
	}
	_, err = svc.CreateEditRequest(EditRequestInput{
		FirstName: "A",
		LastName:  "B",
		FileID:    1,
		Filename:  "f",
		PhotosInApp: []PhotoInput{
			{Filename: "a.jpg", MimeType: "image/jpeg", DataBase64: "b64", Comment: "c"},
		},
		IsEdited: false,
		RowID:    0,
	}, 1)
	if err == nil {
		t.Fatalf("expected upload err")
	}

	// photo record insert error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	uploadToGCSHook = func(base64, bucket, objectPath string) (string, int64, error) {
		return "gs://" + bucket + "/" + objectPath, 123, nil
	}
	_ = db.Migrator().DropTable(&FileEditRequestPhoto{})
	_, err = svc.CreateEditRequest(EditRequestInput{
		FirstName: "A",
		LastName:  "B",
		FileID:    1,
		Filename:  "f",
		PhotosInApp: []PhotoInput{
			{Filename: "a.jpg", MimeType: "image/jpeg", DataBase64: "b64"},
		},
		IsEdited: false,
		RowID:    0,
	}, 1)
	if err == nil {
		t.Fatalf("expected photo insert err")
	}

	// Gallery upload error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	call := 0
	uploadToGCSHook = func(_, _, _ string) (string, int64, error) {
		call++
		if call == 1 {
			return "gs://b/x", 1, nil // app photo ok
		}
		return "", 0, errors.New("gallery upload fail")
	}
	_, err = svc.CreateEditRequest(EditRequestInput{
		FirstName: "A",
		LastName:  "B",
		FileID:    1,
		Filename:  "f",
		PhotosInApp: []PhotoInput{
			{Filename: "a.jpg", MimeType: "image/jpeg", DataBase64: "b64"},
		},
		PhotosForGallery: []PhotoInput{
			{Filename: "b.jpg", MimeType: "image/jpeg", DataBase64: "b642"},
		},
		IsEdited: false,
		RowID:    0,
	}, 1)
	if err == nil {
		t.Fatalf("expected gallery upload err")
	}

	// documents: skip non-document, doc upload error
	db = newTestDB(t)
	svc = &FileService{DB: db}

	uploadToGCSHook = func(_, _, _ string) (string, int64, error) {
		return "", 0, errors.New("doc upload fail")
	}
	_, err = svc.CreateEditRequest(EditRequestInput{
		FirstName: "A", LastName: "B",
		FileID: 1, Filename: "f",
		IsEdited: false, RowID: 0,
		Documents: []DocumentInput{
			{DocumentType: "photos", Filename: "skip.png", DataBase64: "x"},
			{DocumentType: "document", Filename: "doc.pdf", DataBase64: "x", DocumentCategory: ""},
		},
	}, 1)
	if err == nil {
		t.Fatalf("expected doc upload err")
	}

	// full success (covers TempPrefix + RowPrefix logic + doc defaults)
	db = newTestDB(t)
	svc = &FileService{DB: db}

	uploadedPaths = nil
	uploadToGCSHook = func(base64, bucket, objectPath string) (string, int64, error) {
		uploadedPaths = append(uploadedPaths, objectPath)
		return "gs://" + bucket + "/" + objectPath, 123, nil
	}

	req, err := svc.CreateEditRequest(EditRequestInput{
		FirstName: "Athul", LastName: "N",
		FileID: 10, Filename: "master",
		IsEdited: false, RowID: 0,
		Consent: true,
		Changes: map[string][]EditChangeInput{
			"c": {{RowID: 0, FieldName: "Name", OldValue: "", NewValue: "X"}},
		},
		PhotosInApp: []PhotoInput{
			{Filename: "a.jpg", MimeType: "image/jpeg", DataBase64: "b64a", Comment: "hello"},
		},
		PhotosForGallery: []PhotoInput{
			{Filename: "g.jpg", MimeType: "image/jpeg", DataBase64: "b64g", Comment: "gallery"},
		},
		Documents: []DocumentInput{
			{DocumentType: "document", Filename: "id.pdf", DataBase64: "b64d", DocumentCategory: ""},
		},
	}, 55)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if req == nil || req.RequestID == 0 {
		t.Fatalf("expected request created")
	}

	// basePrefix must be TempPrefix because IsEdited=false OR RowID==0
	wantPrefix := util.TempPrefix(req.RequestID, "Athul", "N")
	for _, p := range uploadedPaths {
		if !strings.HasPrefix(p, wantPrefix+"/") {
			t.Fatalf("expected upload path to start with %q, got %q", wantPrefix+"/", p)
		}
	}

	// RowPrefix path (IsEdited=true and RowID!=0)
	uploadedPaths = nil
	_, err = svc.CreateEditRequest(EditRequestInput{
		FirstName: "A", LastName: "B",
		FileID: 10, Filename: "master",
		IsEdited: true, RowID: 123,
		PhotosInApp: []PhotoInput{
			{Filename: "a.jpg", MimeType: "image/jpeg", DataBase64: "b64"},
		},
	}, 55)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	wantRowPrefix := util.RowPrefix(123)
	if len(uploadedPaths) == 0 || !strings.HasPrefix(uploadedPaths[0], wantRowPrefix+"/") {
		t.Fatalf("expected rowprefix %q, got %#v", wantRowPrefix, uploadedPaths)
	}
}

func TestFileService_CreateEditRequest_PhotoExtension_FromFilenameOrMime(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	prevUpload := uploadToGCSHook
	t.Cleanup(func() { uploadToGCSHook = prevUpload })

	var uploadedPaths []string
	uploadToGCSHook = func(base64, bucket, objectPath string) (string, int64, error) {
		uploadedPaths = append(uploadedPaths, objectPath)
		return "gs://" + bucket + "/" + objectPath, 123, nil
	}

	req, err := svc.CreateEditRequest(EditRequestInput{
		FirstName: "A", LastName: "B",
		FileID: 1, Filename: "f",
		IsEdited: false, RowID: 0,
		PhotosInApp: []PhotoInput{
			// extension from filename
			{Filename: "x.png", MimeType: "image/png", DataBase64: "b64"},
			// no extension -> should come from mime
			{Filename: "noext", MimeType: "image/webp", DataBase64: "b64"},
			// neither helpful -> default .jpg
			{Filename: "unknown", MimeType: "application/octet-stream", DataBase64: "b64"},
		},
	}, 1)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if req == nil || req.RequestID == 0 {
		t.Fatalf("expected request created")
	}

	if len(uploadedPaths) != 3 {
		t.Fatalf("expected 3 uploads, got %d (%#v)", len(uploadedPaths), uploadedPaths)
	}
	if !strings.HasSuffix(uploadedPaths[0], ".png") {
		t.Fatalf("expected .png suffix, got %q", uploadedPaths[0])
	}
	if !strings.HasSuffix(uploadedPaths[1], ".webp") {
		t.Fatalf("expected .webp suffix, got %q", uploadedPaths[1])
	}
	if !strings.HasSuffix(uploadedPaths[2], ".jpg") {
		t.Fatalf("expected default .jpg suffix, got %q", uploadedPaths[2])
	}
}

// -----------------------------------------------------------------------------
// GetEditRequests
// -----------------------------------------------------------------------------

func TestFileService_GetEditRequests_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	u := UserForTest{Email: "u@b.com", Role: "User", FirstName: "U", LastName: "L"}
	_ = db.Create(&u).Error

	// seed pending + approved
	r1 := FileEditRequest{UserID: uint(u.ID), Status: "pending", CreatedAt: time.Now(), FirstName: "E", LastName: "F", RowID: 0, IsEdited: true, Consent: true, FileID: 1}
	r2 := FileEditRequest{UserID: uint(u.ID), Status: "approved", CreatedAt: time.Now().Add(-time.Hour), FirstName: "E2", LastName: "F2", RowID: 0, IsEdited: true, Consent: true, FileID: 1}
	_ = db.Create(&r1).Error
	_ = db.Create(&r2).Error

	_ = db.Create(&FileEditRequestDetails{RequestID: r1.RequestID, FileID: 1, Filename: "x", RowID: 0, FieldName: "A", OldValue: "", NewValue: "1"}).Error
	_ = db.Create(&FileEditRequestDetails{RequestID: r2.RequestID, FileID: 1, Filename: "x", RowID: 0, FieldName: "A", OldValue: "", NewValue: "2"}).Error

	// default => pending only
	out, err := svc.GetEditRequests(nil, nil)
	if err != nil || len(out) != 1 || out[0].Status != "pending" {
		t.Fatalf("expected pending only, got %#v err=%v", out, err)
	}

	// both filters => IN (...)
	statusCSV := "approved, pending"
	uid := uint(u.ID)
	out, err = svc.GetEditRequests(&statusCSV, &uid)
	if err != nil || len(out) != 2 {
		t.Fatalf("expected 2, got %d err=%v", len(out), err)
	}

	// garbage statusCSV => fallback pending
	garb := ",,,"
	out, err = svc.GetEditRequests(&garb, &uid)
	if err != nil || len(out) != 1 || out[0].Status != "pending" {
		t.Fatalf("expected pending fallback, got %#v err=%v", out, err)
	}

	// scan error
	breakDB(t, db)
	_, err = svc.GetEditRequests(nil, nil)
	if err == nil {
		t.Fatalf("expected scan err")
	}

	// details query error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	_ = db.Create(&u).Error
	r3 := FileEditRequest{UserID: uint(u.ID), Status: "pending", CreatedAt: time.Now(), FirstName: "E", LastName: "F", RowID: 0, IsEdited: true, Consent: true, FileID: 1}
	_ = db.Create(&r3).Error
	_ = db.Migrator().DropTable(&FileEditRequestDetails{})
	_, err = svc.GetEditRequests(nil, nil)
	if err == nil {
		t.Fatalf("expected details query err")
	}
}

// -----------------------------------------------------------------------------
// ApproveEditRequest (edited + not edited)
// -----------------------------------------------------------------------------

func TestFileService_ApproveEditRequest_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// restore hooks
	prevMove := moveGCSFolderHook
	t.Cleanup(func() { moveGCSFolderHook = prevMove })
	moveGCSFolderHook = func(bucket, src, dst string) (map[string]string, error) {
		return map[string]string{}, nil
	}

	// 0) update new_value error
	_ = db.Migrator().DropTable(&FileEditRequestDetails{})
	if err := svc.ApproveEditRequest(1, []FileEditRequestDetails{{ID: 1, NewValue: "x"}}, 9); err == nil {
		t.Fatalf("expected update new_value err")
	}

	// recreate
	db = newTestDB(t)
	svc = &FileService{DB: db}

	// request not found
	if err := svc.ApproveEditRequest(999, nil, 9); err == nil {
		t.Fatalf("expected request not found err")
	}

	// allDetails fetch error
	req := FileEditRequest{UserID: 1, Status: "pending", IsEdited: true, FirstName: "A", LastName: "B", FileID: 1}
	_ = db.Create(&req).Error
	_ = db.Migrator().DropTable(&FileEditRequestDetails{})
	if err := svc.ApproveEditRequest(req.RequestID, nil, 9); err == nil {
		t.Fatalf("expected details fetch err")
	}

	// --- NOT EDITED path errors + success ---
	db = newTestDB(t)
	svc = &FileService{DB: db}

	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: false, FirstName: "A", LastName: "B", RowID: 0, FileID: 10}
	_ = db.Create(&req).Error
	d1 := FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, Filename: "f", RowID: 0, FieldName: "Name", OldValue: "", NewValue: "X"}
	_ = db.Create(&d1).Error

	// insert file_data error
	_ = db.Migrator().DropTable(&FileData{})
	if err := svc.ApproveEditRequest(req.RequestID, []FileEditRequestDetails{{ID: d1.ID, NewValue: "Y"}}, 9); err == nil {
		t.Fatalf("expected insert file_data err")
	}

	// move folder hook error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	moveGCSFolderHook = func(bucket, src, dst string) (map[string]string, error) {
		return nil, errors.New("move fail")
	}
	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: false, FirstName: "A", LastName: "B", RowID: 0, FileID: 10}
	_ = db.Create(&req).Error
	_ = db.Create(&FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, Filename: "f", RowID: 0, FieldName: "Name", OldValue: "", NewValue: "X"}).Error
	_ = db.Create(&FileEditRequestPhoto{RequestID: req.RequestID, RowID: 0, FileID: 10, PhotoURL: "gs://nordik-drive-photos/requests/x.jpg", FileName: "x.jpg", DocumentType: "photos"}).Error
	if err := svc.ApproveEditRequest(req.RequestID, nil, 9); err == nil {
		t.Fatalf("expected move folder err")
	}

	// NOT EDITED success
	db = newTestDB(t)
	svc = &FileService{DB: db}
	moveGCSFolderHook = func(bucket, src, dst string) (map[string]string, error) { return map[string]string{}, nil }

	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: false, FirstName: "A", LastName: "B", RowID: 0, FileID: 10}
	_ = db.Create(&req).Error
	d1 = FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, Filename: "f", RowID: 0, FieldName: "Name", OldValue: "", NewValue: "X"}
	_ = db.Create(&d1).Error

	photo := FileEditRequestPhoto{
		RequestID:    req.RequestID,
		RowID:        0,
		FileID:       10,
		FileName:     "x.jpg",
		PhotoURL:     "gs://nordik-drive-photos/" + util.TempPrefix(req.RequestID, "A", "B") + "/x.jpg",
		DocumentType: "photos",
	}
	_ = db.Create(&photo).Error

	if err := svc.ApproveEditRequest(req.RequestID, []FileEditRequestDetails{{ID: d1.ID, NewValue: "Y"}}, 9); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	var reqUpdated FileEditRequest
	_ = db.Where("request_id = ?", req.RequestID).First(&reqUpdated).Error
	if reqUpdated.Status != "approved" || reqUpdated.ApprovedBy == nil || *reqUpdated.ApprovedBy != 9 || reqUpdated.RowID == 0 {
		t.Fatalf("unexpected updated request: %#v", reqUpdated)
	}

	// --- EDITED path errors + success ---
	db = newTestDB(t)
	svc = &FileService{DB: db}

	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: true, RowID: 123, FileID: 10}
	_ = db.Create(&req).Error

	// file_data not found
	_ = db.Create(&FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, RowID: 123, FieldName: "A", NewValue: "X"}).Error
	if err := svc.ApproveEditRequest(req.RequestID, nil, 9); err == nil {
		t.Fatalf("expected file_data not found")
	}

	// row_data unmarshal error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: true, RowID: 123, FileID: 10}
	_ = db.Create(&req).Error
	_ = db.Create(&FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, RowID: 123, FieldName: "A", NewValue: "X"}).Error
	_ = db.Create(&FileData{ID: 123, FileID: 10, RowData: datatypes.JSON([]byte(`{bad`)), Version: 1}).Error
	if err := svc.ApproveEditRequest(req.RequestID, nil, 9); err == nil {
		t.Fatalf("expected row_data parse error")
	}

	// update file_data error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: true, RowID: 123, FileID: 10}
	_ = db.Create(&req).Error
	_ = db.Create(&FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, RowID: 123, FieldName: "A", NewValue: "X"}).Error
	_ = db.Create(&FileData{ID: 123, FileID: 10, RowData: datatypes.JSON([]byte(`{"A":"old"}`)), Version: 1}).Error
	_ = db.Migrator().DropTable(&FileData{})
	if err := svc.ApproveEditRequest(req.RequestID, nil, 9); err == nil {
		t.Fatalf("expected update file_data err")
	}

	// status update error (drop request table before final update)
	db = newTestDB(t)
	svc = &FileService{DB: db}
	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: true, RowID: 123, FileID: 10}
	_ = db.Create(&req).Error
	_ = db.Create(&FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, RowID: 123, FieldName: "A", NewValue: "X"}).Error
	_ = db.Create(&FileData{ID: 123, FileID: 10, RowData: datatypes.JSON([]byte(`{"A":"old"}`)), Version: 1}).Error
	_ = db.Migrator().DropTable(&FileEditRequest{})
	if err := svc.ApproveEditRequest(req.RequestID, nil, 9); err == nil {
		t.Fatalf("expected status update err")
	}

	// EDITED success
	db = newTestDB(t)
	svc = &FileService{DB: db}
	req = FileEditRequest{UserID: 1, Status: "pending", IsEdited: true, RowID: 123, FileID: 10}
	_ = db.Create(&req).Error
	_ = db.Create(&FileEditRequestDetails{RequestID: req.RequestID, FileID: 10, RowID: 123, FieldName: "A", NewValue: "X"}).Error
	_ = db.Create(&FileData{ID: 123, FileID: 10, RowData: datatypes.JSON([]byte(`{"A":"old","B":"keep"}`)), Version: 1}).Error
	if err := svc.ApproveEditRequest(req.RequestID, nil, 9); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var updatedReq FileEditRequest
	_ = db.Where("request_id = ?", req.RequestID).First(&updatedReq).Error
	if updatedReq.Status != "approved" || updatedReq.ApprovedBy == nil || *updatedReq.ApprovedBy != 9 {
		t.Fatalf("unexpected req: %#v", updatedReq)
	}
}

// -----------------------------------------------------------------------------
// Photos/docs queries + ReviewPhotos
// -----------------------------------------------------------------------------

func TestFileService_PhotoDocQueries_And_ReviewPhotos(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	// seed photos/docs
	p1 := FileEditRequestPhoto{RequestID: 1, RowID: 10, DocumentType: "photos", IsApproved: false, FileName: "p.jpg"}
	d1 := FileEditRequestPhoto{RequestID: 1, RowID: 10, DocumentType: "document", IsApproved: false, FileName: "d.pdf"}
	_ = db.Create(&p1).Error
	_ = db.Create(&d1).Error

	photos, err := svc.GetPhotosByRequest(1)
	if err != nil || len(photos) != 1 {
		t.Fatalf("expected 1 photo, got %d err=%v", len(photos), err)
	}
	docs, err := svc.GetDocsByRequest(1)
	if err != nil || len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d err=%v", len(docs), err)
	}

	// approved-by-row queries
	now := time.Now()
	_ = db.Model(&FileEditRequestPhoto{}).Where("id = ?", p1.ID).Updates(map[string]any{
		"is_approved": true,
		"approved_at": now,
	}).Error
	_ = db.Model(&FileEditRequestPhoto{}).Where("id = ?", d1.ID).Updates(map[string]any{
		"is_approved": true,
		"approved_at": now,
	}).Error

	phByRow, err := svc.GetPhotosByRow(uint(10))
	if err != nil || len(phByRow) != 1 {
		t.Fatalf("expected 1 approved photo by row, got %d err=%v", len(phByRow), err)
	}
	docByRow, err := svc.GetDocsByRow(uint(10))
	if err != nil || len(docByRow) != 1 {
		t.Fatalf("expected 1 approved doc by row, got %d err=%v", len(docByRow), err)
	}

	// ReviewPhotos: no ids
	if err := svc.ReviewPhotos(nil, nil, "r"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	// ReviewPhotos approve error
	_ = db.Migrator().DropTable(&FileEditRequestPhoto{})
	if err := svc.ReviewPhotos([]uint{1}, nil, "r"); err == nil {
		t.Fatalf("expected approve error")
	}

	// ReviewPhotos reject error
	db = newTestDB(t)
	svc = &FileService{DB: db}
	_ = db.Create(&FileEditRequestPhoto{RequestID: 1, RowID: 10, DocumentType: "photos", IsApproved: true}).Error
	_ = db.Migrator().DropTable(&FileEditRequestPhoto{})
	if err := svc.ReviewPhotos(nil, []uint{1}, "r"); err == nil {
		t.Fatalf("expected reject error")
	}

	// ReviewPhotos success (approve + reject)
	db = newTestDB(t)
	svc = &FileService{DB: db}
	a := FileEditRequestPhoto{RequestID: 1, RowID: 10, DocumentType: "photos", IsApproved: false}
	r := FileEditRequestPhoto{RequestID: 1, RowID: 10, DocumentType: "photos", IsApproved: true}
	_ = db.Create(&a).Error
	_ = db.Create(&r).Error

	if err := svc.ReviewPhotos([]uint{a.ID}, []uint{r.ID}, "reviewer"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	var aa, rr FileEditRequestPhoto
	_ = db.First(&aa, a.ID).Error
	_ = db.First(&rr, r.ID).Error
	if aa.IsApproved != true || rr.IsApproved != false {
		t.Fatalf("unexpected review result: approve=%v reject=%v", aa.IsApproved, rr.IsApproved)
	}
}

// -----------------------------------------------------------------------------
// GCS functions + ioReadAll full branch coverage
// -----------------------------------------------------------------------------

func TestIOReadAll_AllBranches_WithoutNetwork(t *testing.T) {
	// EOF branch (err.Error()=="EOF")
	{
		sr := &storage.Reader{}
		inner := &errReadCloser{
			readFn: func(p []byte) (int, error) { return 0, errors.New("EOF") },
		}
		forceStorageReaderInner(t, sr, inner)
		data, err := ioReadAll(sr)
		if err != nil || string(data) != "" {
			t.Fatalf("expected empty,nil got=%q err=%v", string(data), err)
		}
	}

	// context.Canceled branch
	{
		sr := &storage.Reader{}
		inner := &errReadCloser{
			readFn: func(p []byte) (int, error) { return 0, context.Canceled },
		}
		forceStorageReaderInner(t, sr, inner)
		_, err := ioReadAll(sr)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	}

	// context.DeadlineExceeded branch
	{
		sr := &storage.Reader{}
		inner := &errReadCloser{
			readFn: func(p []byte) (int, error) { return 0, context.DeadlineExceeded },
		}
		forceStorageReaderInner(t, sr, inner)
		_, err := ioReadAll(sr)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	}

	// contains "eof" branch (e.g., "unexpected EOF")
	{
		sr := &storage.Reader{}
		inner := &errReadCloser{
			readFn: func(p []byte) (int, error) { return 0, errors.New("unexpected EOF") },
		}
		forceStorageReaderInner(t, sr, inner)
		data, err := ioReadAll(sr)
		if err != nil || string(data) != "" {
			t.Fatalf("expected empty,nil got=%q err=%v", string(data), err)
		}
	}

	// generic error branch
	{
		sr := &storage.Reader{}
		inner := &errReadCloser{
			readFn: func(p []byte) (int, error) { return 0, errors.New("boom") },
		}
		forceStorageReaderInner(t, sr, inner)
		_, err := ioReadAll(sr)
		if err == nil || err.Error() != "boom" {
			t.Fatalf("expected boom, got %v", err)
		}
	}

	// normal read path (n>0 then EOF)
	{
		sr := &storage.Reader{}
		step := 0
		inner := &errReadCloser{
			readFn: func(p []byte) (int, error) {
				step++
				if step == 1 {
					copy(p, []byte("abc"))
					return 3, nil
				}
				return 0, errors.New("EOF")
			},
		}
		forceStorageReaderInner(t, sr, inner)
		data, err := ioReadAll(sr)
		if err != nil || string(data) != "abc" {
			t.Fatalf("expected abc,nil got=%q err=%v", string(data), err)
		}
	}
}

func TestFileService_GCS_Readers_AllBranches(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	srv, bucket := withFakeGCS(t)

	// GetDocBytes: non-document guard
	doc := FileEditRequestPhoto{DocumentType: "photos"}
	_ = db.Create(&doc).Error
	if _, _, _, err := svc.GetDocBytes(doc.ID); err == nil {
		t.Fatalf("expected non-document err")
	}

	// GetDocBytes: BUCKET_NAME missing
	_ = os.Unsetenv("BUCKET_NAME")
	db2 := newTestDB(t)
	svc2 := &FileService{DB: db2}
	doc2 := FileEditRequestPhoto{DocumentType: "document", PhotoURL: "gs://x/y.pdf", FileName: ""}
	_ = db2.Create(&doc2).Error
	if _, _, _, err := svc2.GetDocBytes(doc2.ID); err == nil {
		t.Fatalf("expected BUCKET_NAME env err")
	}

	// Success GetDocBytes (also covers filename fallback)
	_ = os.Setenv("BUCKET_NAME", bucket)

	object := "docs/test.pdf"
	content := []byte("%PDF-1.4 test")
	putGCSObject(t, srv, bucket, object, content, "") // empty ct => service falls back to DetectContentType

	doc3 := FileEditRequestPhoto{
		DocumentType: "document",
		PhotoURL:     "gs://" + bucket + "/" + object,
		FileName:     "", // triggers path.Base(objectPath) fallback
	}
	_ = db.Create(&doc3).Error

	data, ct, filename, err := svc.GetDocBytes(doc3.ID)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("bytes mismatch")
	}
	if ct == "" {
		t.Fatalf("expected contentType derived")
	}
	if filename != path.Base(object) {
		t.Fatalf("expected filename %q got %q", path.Base(object), filename)
	}

	// GetPhotoBytes success
	photoObj := "photos/p1.jpg"
	photoBytes := []byte{0xFF, 0xD8, 0xFF, 0x00}
	putGCSObject(t, srv, bucket, photoObj, photoBytes, "image/jpeg")

	p := FileEditRequestPhoto{PhotoURL: "gs://" + bucket + "/" + photoObj, DocumentType: "photos"}
	_ = db.Create(&p).Error
	got, ct2, err := svc.GetPhotoBytes(p.ID)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(got) != len(photoBytes) || ct2 != "image/jpeg" {
		t.Fatalf("unexpected photo bytes/ct: len=%d ct=%q", len(got), ct2)
	}

	// GetPhotoBytes error: object missing
	pMissing := FileEditRequestPhoto{PhotoURL: "gs://" + bucket + "/photos/missing.jpg", DocumentType: "photos"}
	_ = db.Create(&pMissing).Error
	if _, _, err := svc.GetPhotoBytes(pMissing.ID); err == nil {
		t.Fatalf("expected missing object error")
	}

	// OpenMediaHandle: kind guard + invalid url + bucket-from-env missing + success + disposition logic
	db3 := newTestDB(t)
	svc3 := &FileService{DB: db3}

	// kind mismatch
	rec := FileEditRequestPhoto{DocumentType: "document", PhotoURL: "gs://x/y.pdf"}
	_ = db3.Create(&rec).Error
	if _, _, _, _, err := svc3.OpenMediaHandle(context.Background(), rec.ID, "photo"); err == nil {
		t.Fatalf("expected kind mismatch error")
	}

	// invalid gs url format
	rec2 := FileEditRequestPhoto{DocumentType: "photos", PhotoURL: "http://x/y"}
	_ = db3.Create(&rec2).Error
	if _, _, _, _, err := svc3.OpenMediaHandle(context.Background(), rec2.ID, ""); err == nil {
		t.Fatalf("expected invalid gs url error")
	}

	// bucket empty in URL and BUCKET_NAME empty => error
	_ = os.Unsetenv("BUCKET_NAME")
	rec3 := FileEditRequestPhoto{DocumentType: "photos", PhotoURL: "gs:///obj.txt", FileName: ""}
	_ = db3.Create(&rec3).Error
	if _, _, _, _, err := svc3.OpenMediaHandle(context.Background(), rec3.ID, ""); err == nil {
		t.Fatalf("expected bucket missing error")
	}

	// restore bucket env for success
	_ = os.Setenv("BUCKET_NAME", bucket)

	// pdf => inline
	pdfObj := "media/a.pdf"
	putGCSObject(t, srv, bucket, pdfObj, []byte("%PDF-x"), "application/pdf")
	rec4 := FileEditRequestPhoto{DocumentType: "document", PhotoURL: "gs://" + bucket + "/" + pdfObj, FileName: ""}
	_ = db3.Create(&rec4).Error

	rc, fn, ct3, disp, err := svc3.OpenMediaHandle(context.Background(), rec4.ID, "document")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if fn != path.Base(pdfObj) || ct3 != "application/pdf" || disp != "inline" {
		t.Fatalf("unexpected: fn=%q ct=%q disp=%q", fn, ct3, disp)
	}
	_ = rc.Close()

	// non-image + non-pdf => attachment
	txtObj := "media/a.txt"
	putGCSObject(t, srv, bucket, txtObj, []byte("hello"), "text/plain")
	rec5 := FileEditRequestPhoto{DocumentType: "photos", PhotoURL: "gs://" + bucket + "/" + txtObj, FileName: "named.txt"}
	_ = db3.Create(&rec5).Error

	rc2, fn2, ct4, disp2, err := svc3.OpenMediaHandle(context.Background(), rec5.ID, "")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if fn2 != "named.txt" || ct4 != "text/plain" || disp2 != "attachment" {
		t.Fatalf("unexpected: fn=%q ct=%q disp=%q", fn2, ct4, disp2)
	}
	_ = rc2.Close()

	// readFromGCS: invalid url + bucket missing + success
	_, _, _, err = svc.readFromGCS("http://x", "")
	if err == nil {
		t.Fatalf("expected invalid gs url")
	}
	_ = os.Unsetenv("BUCKET_NAME")
	_, _, _, err = svc.readFromGCS("gs:///obj.txt", "")
	if err == nil {
		t.Fatalf("expected bucket missing")
	}
	_ = os.Setenv("BUCKET_NAME", bucket)

	obj := "bytes/x.bin"
	putGCSObject(t, srv, bucket, obj, []byte("xyz"), "")
	bts, ctt, fname, err := svc.readFromGCS("gs://"+bucket+"/"+obj, "")
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if string(bts) != "xyz" || ctt == "" || fname != path.Base(obj) {
		t.Fatalf("unexpected readFromGCS: b=%q ct=%q fn=%q", string(bts), ctt, fname)
	}

	// gcsReadHandle Close/Read: cover both methods using a forced storage.Reader
	h := &gcsReadHandle{
		Client: nil,
		Reader: func() *storage.Reader {
			sr := &storage.Reader{}
			step := 0
			forceStorageReaderInner(t, sr, &errReadCloser{
				readFn: func(p []byte) (int, error) {
					step++
					if step == 1 {
						copy(p, []byte("hi"))
						return 2, nil
					}
					return 0, errors.New("EOF")
				},
				closeFn: func() error { return nil },
			})
			return sr
		}(),
	}

	buf := make([]byte, 8)
	n, rerr := h.Read(buf)
	if n != 2 || string(buf[:2]) != "hi" || rerr != nil {
		t.Fatalf("unexpected read: n=%d data=%q err=%v", n, string(buf[:2]), rerr)
	}
	_ = h.Close()
}
