package file

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gorm.io/datatypes"
)

func TestFileService_NormalizedDataSyncAndRead(t *testing.T) {
	db := newTestDB(t)
	svc := &FileService{DB: db}

	file := File{
		Filename:   "records.csv",
		InsertedBy: 1,
		CreatedAt:  time.Now(),
		Version:    1,
		Rows:       1,
		Size:       1,
	}
	if err := db.Create(&file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	rowJSON := datatypes.JSON([]byte(`{
		"NAME":"Wells, WEST",
		"First Nation/Community":"Garden River First Nation",
		"SCHOOL":"Shingwauk Indian Residential School",
		"DATE OF DEATH":"abt 1819"
	}`))

	row := FileData{
		FileID:     file.ID,
		RowData:    rowJSON,
		InsertedBy: 1,
		CreatedAt:  time.Now().Add(-2 * time.Hour),
		UpdatedAt:  time.Now().Add(-2 * time.Hour),
		Version:    1,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("create row: %v", err)
	}

	result, err := svc.SyncNormalizedFileData(file.Filename, 1)
	if err != nil {
		t.Fatalf("sync normalized data: %v", err)
	}
	if result == nil {
		t.Fatalf("expected sync result")
	}
	if result.Processed != 1 || result.Inserted != 1 || result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("unexpected sync result: %#v", result)
	}

	rows, err := svc.GetNormalizedFileData(file.Filename, 1)
	if err != nil {
		t.Fatalf("get normalized data: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 normalized row, got %d", len(rows))
	}

	if rows[0].CanonicalName != "wells west" {
		t.Fatalf("unexpected canonical name: %q", rows[0].CanonicalName)
	}
	if rows[0].CanonicalCommunity != "garden river first nation" {
		t.Fatalf("unexpected canonical community: %q", rows[0].CanonicalCommunity)
	}
	if rows[0].CanonicalSchool != "shingwauk indian residential school" {
		t.Fatalf("unexpected canonical school: %q", rows[0].CanonicalSchool)
	}
	if !strings.Contains(rows[0].SearchText, "garden") || !strings.Contains(rows[0].SearchText, "shingwauk") {
		t.Fatalf("unexpected search text: %q", rows[0].SearchText)
	}

	var payload normalizedRowPayload
	if err := json.Unmarshal(rows[0].RowDataNormalized, &payload); err != nil {
		t.Fatalf("unmarshal normalized payload: %v", err)
	}
	dateField, ok := payload.Fields["DATE OF DEATH"]
	if !ok || dateField.DateHint == nil {
		t.Fatalf("expected date hint in payload: %#v", payload.Fields)
	}
	if dateField.DateHint.Kind != "approximate_year" {
		t.Fatalf("unexpected date hint kind: %#v", dateField.DateHint)
	}

	updatedJSON := datatypes.JSON([]byte(`{
		"NAME":"Wells, WEST",
		"First Nation/Community":"Walpole Island First Nation",
		"SCHOOL":"Shingwauk Indian Residential School",
		"DATE OF DEATH":"11-11-1996"
	}`))
	later := time.Now().Add(3 * time.Minute)
	if err := db.Model(&FileData{}).
		Where("id = ?", row.ID).
		Updates(map[string]interface{}{
			"row_data":   updatedJSON,
			"updated_at": later,
		}).Error; err != nil {
		t.Fatalf("update source row: %v", err)
	}

	result, err = svc.SyncNormalizedFileData(file.Filename, 1)
	if err != nil {
		t.Fatalf("resync normalized data: %v", err)
	}
	if result.Processed != 1 || result.Updated != 1 {
		t.Fatalf("unexpected resync result: %#v", result)
	}

	rows, err = svc.GetNormalizedFileData(file.Filename, 1)
	if err != nil {
		t.Fatalf("reload normalized rows: %v", err)
	}
	if rows[0].CanonicalCommunity != "walpole island first nation" {
		t.Fatalf("expected updated community, got %q", rows[0].CanonicalCommunity)
	}
}

func TestSyncPendingNormalizedRowsMarksFailuresWithoutLooping(t *testing.T) {
	db := newTestDB(t)

	file := File{
		Filename:   "bad.csv",
		InsertedBy: 1,
		CreatedAt:  time.Now(),
		Version:    1,
		Rows:       1,
		Size:       1,
	}
	if err := db.Create(&file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	row := FileData{
		FileID:     file.ID,
		RowData:    datatypes.JSON([]byte(`{bad json`)),
		InsertedBy: 1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Version:    1,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("create broken row: %v", err)
	}

	fileID := file.ID
	version := 1
	result, err := SyncPendingNormalizedRows(db, &fileID, &version, 10)
	if err != nil {
		t.Fatalf("sync pending normalized rows: %v", err)
	}
	if result.Processed != 1 || result.Inserted != 1 || result.Failed != 1 {
		t.Fatalf("unexpected first result: %#v", result)
	}

	var normalized FileDataNormalized
	if err := db.Where("source_row_id = ?", row.ID).First(&normalized).Error; err != nil {
		t.Fatalf("load normalized failure row: %v", err)
	}
	if normalized.Status != "failed" {
		t.Fatalf("expected failed status, got %q", normalized.Status)
	}
	if !strings.Contains(normalized.ErrorMessage, "invalid row data json") {
		t.Fatalf("unexpected error message: %q", normalized.ErrorMessage)
	}

	result, err = SyncPendingNormalizedRows(db, &fileID, &version, 10)
	if err != nil {
		t.Fatalf("second sync pending normalized rows: %v", err)
	}
	if result.Processed != 0 {
		t.Fatalf("expected no retry without source changes, got %#v", result)
	}
}

func TestRunNormalizationSyncPrioritizesLatestFileVersions(t *testing.T) {
	db := newTestDB(t)

	fileOne := File{
		Filename:   "one.csv",
		InsertedBy: 1,
		CreatedAt:  time.Now(),
		Version:    2,
		Rows:       2,
		Size:       1,
	}
	fileTwo := File{
		Filename:   "two.csv",
		InsertedBy: 1,
		CreatedAt:  time.Now(),
		Version:    3,
		Rows:       2,
		Size:       1,
	}
	if err := db.Create(&fileOne).Error; err != nil {
		t.Fatalf("create file one: %v", err)
	}
	if err := db.Create(&fileTwo).Error; err != nil {
		t.Fatalf("create file two: %v", err)
	}

	rows := []FileData{
		{
			FileID:     fileOne.ID,
			RowData:    datatypes.JSON([]byte(`{"NAME":"Old One"}`)),
			InsertedBy: 1,
			CreatedAt:  time.Now().Add(-4 * time.Hour),
			UpdatedAt:  time.Now().Add(-4 * time.Hour),
			Version:    1,
		},
		{
			FileID:     fileOne.ID,
			RowData:    datatypes.JSON([]byte(`{"NAME":"Latest One"}`)),
			InsertedBy: 1,
			CreatedAt:  time.Now().Add(-3 * time.Hour),
			UpdatedAt:  time.Now().Add(-3 * time.Hour),
			Version:    2,
		},
		{
			FileID:     fileTwo.ID,
			RowData:    datatypes.JSON([]byte(`{"NAME":"Old Two"}`)),
			InsertedBy: 1,
			CreatedAt:  time.Now().Add(-2 * time.Hour),
			UpdatedAt:  time.Now().Add(-2 * time.Hour),
			Version:    1,
		},
		{
			FileID:     fileTwo.ID,
			RowData:    datatypes.JSON([]byte(`{"NAME":"Latest Two"}`)),
			InsertedBy: 1,
			CreatedAt:  time.Now().Add(-1 * time.Hour),
			UpdatedAt:  time.Now().Add(-1 * time.Hour),
			Version:    3,
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("create file rows: %v", err)
	}

	result, err := RunNormalizationSync(db, NormalizationSyncOptions{
		BatchSize:  2,
		MaxBatches: 1,
	})
	if err != nil {
		t.Fatalf("run normalization sync: %v", err)
	}
	if result.Processed != 2 || result.Inserted != 2 {
		t.Fatalf("unexpected sync result: %#v", result)
	}

	var normalizedRows []FileDataNormalized
	if err := db.Order("source_row_id ASC").Find(&normalizedRows).Error; err != nil {
		t.Fatalf("load normalized rows: %v", err)
	}
	if len(normalizedRows) != 2 {
		t.Fatalf("expected 2 normalized rows after one batch, got %d", len(normalizedRows))
	}

	gotVersions := []int{normalizedRows[0].Version, normalizedRows[1].Version}
	if !(gotVersions[0] == fileOne.Version || gotVersions[0] == fileTwo.Version) {
		t.Fatalf("expected latest version first batch, got %#v", gotVersions)
	}
	if !(gotVersions[1] == fileOne.Version || gotVersions[1] == fileTwo.Version) {
		t.Fatalf("expected latest version first batch, got %#v", gotVersions)
	}
	if normalizedRows[0].Version == 1 || normalizedRows[1].Version == 1 {
		t.Fatalf("expected only latest-version rows in first batch, got %#v", gotVersions)
	}
}
