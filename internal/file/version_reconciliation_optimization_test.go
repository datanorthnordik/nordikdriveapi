package file

import (
	"testing"

	"gorm.io/datatypes"
)

func TestBulkRelinkRowReferences_UpdatesAllReferencedTables(t *testing.T) {
	db := newTestDB(t)

	user := UserForTest{
		ID:        1,
		Email:     "optimizer@example.com",
		FirstName: "Opt",
		LastName:  "Tester",
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	file := File{
		Filename:   "optimized.csv",
		InsertedBy: user.ID,
		Version:    1,
	}
	if err := db.Create(&file).Error; err != nil {
		t.Fatalf("seed file: %v", err)
	}

	requests := []FileEditRequest{
		{RowID: 10, UserID: user.ID, Status: fileEditRequestStatusCompleted, FileID: file.ID},
		{RowID: 20, UserID: user.ID, Status: fileEditRequestStatusPending, FileID: file.ID},
		{RowID: 30, UserID: user.ID, Status: fileEditRequestStatusPending, FileID: file.ID},
	}
	for i := range requests {
		if err := db.Create(&requests[i]).Error; err != nil {
			t.Fatalf("seed request %d: %v", i, err)
		}
		if err := db.Create(&FileEditRequestDetails{
			RequestID: requests[i].RequestID,
			FileID:    file.ID,
			Filename:  "before.csv",
			RowID:     requests[i].RowID,
			FieldName: "Last Names",
			OldValue:  "Old",
			NewValue:  "New",
			Status:    "approved",
		}).Error; err != nil {
			t.Fatalf("seed request detail %d: %v", i, err)
		}
		if err := db.Create(&FileEditRequestPhoto{
			RequestID: requests[i].RequestID,
			RowID:     requests[i].RowID,
			FileName:  "photo.jpg",
			PhotoURL:  "https://example.com/photo.jpg",
			SizeBytes: 123,
			FileID:    file.ID,
		}).Error; err != nil {
			t.Fatalf("seed request photo %d: %v", i, err)
		}
		if err := db.Create(&FormSubmissionForTest{
			ID:       int64(i + 1),
			FileID:   file.ID,
			RowID:    int64(requests[i].RowID),
			FileName: "before.csv",
			Status:   "pending",
		}).Error; err != nil {
			t.Fatalf("seed form submission %d: %v", i, err)
		}
	}

	rowIDMap := map[uint]uint{
		10: 101,
		20: 202,
	}
	if err := bulkRelinkRowReferences(db, file.ID, file.Filename, rowIDMap); err != nil {
		t.Fatalf("bulk relink row references: %v", err)
	}

	assertRequestRowID := func(requestID uint, want int) {
		t.Helper()
		var request FileEditRequest
		if err := db.First(&request, requestID).Error; err != nil {
			t.Fatalf("reload request %d: %v", requestID, err)
		}
		if request.RowID != want {
			t.Fatalf("request %d row_id = %d, want %d", requestID, request.RowID, want)
		}
	}
	assertDetailRowID := func(requestID uint, want int) {
		t.Helper()
		var detail FileEditRequestDetails
		if err := db.Where("request_id = ?", requestID).First(&detail).Error; err != nil {
			t.Fatalf("reload detail %d: %v", requestID, err)
		}
		if detail.RowID != want {
			t.Fatalf("detail %d row_id = %d, want %d", requestID, detail.RowID, want)
		}
	}
	assertPhotoRowID := func(requestID uint, want int) {
		t.Helper()
		var photo FileEditRequestPhoto
		if err := db.Where("request_id = ?", requestID).First(&photo).Error; err != nil {
			t.Fatalf("reload photo %d: %v", requestID, err)
		}
		if photo.RowID != want {
			t.Fatalf("photo %d row_id = %d, want %d", requestID, photo.RowID, want)
		}
	}
	assertSubmission := func(sourceRowID int64, wantRowID int64, wantFileName string) {
		t.Helper()
		var submission FormSubmissionForTest
		if err := db.Where("row_id = ?", wantRowID).First(&submission).Error; err != nil {
			t.Fatalf("reload submission for row_id %d: %v", wantRowID, err)
		}
		if submission.RowID != wantRowID {
			t.Fatalf("submission row_id = %d, want %d", submission.RowID, wantRowID)
		}
		if submission.FileName != wantFileName {
			t.Fatalf("submission file_name = %q, want %q", submission.FileName, wantFileName)
		}

		var oldCount int64
		if err := db.Model(&FormSubmissionForTest{}).Where("row_id = ?", sourceRowID).Count(&oldCount).Error; err != nil {
			t.Fatalf("count old submissions for row_id %d: %v", sourceRowID, err)
		}
		if sourceRowID != wantRowID && oldCount != 0 {
			t.Fatalf("expected old submission row_id %d to be cleared, found %d rows", sourceRowID, oldCount)
		}
	}

	assertRequestRowID(requests[0].RequestID, 101)
	assertRequestRowID(requests[1].RequestID, 202)
	assertRequestRowID(requests[2].RequestID, 30)

	assertDetailRowID(requests[0].RequestID, 101)
	assertDetailRowID(requests[1].RequestID, 202)
	assertDetailRowID(requests[2].RequestID, 30)

	assertPhotoRowID(requests[0].RequestID, 101)
	assertPhotoRowID(requests[1].RequestID, 202)
	assertPhotoRowID(requests[2].RequestID, 30)

	assertSubmission(10, 101, file.Filename)
	assertSubmission(20, 202, file.Filename)
	assertSubmission(30, 30, "before.csv")
}

func TestRunNormalizationSync_SkipsProcessingReconciliationVersions(t *testing.T) {
	db := newTestDB(t)

	user := UserForTest{
		ID:        1,
		Email:     "normalizer@example.com",
		FirstName: "Norm",
		LastName:  "Tester",
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	file := File{
		Filename:   "normalize.csv",
		InsertedBy: user.ID,
		Version:    1,
	}
	if err := db.Create(&file).Error; err != nil {
		t.Fatalf("seed file: %v", err)
	}

	if err := db.Create(&FileVersion{
		FileID:               file.ID,
		Filename:             file.Filename,
		InsertedBy:           user.ID,
		Version:              1,
		Rows:                 1,
		ColumnsOrder:         datatypes.JSON([]byte(`["name"]`)),
		ReconciliationStatus: fileVersionStatusReady,
	}).Error; err != nil {
		t.Fatalf("seed ready version: %v", err)
	}
	if err := db.Create(&FileVersion{
		FileID:               file.ID,
		Filename:             file.Filename,
		InsertedBy:           user.ID,
		Version:              2,
		Rows:                 1,
		ColumnsOrder:         datatypes.JSON([]byte(`["name"]`)),
		ReconciliationStatus: fileVersionStatusProcessing,
		TransitionOperation:  versionTransitionOperationReplace,
	}).Error; err != nil {
		t.Fatalf("seed processing version: %v", err)
	}

	readyRow := FileData{
		FileID:     file.ID,
		RowData:    datatypes.JSON([]byte(`{"name":"ready"}`)),
		InsertedBy: user.ID,
		Version:    1,
	}
	if err := db.Create(&readyRow).Error; err != nil {
		t.Fatalf("seed ready row: %v", err)
	}
	processingRow := FileData{
		FileID:     file.ID,
		RowData:    datatypes.JSON([]byte(`{"name":"processing"}`)),
		InsertedBy: user.ID,
		Version:    2,
	}
	if err := db.Create(&processingRow).Error; err != nil {
		t.Fatalf("seed processing row: %v", err)
	}

	fileID := file.ID
	result, err := RunNormalizationSync(db, NormalizationSyncOptions{
		FileID:     &fileID,
		BatchSize:  10,
		MaxBatches: 1,
	})
	if err != nil {
		t.Fatalf("run normalization sync: %v", err)
	}
	if result.Processed != 1 || result.Inserted != 1 {
		t.Fatalf("expected only ready rows to normalize, got %#v", result)
	}

	var records []FileDataNormalized
	if err := db.Order("source_row_id ASC").Find(&records).Error; err != nil {
		t.Fatalf("load normalized records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 normalized record, got %d", len(records))
	}
	if records[0].SourceRowID != readyRow.ID {
		t.Fatalf("normalized source_row_id = %d, want %d", records[0].SourceRowID, readyRow.ID)
	}
	if records[0].Version != 1 {
		t.Fatalf("normalized version = %d, want 1", records[0].Version)
	}
}
