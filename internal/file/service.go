package file

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"nordik-drive-api/internal/auth"
	"nordik-drive-api/internal/util"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/iancoleman/orderedmap"
	"github.com/xuri/excelize/v2"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type FileService struct {
	DB *gorm.DB
}

func (fs *FileService) SaveFilesMultipart(uploadedFiles []*multipart.FileHeader, filenames FileUploadInput, userID uint) ([]File, error) {
	var savedFiles []File
	files := filenames.FileNames
	privateList := filenames.Private
	communityFilterList := filenames.CommunityFilter

	for i, fileHeader := range uploadedFiles {
		filename := files[i]
		private := privateList[i]
		communityFilter := communityFilterList[i]

		// Check for duplicate filename
		var existing File
		if err := fs.DB.Where("filename = ?", filename).First(&existing).Error; err == nil {
			return nil, fmt.Errorf("file with name %s already exists", filename)
		}

		f, err := fileHeader.Open()
		if err != nil {
			return nil, err
		}
		defer f.Close()

		ext := filepath.Ext(fileHeader.Filename)
		var headers []string
		var dataRows [][]string

		if ext == ".xlsx" || ext == ".xls" {
			headers, dataRows, err = parseExcelReader(f)
		} else if ext == ".csv" {
			headers, dataRows, err = parseCSVReader(f)
		} else {
			return nil, fmt.Errorf("unsupported file type: %s", ext)
		}
		if err != nil {
			return nil, err
		}

		// Save File entry with column order
		headersJSON, _ := json.Marshal(headers)
		newFile := File{
			Filename:        filename,
			InsertedBy:      userID,
			CreatedAt:       time.Now(),
			Private:         private,
			Version:         1,
			IsDelete:        false,
			Rows:            len(dataRows),
			Size:            float64(fileHeader.Size) / 1024.0,
			ColumnsOrder:    headersJSON, // store headers
			CommunityFilter: communityFilter,
		}
		if err := fs.DB.Create(&newFile).Error; err != nil {
			return nil, err
		}

		// Save FileVersion entry
		fileVersion := FileVersion{
			FileID:     newFile.ID,
			Filename:   filename,
			InsertedBy: userID,
			CreatedAt:  time.Now(),
			Private:    private,
			Version:    1,
			IsDelete:   false,
			Rows:       len(dataRows),
			Size:       float64(fileHeader.Size) / 1024.0,
		}
		if err := fs.DB.Create(&fileVersion).Error; err != nil {
			return nil, err
		}

		// Save each row as normal JSON
		for _, row := range dataRows {
			rowMap := make(map[string]string)
			for j, header := range headers {
				val := ""
				if j < len(row) {
					val = row[j]
				}
				rowMap[header] = val
			}

			jsonBytes, _ := json.Marshal(rowMap)

			record := FileData{
				FileID:     newFile.ID,
				RowData:    datatypes.JSON(jsonBytes),
				InsertedBy: userID,
				CreatedAt:  time.Now(),
				Version:    1,
			}

			if err := fs.DB.Create(&record).Error; err != nil {
				return nil, err
			}
		}

		savedFiles = append(savedFiles, newFile)
	}

	return savedFiles, nil
}

// parseExcelReader preserves original Excel column order
func parseExcelReader(file multipart.File) ([]string, [][]string, error) {
	defer file.Seek(0, 0)

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(file); err != nil {
		return nil, nil, fmt.Errorf("failed to read excel file: %w", err)
	}

	f, err := excelize.OpenReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse excel file: %w", err)
	}

	sheetName := f.GetSheetName(0)
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read rows: %w", err)
	}
	if len(rows) < 1 {
		return nil, nil, fmt.Errorf("excel file is empty")
	}

	headers := rows[0]
	var dataRows [][]string
	colorToSource := map[string]string{
		"#FFFF00": "FURTHER INVESTIGATION REQUIRED",
		"#FFC000": "NCTR SOURCE",
		"#FF0000": "CORONER'S OFFICE SOURCE",
		"#00B0F0": "BAND DOCUMENTS",
		"#7030A0": "CIRNAC SOURCE",
		"#00B050": "OFFICE OF THE REGISTRAR GENERAL",
	}

	for rowIdx, _ := range rows[1:] {
		newRow := make([]string, len(headers))
		for colIdx := range headers {
			cellRef, _ := excelize.CoordinatesToCellName(colIdx+1, rowIdx+2)
			val, _ := f.GetCellValue(sheetName, cellRef)
			if styleID, err := f.GetCellStyle(sheetName, cellRef); err == nil && styleID != 0 {
				if style, err := f.GetStyle(styleID); err == nil && style != nil {
					if len(style.Fill.Color) > 0 {
						rawColor := style.Fill.Color[0]
						hex := normalizeColorHex(rawColor)
						if src, ok := colorToSource[hex]; ok && val != "" {
							// Append source in brackets (no duplicate-avoid logic per request)
							val = fmt.Sprintf("%s (%s)", val, src)
						}
					}
				}
			}
			newRow[colIdx] = val
		}
		dataRows = append(dataRows, newRow)
	}

	return headers, dataRows, nil
}

func (fs *FileService) ReplaceFiles(uploadedFile *multipart.FileHeader, fileID uint, userID uint) error {
	var existing File
	if err := fs.DB.First(&existing, fileID).Error; err != nil {
		return fmt.Errorf("file not found: %w", err)
	}

	f, err := uploadedFile.Open()
	if err != nil {
		return err
	}
	defer f.Close()

	// 3. Parse file
	ext := filepath.Ext(uploadedFile.Filename)
	var headers []string
	var dataRows [][]string

	if ext == ".xlsx" || ext == ".xls" {
		headers, dataRows, err = parseExcelReader(f)
	} else if ext == ".csv" {
		headers, dataRows, err = parseCSVReader(f)
	} else {
		return fmt.Errorf("unsupported file type: %s", ext)
	}
	if err != nil {
		return err
	}

	sizeInBytes := uploadedFile.Size
	sizeInKB := float64(sizeInBytes) / 1024.0
	newVersion := existing.Version + 1

	// 4. Update file metadata (only certain fields, keep same ID)
	existing.Version = newVersion
	existing.Rows = len(dataRows)
	existing.Size = sizeInKB

	if err := fs.DB.Save(&existing).Error; err != nil {
		return err
	}

	// 5. Insert into FileVersion table
	fileVersion := FileVersion{
		FileID:     existing.ID,
		Filename:   existing.Filename,
		InsertedBy: userID,
		CreatedAt:  time.Now(),
		Private:    existing.Private,
		Version:    newVersion,
		IsDelete:   false,
		Rows:       len(dataRows),
		Size:       sizeInKB,
	}
	if err := fs.DB.Create(&fileVersion).Error; err != nil {
		return err
	}

	for _, row := range dataRows {
		recordMap := make(map[string]string)
		for j, header := range headers {
			if j < len(row) {
				recordMap[header] = row[j]
			} else {
				recordMap[header] = ""
			}
		}

		jsonBytes, err := json.Marshal(recordMap)
		if err != nil {
			return err
		}

		record := FileData{
			FileID:     existing.ID,
			RowData:    jsonBytes,
			InsertedBy: userID,
			CreatedAt:  time.Now(),
			Version:    newVersion,
		}
		if err := fs.DB.Create(&record).Error; err != nil {
			return err
		}
	}

	return nil
}

func (fs *FileService) GetUserRole(userID uint) (string, error) {
	var user auth.Auth
	if err := fs.DB.First(&user, userID).Error; err != nil {
		return "", err
	}
	return user.Role, nil
}

func (fs *FileService) GetAllFiles(userID uint, role string) ([]FileWithUser, error) {
	var files []FileWithUser

	if role == "Admin" {
		// Admin → all files with uploader info
		if err := fs.DB.
			Table("file f").
			Select("f.*, u.firstname, u.lastname").
			Joins("LEFT JOIN users u ON u.id = f.inserted_by").
			Scan(&files).Error; err != nil {
			return nil, err
		}
		return files, nil
	}

	// User → public files OR private files they have access to
	err := fs.DB.
		Raw(`
			SELECT f.*, u.firstname, u.lastname
			FROM file f
			LEFT JOIN users u ON u.id = f.inserted_by
			LEFT JOIN file_access fa ON f.id = fa.file_id AND fa.user_id = ?
			WHERE f.private = false OR (fa.user_id = ? AND f.is_delete = ?)
		`, userID, userID, false).
		Scan(&files).Error

	if err != nil {
		return nil, err
	}

	return files, nil
}

func (fs *FileService) GetFileData(filename string, version int) ([]FileData, error) {
	var file File

	// Fetch file by filename
	if err := fs.DB.Where("filename = ? AND is_delete = ?", filename, false).First(&file).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// Unmarshal the column order
	var columnsOrder []string
	if err := json.Unmarshal(file.ColumnsOrder, &columnsOrder); err != nil {
		return nil, fmt.Errorf("failed to unmarshal columns order: %w", err)
	}

	var fileData []FileData
	if err := fs.DB.Where("file_id = ? AND version = ?", file.ID, version).
		Order("id ASC"). // preserve row insertion order
		Find(&fileData).Error; err != nil {
		return nil, err
	}

	// Reorder each row's JSON according to ColumnsOrder
	for i := range fileData {
		var rowMap map[string]interface{}
		if err := json.Unmarshal(fileData[i].RowData, &rowMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal row: %w", err)
		}

		orderedRow := orderedmap.New()
		for _, col := range columnsOrder {
			if val, exists := rowMap[col]; exists {
				orderedRow.Set(col, val)
			} else {
				orderedRow.Set(col, "") // fill missing columns
			}
		}

		// Marshal back to JSON
		jsonBytes, err := orderedRow.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal ordered row: %w", err)
		}

		fileData[i].RowData = datatypes.JSON(jsonBytes)
	}

	return fileData, nil
}

// ...existing code...
// normalizeColorHex converts excelize color strings (ARGB/RGB/short) into "#RRGGBB" uppercase.
func normalizeColorHex(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	// remove common prefixes
	raw = strings.TrimPrefix(raw, "0x")
	raw = strings.TrimPrefix(raw, "#")
	raw = strings.ToUpper(raw)

	// If ARGB (8 chars) drop the leading alpha (first two chars)
	if len(raw) == 8 {
		raw = raw[2:]
	}
	// If 6 chars -> use as is
	if len(raw) == 6 {
		return "#" + raw
	}
	// If 3 chars -> expand to 6
	if len(raw) == 3 {
		return fmt.Sprintf("#%c%c%c%c%c%c", raw[0], raw[0], raw[1], raw[1], raw[2], raw[2])
	}
	// fallback: pad or trim to 6
	if len(raw) < 6 {
		raw = raw + strings.Repeat("0", 6-len(raw))
		return "#" + raw
	}
	return "#" + raw[:6]
}

// ...existing code...

func (fs *FileService) DeleteFile(fileID string) (File, error) {
	// Check if file exists
	var file File
	if err := fs.DB.Where("id = ?", fileID).First(&file).Error; err != nil {
		return file, err
	}

	// Soft delete: just mark is_delete = true
	if err := fs.DB.Model(&file).Update("is_delete", true).Error; err != nil {
		return file, err
	}

	return file, nil
}

func (fs *FileService) ResetFile(fileID string) (File, error) {
	var file File
	if err := fs.DB.Where("id = ?", fileID).First(&file).Error; err != nil {
		return file, err
	}

	// Reset soft delete: mark is_delete = false
	if err := fs.DB.Model(&file).Update("is_delete", false).Error; err != nil {
		return file, err
	}

	return file, nil
}

// parseCSVReader reads CSV file from multipart.File and returns headers + data rows
func parseCSVReader(file multipart.File) ([]string, [][]string, error) {
	defer file.Seek(0, 0) // reset file pointer if needed

	reader := csv.NewReader(file)
	allRows, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read csv file: %w", err)
	}

	if len(allRows) < 1 {
		return nil, nil, fmt.Errorf("csv file is empty")
	}

	headers := allRows[0]
	dataRows := allRows[1:]

	return headers, dataRows, nil
}

func (fs *FileService) CreateAccess(input []FileAccess) error {
	if err := fs.DB.Create(&input).Error; err != nil {
		return err
	}
	return nil
}

func (fs *FileService) DeleteAccess(accessId string) error {
	// Check if access record exists
	var access FileAccess
	if err := fs.DB.Where("id = ?", accessId).First(&access).Error; err != nil {
		return err
	}

	// Delete access record
	if err := fs.DB.Delete(&access).Error; err != nil {
		return err
	}

	return nil
}

func (fs *FileService) GetFileAccess(fileId string) ([]FileAccessWithUser, error) {
	var results []FileAccessWithUser

	err := fs.DB.Table("file_access").
		Select("file_access.id, file_access.user_id, file_access.file_id, users.firstname, users.lastname").
		Joins("JOIN users ON users.id = file_access.user_id").
		Where("file_access.file_id = ?", fileId).
		Scan(&results).Error

	if err != nil {
		return nil, err
	}

	return results, nil
}

func (fs *FileService) GetFileHistory(fileId string) ([]FileVersionWithUser, error) {
	var results []FileVersionWithUser

	err := fs.DB.Table("file_version").
		Select(`file_version.id, file_version.file_id, file_version.filename, 
		        users.firstname AS firstname, users.lastname AS lastname,
		        file_version.created_at, file_version.private, file_version.is_delete,
		        file_version.size, file_version.version, file_version.rows`).
		Joins("JOIN users ON users.id = file_version.inserted_by").
		Where("file_version.file_id = ?", fileId).
		Order("file_version.version DESC").
		Scan(&results).Error

	if err != nil {
		return nil, err
	}

	return results, nil
}

func (fs *FileService) RevertFile(filename string, version int, userID uint) error {
	var file File
	if err := fs.DB.Where("filename = ?", filename).First(&file).Error; err != nil {
		return fmt.Errorf("file not found: %w", err)
	}

	// get target version from file_version
	var targetVersion FileVersion
	if err := fs.DB.Where("file_id = ? AND version = ?", file.ID, version).First(&targetVersion).Error; err != nil {
		return fmt.Errorf("target version not found: %w", err)
	}

	// new version number
	newVersion := file.Version + 1

	// update file table to new version
	if err := fs.DB.Model(&file).Updates(File{
		Version: newVersion,
		Rows:    targetVersion.Rows,
		Size:    targetVersion.Size,
		Private: targetVersion.Private,
	}).Error; err != nil {
		return err
	}

	// insert new row in file_version
	newFileVersion := FileVersion{
		FileID:     file.ID,
		Filename:   filename,
		InsertedBy: userID,
		CreatedAt:  time.Now(),
		Private:    targetVersion.Private,
		Version:    newVersion,
		IsDelete:   false,
		Rows:       targetVersion.Rows,
		Size:       targetVersion.Size,
	}
	if err := fs.DB.Create(&newFileVersion).Error; err != nil {
		return err
	}

	// copy file_data rows of target version into new version
	var dataRows []FileData
	if err := fs.DB.Where("file_id = ? AND version = ?", file.ID, version).Find(&dataRows).Error; err != nil {
		return err
	}

	for _, row := range dataRows {
		newRow := FileData{
			FileID:     file.ID,
			RowData:    row.RowData,
			InsertedBy: userID,
			CreatedAt:  time.Now(),
			Version:    newVersion,
		}
		if err := fs.DB.Create(&newRow).Error; err != nil {
			return err
		}
	}

	return nil
}

func (fs *FileService) CreateEditRequest(input EditRequestInput, userID uint) (*FileEditRequest, error) {

	// Step 1: Insert main request
	request := FileEditRequest{
		UserID:            userID,
		Status:            "pending",
		CreatedAt:         time.Now(),
		FirstName:         input.FirstName,
		LastName:          input.LastName,
		Consent:           input.Consent,
		ArchiveConsent:    input.ArchiveConsent,
		RowID:             input.RowID,
		IsEdited:          input.IsEdited,
		FileID:            input.FileID,
		Community:         input.Community,
		UploaderCommunity: input.UploaderCommunity,
	}

	// Keep as-is; if you ever see bool issues, use Select("*") here.
	if err := fs.DB.Create(&request).Error; err != nil {
		return nil, err
	}

	// Step 2: Insert change details
	var details []FileEditRequestDetails
	now := time.Now()
	rowId := input.RowID

	for _, edits := range input.Changes {
		for _, item := range edits {
			details = append(details, FileEditRequestDetails{
				RequestID: request.RequestID,
				FileID:    input.FileID,
				Filename:  input.Filename,
				RowID:     item.RowID,
				FieldName: item.FieldName,
				OldValue:  item.OldValue,
				NewValue:  item.NewValue,
				CreatedAt: now,
			})
		}
	}

	if len(details) > 0 {
		if err := fs.DB.Create(&details).Error; err != nil {
			return nil, err
		}
	}

	// Step 3: Upload images to GCS
	bucket := "nordik-drive-photos"
	timestamp := time.Now().Format("20060102150405")

	// ✅ Folder rule:
	// - if is_edited=false OR row_id==0 => requests/<requestID>_<first>_<last>
	// - else => requests/<row_id>
	var basePrefix string
	if input.IsEdited && rowId != 0 {
		basePrefix = util.RowPrefix(rowId)
	} else {
		basePrefix = util.TempPrefix(request.RequestID, input.FirstName, input.LastName)
	}

	// App photos
	for i, base64Img := range input.PhotosInApp {

		fileName := fmt.Sprintf("%s_%s_%s_%d.jpg",
			input.FirstName,
			input.LastName,
			timestamp,
			i+1,
		)

		objectPath := fmt.Sprintf("%s/%s", basePrefix, fileName)

		url, sizeBytes, err := util.UploadPhotoToGCS(base64Img, bucket, objectPath)
		if err != nil {
			return nil, err
		}

		photoRecord := FileEditRequestPhoto{
			RequestID:        request.RequestID,
			RowID:            rowId, // 0 for new request, as you want
			PhotoURL:         url,
			FileName:         fileName,
			SizeBytes:        sizeBytes,
			IsGalleryPhoto:   false,
			IsApproved:       false,
			CreatedAt:        time.Now(),
			SourceFile:       input.Filename,
			FileID:           input.FileID,
			DocumentType:     "photos",
			DocumentCategory: "",
		}

		if err := fs.DB.Create(&photoRecord).Error; err != nil {
			return nil, err
		}
	}

	// Gallery photos
	for i, base64Img := range input.PhotosForGallery {

		fileName := fmt.Sprintf("%s_%s_%s_gallery_%d.jpg",
			input.FirstName,
			input.LastName,
			timestamp,
			i+1,
		)

		objectPath := fmt.Sprintf("%s/%s", basePrefix, fileName)

		url, sizeBytes, err := util.UploadPhotoToGCS(base64Img, bucket, objectPath)
		if err != nil {
			return nil, err
		}

		photoRecord := FileEditRequestPhoto{
			RequestID:        request.RequestID,
			RowID:            rowId, // 0 for new request
			PhotoURL:         url,
			FileName:         fileName,
			SizeBytes:        sizeBytes,
			IsGalleryPhoto:   true,
			IsApproved:       false,
			CreatedAt:        time.Now(),
			SourceFile:       input.Filename,
			FileID:           input.FileID,
			DocumentType:     "photos",
			DocumentCategory: "",
		}

		if err := fs.DB.Create(&photoRecord).Error; err != nil {
			return nil, err
		}
	}

	for i, doc := range input.Documents {
		// only accept document_type=document (ignore photos if any client sends)
		docType := doc.DocumentType
		if docType == "" {
			docType = "document"
		}
		if docType != "document" {
			continue
		}

		// name + path
		safeCategory := doc.DocumentCategory
		if safeCategory == "" {
			safeCategory = "other_document"
		}

		fileName := fmt.Sprintf("%s_%s_%s_doc_%d_%s",
			input.FirstName,
			input.LastName,
			timestamp,
			i+1,
			doc.Filename,
		)

		objectPath := fmt.Sprintf("%s/%s", basePrefix, fileName)

		// ✅ You can reuse UploadPhotoToGCS if it supports any base64 mime.
		// Better rename it to UploadBase64ToGCS.
		url, sizeBytes, err := util.UploadPhotoToGCS(doc.DataBase64, bucket, objectPath)
		if err != nil {
			return nil, err
		}

		rec := FileEditRequestPhoto{
			RequestID:      request.RequestID,
			RowID:          rowId,
			PhotoURL:       url,
			FileName:       fileName,
			SizeBytes:      sizeBytes,
			IsGalleryPhoto: false,
			IsApproved:     false,
			CreatedAt:      time.Now(),
			SourceFile:     input.Filename,
			FileID:         input.FileID,

			DocumentType:     "document",
			DocumentCategory: safeCategory,
		}

		if err := fs.DB.Create(&rec).Error; err != nil {
			return nil, err
		}
	}

	return &request, nil
}

func parseStatuses(csv string) []string {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil
	}

	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}

	for _, p := range parts {
		s := strings.ToLower(strings.TrimSpace(p))
		if s == "" {
			continue
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (fs *FileService) GetEditRequests(statusCSV *string, userID *uint) ([]FileEditRequestWithUser, error) {
	var baseRequests []struct {
		RequestID  uint
		RowID      int
		UserID     uint
		Firstname  string
		Lastname   string
		Status     string
		CreatedAt  time.Time
		EFirstName string `gorm:"column:efirstname"`
		ELastName  string `gorm:"column:elastname"`
		IsEdited   bool   `gorm:"default:true"`
		Consent    bool
		FileID     uint `gorm:"column:file_id"`
	}

	q := fs.DB.Table("file_edit_request").
		Select(`
			file_edit_request.request_id,
			file_edit_request.row_id,
			file_edit_request.user_id,
			users.firstname,
			users.lastname,
			file_edit_request.status,
			file_edit_request.created_at,
			file_edit_request.firstname as efirstname,
			file_edit_request.lastname as elastname,
			file_edit_request.is_edited,
			file_edit_request.consent,
			file_edit_request.file_id
		`).
		Joins("JOIN users ON users.id = file_edit_request.user_id").
		Order("file_edit_request.created_at DESC")

	// ✅ Only if BOTH provided: filter by status IN (...) + user_id
	if statusCSV != nil && strings.TrimSpace(*statusCSV) != "" && userID != nil && *userID > 0 {
		statuses := parseStatuses(*statusCSV)
		if len(statuses) > 0 {
			q = q.Where("file_edit_request.user_id = ?", *userID).
				Where("file_edit_request.status IN ?", statuses)
		} else {
			// If statusCSV was garbage like ",,,", just fall back to pending
			q = q.Where("file_edit_request.status = ?", "pending")
		}
	} else {
		// ✅ Default behavior: pending only (exactly like today)
		q = q.Where("file_edit_request.status = ?", "pending")
	}

	if err := q.Scan(&baseRequests).Error; err != nil {
		return nil, err
	}

	final := make([]FileEditRequestWithUser, 0, len(baseRequests))

	for _, req := range baseRequests {
		var details []FileEditRequestDetails
		if err := fs.DB.Where("request_id = ?", req.RequestID).
			Order("id ASC").
			Find(&details).Error; err != nil {
			return nil, err
		}

		final = append(final, FileEditRequestWithUser{
			RequestID:  req.RequestID,
			UserID:     req.UserID,
			Firstname:  req.Firstname,
			Lastname:   req.Lastname,
			Status:     req.Status,
			CreatedAt:  req.CreatedAt,
			Details:    details,
			EFirstName: req.EFirstName,
			ELastName:  req.ELastName,
			RowID:      req.RowID,
			IsEdited:   req.IsEdited,
			Consent:    req.Consent,
		})
	}

	return final, nil
}

func (fs *FileService) ApproveEditRequest(requestID uint, updates []FileEditRequestDetails, userId uint) error {
	return fs.DB.Transaction(func(tx *gorm.DB) error {

		// 0) Update changed details (new_value) coming from UI
		for _, upd := range updates {
			if err := tx.Model(&FileEditRequestDetails{}).
				Where("id = ?", upd.ID).
				Update("new_value", upd.NewValue).Error; err != nil {
				return err
			}
		}

		// 1) Load request (need is_edited + name for folder)
		var req FileEditRequest
		if err := tx.Where("request_id = ?", requestID).First(&req).Error; err != nil {
			return err
		}

		// 2) Fetch all details for this request
		var allDetails []FileEditRequestDetails
		if err := tx.Where("request_id = ?", requestID).Find(&allDetails).Error; err != nil {
			return err
		}

		// 3) If is_edited=false => create new FileData row FIRST, use its ID as rowID
		var finalRowID uint
		if !req.IsEdited {

			// Build row_data JSON from details (field -> new_value)
			row := make(map[string]string, len(allDetails))
			for _, d := range allDetails {
				row[d.FieldName] = d.NewValue
			}

			newJSON, err := json.Marshal(row)
			if err != nil {
				return fmt.Errorf("failed to build row_data json: %v", err)
			}

			// Create file_data row (row_id will be new FileData.ID)
			fileID := allDetails[0].FileID

			fd := FileData{
				FileID:     fileID,
				RowData:    datatypes.JSON(newJSON),
				InsertedBy: req.UserID, // keeps your current meaning (creator)
				CreatedAt:  time.Now(),
				Version:    1,
			}

			if err := tx.Create(&fd).Error; err != nil {
				return fmt.Errorf("failed to insert file_data: %v", err)
			}

			finalRowID = fd.ID

			// Update ALL details row_id (they were 0) to new row id
			if err := tx.Model(&FileEditRequestDetails{}).
				Where("request_id = ?", requestID).
				Update("row_id", finalRowID).Error; err != nil {
				return err
			}

			// Update request row_id (optional but usually needed)
			if err := tx.Model(&FileEditRequest{}).
				Where("request_id = ?", requestID).
				Update("row_id", finalRowID).Error; err != nil {
				return err
			}

			// ---- Move photos folder: requests/<requestID>_<first>_<last> -> requests/<rowID> ----
			bucket := "nordik-drive-photos"

			srcPrefix := util.TempPrefix(req.RequestID, req.FirstName, req.LastName)
			dstPrefix := util.RowPrefix(int(finalRowID))

			mapping, err := util.MoveGCSFolder(bucket, srcPrefix, dstPrefix)
			if err != nil {
				return err
			}

			// Update photo rows: row_id + photo_url
			var photos []FileEditRequestPhoto
			if err := tx.Where("request_id = ?", requestID).Find(&photos).Error; err != nil {
				return err
			}

			for _, p := range photos {

				// ✅ old object path is deterministic, no URL parsing needed
				oldObj := fmt.Sprintf("%s/%s", srcPrefix, p.FileName)

				// new object path comes from MoveGCSFolder mapping
				newObj, ok := mapping[oldObj]
				if !ok {
					// fallback: assume it should exist in the destination folder
					newObj = fmt.Sprintf("%s/%s", dstPrefix, p.FileName)
				}

				// ✅ force gs:// format (what you want)
				newURL := fmt.Sprintf("gs://%s/%s", bucket, newObj)

				if err := tx.Model(&FileEditRequestPhoto{}).
					Where("id = ?", p.ID).
					Updates(map[string]any{
						"row_id":    finalRowID,
						"photo_url": newURL,
					}).Error; err != nil {
					return err
				}
			}

			// Since it’s a NEW row, we’re done with updating file_data (already inserted with all fields)
		} else {
			// 4) Existing edit: your current logic (update the existing file_data row)
			for _, det := range allDetails {

				var fileData FileData
				err := tx.Where("file_id = ? AND id = ?", det.FileID, det.RowID).First(&fileData).Error
				if err != nil {
					return fmt.Errorf("file data row not found for file %d row %d", det.FileID, det.RowID)
				}

				var row map[string]string
				if err := json.Unmarshal(fileData.RowData, &row); err != nil {
					return fmt.Errorf("failed to parse row_data: %v", err)
				}

				row[det.FieldName] = det.NewValue

				newJSON, _ := json.Marshal(row)
				if err := tx.Model(&fileData).
					Update("row_data", datatypes.JSON(newJSON)).Error; err != nil {
					return fmt.Errorf("failed to update file_data: %v", err)
				}
			}
		}

		if err := tx.Model(&FileEditRequest{}).
			Where("request_id = ?", requestID).
			Updates(map[string]interface{}{
				"status":      "approved",
				"approved_by": userId,
			}).Error; err != nil {
			return err
		}

		return nil
	})
}

func (fs *FileService) GetPhotosByRequest(requestID uint) ([]FileEditRequestPhoto, error) {
	var photos []FileEditRequestPhoto

	if err := fs.DB.Where("request_id = ? AND document_type = ?", requestID, "photos").Find(&photos).Error; err != nil {
		return nil, err
	}

	return photos, nil
}

func (fs *FileService) GetDocsByRequest(requestID uint) ([]FileEditRequestPhoto, error) {
	var documents []FileEditRequestPhoto

	if err := fs.DB.Where("request_id = ? AND document_type = ?", requestID, "document").Find(&documents).Error; err != nil {
		return nil, err
	}
	return documents, nil
}

func (fs *FileService) GetPhotosByRow(requestID uint) ([]FileEditRequestPhoto, error) {
	var photos []FileEditRequestPhoto

	if err := fs.DB.Where("row_id = ? and is_approved = ? and document_type = ?", requestID, true, "photos").Find(&photos).Error; err != nil {
		return nil, err
	}

	return photos, nil
}

func (fs *FileService) GetDocsByRow(requestID uint) ([]FileEditRequestPhoto, error) {
	var documents []FileEditRequestPhoto

	if err := fs.DB.Where("row_id = ? and is_approved = ? and document_type = ?", requestID, true, "document").Find(&documents).Error; err != nil {
		return nil, err
	}
	return documents, nil
}

func (fs *FileService) GetPhotoBytes(photoID uint) ([]byte, string, error) {
	var photo FileEditRequestPhoto

	// 1. Fetch photo record
	if err := fs.DB.First(&photo, photoID).Error; err != nil {
		return nil, "", err
	}

	bucketName := os.Getenv("BUCKET_NAME")

	// 2. Extract object path from: gs://bucket/OBJECT_PATH
	prefix := "gs://" + bucketName + "/"
	objectPath := strings.TrimPrefix(photo.PhotoURL, prefix)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, "", err
	}
	defer client.Close()

	// 3. Read the file
	rc, err := client.Bucket(bucketName).Object(objectPath).NewReader(ctx)
	if err != nil {
		return nil, "", err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, "", err
	}

	contentType := rc.ContentType()

	return data, contentType, nil
}

func (fs *FileService) ReviewPhotos(approved []uint, rejected []uint, reviewer string) error {

	now := time.Now()

	// -------------------------------
	// APPROVE PHOTOS
	// -------------------------------
	if len(approved) > 0 {
		err := fs.DB.Model(&FileEditRequestPhoto{}).
			Where("id IN ?", approved).
			Updates(map[string]interface{}{
				"is_approved": true,
				"approved_by": reviewer,
				"approved_at": now,
			}).Error

		if err != nil {
			return fmt.Errorf("failed to approve photos: %v", err)
		}
	}

	// -------------------------------
	// REJECT PHOTOS
	// -------------------------------
	if len(rejected) > 0 {
		err := fs.DB.Model(&FileEditRequestPhoto{}).
			Where("id IN ?", rejected).
			Updates(map[string]interface{}{
				"is_approved": false,
				"approved_by": reviewer,
				"approved_at": now,
			}).Error

		if err != nil {
			return fmt.Errorf("failed to reject photos: %v", err)
		}
	}

	return nil
}

func (fs *FileService) GetDocBytes(docID uint) ([]byte, string, string, error) {
	var doc FileEditRequestPhoto

	// 1) Fetch record (docs are stored in file_edit_request_photos)
	if err := fs.DB.First(&doc, docID).Error; err != nil {
		return nil, "", "", err
	}

	// Optional safety: ensure it's a "document"
	// (remove if you want same endpoint to serve both)
	if doc.DocumentType != "document" {
		return nil, "", "", fmt.Errorf("requested item is not a document")
	}

	bucketName := os.Getenv("BUCKET_NAME")
	if bucketName == "" {
		return nil, "", "", fmt.Errorf("BUCKET_NAME env not set")
	}

	// 2) Extract object path from gs://bucket/OBJECT_PATH
	prefix := "gs://" + bucketName + "/"
	objectPath := strings.TrimPrefix(doc.PhotoURL, prefix)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, "", "", err
	}
	defer client.Close()

	// 3) Read file from GCS
	rc, err := client.Bucket(bucketName).Object(objectPath).NewReader(ctx)
	if err != nil {
		return nil, "", "", err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, "", "", err
	}

	contentType := rc.ContentType()
	if contentType == "" {
		// fallback (optional) if GCS contentType missing
		contentType = http.DetectContentType(data)
	}

	// Prefer DB filename if present
	filename := doc.FileName
	if filename == "" {
		filename = path.Base(objectPath)
	}

	return data, contentType, filename, nil
}

func (h *gcsReadHandle) Close() error {
	if h.Reader != nil {
		_ = h.Reader.Close()
	}
	if h.Client != nil {
		_ = h.Client.Close()
	}
	return nil
}

// OpenMediaHandle opens a streaming reader for a FileEditRequestPhoto row by its ID.
// kind is optional; if provided it enforces type ("photo" => DocumentType must be "photos",
// "doc"/"document" => DocumentType must be "document")
func (fs *FileService) OpenMediaHandle(ctx context.Context, id uint, kind string) (*gcsReadHandle, string, string, string, error) {
	var rec FileEditRequestPhoto
	if err := fs.DB.First(&rec, id).Error; err != nil {
		return nil, "", "", "", err
	}

	// Optional guard on kind
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind != "" {
		if kind == "photo" && rec.DocumentType != "photos" {
			return nil, "", "", "", fmt.Errorf("requested item is not a photo")
		}
		if (kind == "doc" || kind == "document") && rec.DocumentType != "document" {
			return nil, "", "", "", fmt.Errorf("requested item is not a document")
		}
	}

	// Parse gs://bucket/object
	bucketFromURL, objectPath, err := parseGSURL(rec.PhotoURL)
	if err != nil {
		return nil, "", "", "", err
	}

	// If BUCKET_NAME is set, prefer it ONLY if URL bucket is empty (normally URL bucket exists)
	bucketName := bucketFromURL
	if bucketName == "" {
		bucketName = os.Getenv("BUCKET_NAME")
	}
	if bucketName == "" {
		return nil, "", "", "", fmt.Errorf("bucket name not found (gs url + BUCKET_NAME empty)")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, "", "", "", err
	}

	reader, err := client.Bucket(bucketName).Object(objectPath).NewReader(ctx)
	if err != nil {
		_ = client.Close()
		return nil, "", "", "", err
	}

	contentType := reader.ContentType()
	if contentType == "" {
		// controller will sniff if needed
		contentType = ""
	}

	filename := strings.TrimSpace(rec.FileName)
	if filename == "" {
		filename = path.Base(objectPath)
		if filename == "" {
			filename = fmt.Sprintf("file_%d", rec.ID)
		}
	}

	disposition := "attachment"
	if strings.HasPrefix(contentType, "image/") || contentType == "application/pdf" {
		disposition = "inline"
	}

	// If contentType is still empty, controller will sniff and recompute disposition

	return &gcsReadHandle{
		Client: client,
		Reader: reader,
	}, filename, contentType, disposition, nil
}

// ✅ Missing helper you asked for: readFromGCS
// Use this when you want full bytes (not recommended for huge files).
func (fs *FileService) readFromGCS(gsURL string, dbFilename string) ([]byte, string, string, error) {
	bucket, objectPath, err := parseGSURL(gsURL)
	if err != nil {
		return nil, "", "", err
	}
	if bucket == "" {
		bucket = os.Getenv("BUCKET_NAME")
	}
	if bucket == "" {
		return nil, "", "", fmt.Errorf("bucket name not found (gs url + BUCKET_NAME empty)")
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, "", "", err
	}
	defer client.Close()

	rc, err := client.Bucket(bucket).Object(objectPath).NewReader(ctx)
	if err != nil {
		return nil, "", "", err
	}
	defer rc.Close()

	data, err := ioReadAll(rc)
	if err != nil {
		return nil, "", "", err
	}

	contentType := rc.ContentType()
	if contentType == "" {
		contentType = http.DetectContentType(data)
	}

	filename := strings.TrimSpace(dbFilename)
	if filename == "" {
		filename = path.Base(objectPath)
		if filename == "" {
			filename = "file"
		}
	}

	return data, contentType, filename, nil
}

// parseGSURL parses gs://bucket/object
func parseGSURL(gsURL string) (bucket string, objectPath string, err error) {
	gsURL = strings.TrimSpace(gsURL)
	if gsURL == "" {
		return "", "", fmt.Errorf("empty gs url")
	}
	if !strings.HasPrefix(gsURL, "gs://") {
		return "", "", fmt.Errorf("invalid gs url (must start with gs://): %s", gsURL)
	}

	rest := strings.TrimPrefix(gsURL, "gs://") // bucket/object
	slash := strings.Index(rest, "/")
	if slash < 0 || slash == len(rest)-1 {
		return "", "", fmt.Errorf("invalid gs url format: %s", gsURL)
	}

	bucket = rest[:slash]
	objectPath = rest[slash+1:]
	if strings.TrimSpace(objectPath) == "" {
		return "", "", fmt.Errorf("empty object path in gs url: %s", gsURL)
	}
	return bucket, objectPath, nil
}

// small wrapper so you don't need to import io everywhere in this file
func ioReadAll(r *storage.Reader) ([]byte, error) {
	// storage.Reader implements io.Reader
	// keep it simple:
	buf := new(strings.Builder)
	tmp := make([]byte, 32*1024)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf.Write(tmp[:n])
		}
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			if err == context.Canceled {
				return nil, err
			}
			if err == context.DeadlineExceeded {
				return nil, err
			}
			// many readers return io.EOF, but storage may wrap; we’ll treat any error containing EOF as EOF
			if strings.Contains(strings.ToLower(err.Error()), "eof") {
				break
			}
			return nil, err
		}
	}
	return []byte(buf.String()), nil
}
