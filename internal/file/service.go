package file

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"nordik-drive-api/internal/auth"
	"path/filepath"
	"strings"
	"time"

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

	// Create the main request
	request := FileEditRequest{
		UserID:    userID,
		Status:    "pending",
		CreatedAt: time.Now(),
		FirstName: input.FirstName,
		LastName:  input.LastName,
	}

	if err := fs.DB.Create(&request).Error; err != nil {
		return nil, err
	}

	// Insert each detail item
	for _, change := range input.Changes {
		detail := FileEditRequestDetails{
			RequestID: request.RequestID,
			FileID:    input.FileID,
			Filename:  input.Filename,
			RowID:     change.RowID,
			FieldName: change.FieldName,
			OldValue:  change.OldValue,
			NewValue:  change.NewValue,
			CreatedAt: time.Now(),
		}

		if err := fs.DB.Create(&detail).Error; err != nil {
			return nil, err
		}
	}

	return &request, nil
}

func (fs *FileService) GetPendingEditRequests() ([]FileEditRequestWithUser, error) {
	var baseRequests []struct {
		RequestID  uint
		UserID     uint
		Firstname  string
		Lastname   string
		Status     string
		CreatedAt  time.Time
		EFirstName string `gorm:"column:efirstname"`
		ELastName  string `gorm:"column:elastname"`
	}

	err := fs.DB.Table("file_edit_request").
		Select("file_edit_request.request_id, file_edit_request.user_id, users.firstname, users.lastname, file_edit_request.status, file_edit_request.created_at, file_edit_request.firstname as efirstname, file_edit_request.lastname as elastname").
		Joins("JOIN users ON users.id = file_edit_request.user_id").
		Where("file_edit_request.status = ?", "pending").
		Order("file_edit_request.created_at DESC").
		Scan(&baseRequests).Error

	if err != nil {
		return nil, err
	}

	var final []FileEditRequestWithUser

	// Fetch details for each request
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
		})
	}

	return final, nil
}

func (fs *FileService) ApproveEditRequest(requestID uint, updates []FileEditRequestDetails) error {

	// 1. Update changed details (new_value)
	for _, upd := range updates {
		err := fs.DB.Model(&FileEditRequestDetails{}).
			Where("id = ?", upd.ID).
			Update("new_value", upd.NewValue).Error

		if err != nil {
			return err
		}
	}

	// 2. Fetch all details for this request
	var allDetails []FileEditRequestDetails
	if err := fs.DB.Where("request_id = ?", requestID).Find(&allDetails).Error; err != nil {
		return err
	}

	// 3. For each field change → update corresponding FileData row
	for _, det := range allDetails {

		// Load the file data row for this row_id
		var fileData FileData
		err := fs.DB.Where("file_id = ? AND id = ?", det.FileID, det.RowID).First(&fileData).Error
		if err != nil {
			return fmt.Errorf("file data row not found for file %d row %d", det.FileID, det.RowID)
		}

		// Convert row_data JSON → map
		var row map[string]string
		if err := json.Unmarshal(fileData.RowData, &row); err != nil {
			return fmt.Errorf("failed to parse row_data: %v", err)
		}

		// Update the changed field
		row[det.FieldName] = det.NewValue

		// Convert back to JSON
		newJSON, _ := json.Marshal(row)

		// Save updated row
		err = fs.DB.Model(&fileData).
			Update("row_data", datatypes.JSON(newJSON)).Error

		if err != nil {
			return fmt.Errorf("failed to update file_data: %v", err)
		}
	}

	// 4. Mark request as approved
	return fs.DB.Model(&FileEditRequest{}).
		Where("request_id = ?", requestID).
		Update("status", "approved").Error
}
