package file

import (
	"fmt"
	"io"
	"net/http"
	"nordik-drive-api/internal/logs"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type FileController struct {
	FileService *FileService
	LogService  *logs.LogService
}

type FileUploadInput struct {
	FileNames       []string `form:"filenames" binding:"required"`
	Private         []bool   `form:"private" binding:"required"`
	CommunityFilter []bool   `form:"community_filter" binding:"required"`
}

type ReplaceFileInput struct {
	Id uint `form:"id" binding:"required"`
}

func (fc *FileController) UploadFiles(c *gin.Context) {
	// parse multipart form
	form, err := c.MultipartForm()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read form"})
		return
	}

	uploadedFiles := form.File["files"]
	if len(uploadedFiles) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no files uploaded"})
		return
	}

	// parse filenames array
	var input FileUploadInput
	if err := c.ShouldBind(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read filenames array"})
		return
	}

	if len(input.FileNames) != len(uploadedFiles) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "files count and filenames array length mismatch"})
		return
	}

	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	savedFiles, err := fc.FileService.SaveFilesMultipart(uploadedFiles, input, uint(userID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "INFO",
		Service:     "file",
		UserID:      &uid,
		Action:      "UPLOAD_FILE",
		Message:     fmt.Sprintf("Files uploaded: %s", strings.Join(input.FileNames, ",")),
		Communities: communities,
		Filename:    &input.FileNames[0],
	}

	if err := fc.LogService.Log(log, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{"message": "files uploaded successfully", "files": savedFiles})
}

func (fc *FileController) GetAllFiles(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(uint) // assuming middleware sets uint
	if !ok {
		// handle jwt float64 conversion case
		if f, ok2 := userIDVal.(float64); ok2 {
			userID = uint(f)
		} else {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
			return
		}
	}

	// fetch role
	role, err := fc.FileService.GetUserRole(userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	files, err := fc.FileService.GetAllFiles(userID, role)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Files fetched successfully",
		"files":   files,
	})
}

// type FileDataRequest struct {
// 	Filename string `json:"filename"`
// }

func (fc *FileController) GetFileData(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	fileName := c.Query("filename")
	versionStr := c.Query("version")
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid version"})
		return
	}

	if fileName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file name is required"})
		return
	}

	fileData, err := fc.FileService.GetFileData(fileName, version)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if fileData == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "file not found"})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "INFO",
		Service:     "file",
		UserID:      &uid,
		Action:      "ACCESS_FILE",
		Message:     fmt.Sprintf("File accessed : %s", fileName),
		Communities: communities,
		Filename:    &fileName,
	}

	if err := fc.LogService.Log(log, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, fileData)
}

func (fc *FileController) DeleteFile(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	fileID := c.Query("id")
	if fileID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file ID is required"})
		return
	}

	file, err := fc.FileService.DeleteFile(fileID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "WARN",
		Service:     "file",
		UserID:      &uid,
		Action:      "DELETE_FILE",
		Message:     fmt.Sprintf("File deleted : %s", file.Filename),
		Communities: communities,
		Filename:    &file.Filename,
	}

	if err := fc.LogService.Log(log, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "File deleted successfully",
	})
}

func (fc *FileController) ResetFile(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	fileID := c.Query("id")
	if fileID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file ID is required"})
		return
	}

	file, err := fc.FileService.ResetFile(fileID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "INFO",
		Service:     "file",
		UserID:      &uid,
		Action:      "RESTORE_FILE",
		Message:     fmt.Sprintf("File restored : %s", file.Filename),
		Communities: communities,
		Filename:    &file.Filename,
	}

	if err := fc.LogService.Log(log, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "File restored successfully",
	})
}

func (fc *FileController) CreateAccess(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	var input []FileAccess
	if err := c.ShouldBind(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read data"})
		return
	}

	if err := fc.FileService.CreateAccess(input); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "INFO",
		Service:     "file",
		UserID:      &uid,
		Action:      "GRAND_FILE_ACCESS",
		Message:     "file access granted",
		Communities: communities,
	}

	if err := fc.LogService.Log(log, input); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "File access given successfully",
	})
}

func (fc *FileController) DeleteAccess(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	accessId := c.Query("id")

	if err := fc.FileService.DeleteAccess(accessId); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "WARN",
		Service:     "file",
		UserID:      &uid,
		Action:      "REVOKE_FILE_ACCESS",
		Message:     "file access revoked",
		Communities: communities,
	}

	if err := fc.LogService.Log(log, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "File access revoked successfully",
	})
}

type FileAccessWithUser struct {
	ID        uint   `json:"id"`
	UserID    uint   `json:"user_id"`
	FileID    uint   `json:"file_id"`
	FirstName string `json:"firstname" gorm:"column:firstname"`
	LastName  string `json:"lastname" gorm:"column:lastname"`
}

func (fc *FileController) GetAllAccess(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	_, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	fileId := c.Query("id")

	FileAccess, err := fc.FileService.GetFileAccess(fileId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if FileAccess == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "access not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Access fetched successfully",
		"access":  FileAccess,
	})
}

func (fc *FileController) GetFileHistory(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	_, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	fileId := c.Query("id")

	fileHistory, err := fc.FileService.GetFileHistory(fileId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if fileHistory == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "history not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "File history fetched successfully",
		"history": fileHistory,
	})
}

func (fc *FileController) ReplaceFile(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no file uploaded"})
		return
	}

	var replaceFileInput ReplaceFileInput
	if err := c.ShouldBind(&replaceFileInput); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read file id"})
		return
	}

	err = fc.FileService.ReplaceFiles(file, replaceFileInput.Id, uint(userID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "INFO",
		Service:     "file",
		UserID:      &uid,
		Action:      "REPLACE_FILE",
		Message:     fmt.Sprintf("File replaced: %s", file.Filename),
		Communities: communities,
		Filename:    &file.Filename,
	}

	if err := fc.LogService.Log(log, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{"message": "File replaced successfully"})
}

func (fc *FileController) RevertFile(c *gin.Context) {
	var input RevertFileInput
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := fc.FileService.RevertFile(input.Filename, input.Version, uint(userID)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	uid := uint(userID)

	communitiesVal, exists := c.Get("communities")
	if !exists {
		communitiesVal = []string{}
	}

	communities, ok := communitiesVal.([]string)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid communities"})
		return
	}

	log := logs.SystemLog{
		Level:       "INFO",
		Service:     "file",
		UserID:      &uid,
		Action:      "REVERT_FILE",
		Message:     fmt.Sprintf("%s file reverted to %d version", input.Filename, input.Version),
		Communities: communities,
		Filename:    &input.Filename,
	}

	if err := fc.LogService.Log(log, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("file reverted to version %d successfully", input.Version),
	})

}

func (fc *FileController) CreateEditRequest(c *gin.Context) {
	var input EditRequestInput

	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	request, err := fc.FileService.CreateEditRequest(input, uint(userID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Edit request submitted",
		"request": request,
	})
}

func (fc *FileController) GetEditRequests(c *gin.Context) {
	_, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	// Query params:
	// ?status=approved,rejected
	// ?user_id=26
	statusRaw := strings.TrimSpace(c.Query("status"))
	userIDRaw := strings.TrimSpace(c.Query("user_id"))

	var statusPtr *string
	if statusRaw != "" {
		statusPtr = &statusRaw
	}

	var userIDPtr *uint
	if userIDRaw != "" {
		parsed, err := strconv.ParseUint(userIDRaw, 10, 32)
		if err != nil || parsed == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
			return
		}
		u := uint(parsed)
		userIDPtr = &u
	}

	requests, err := fc.FileService.GetEditRequests(statusPtr, userIDPtr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"requests": requests})
}

func (fc *FileController) ApproveEditRequest(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userID, ok := userIDVal.(float64)

	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}
	var input struct {
		RequestID uint                     `json:"request_id"`
		Updates   []FileEditRequestDetails `json:"updates"`
	}

	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid input"})
		return
	}

	if err := fc.FileService.ApproveEditRequest(input.RequestID, input.Updates, uint(userID)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Request approved and file updated"})
}

// POST /api/file/photos/review
func (fc *FileController) ReviewPhotos(c *gin.Context) {

	var input struct {
		Approved []uint `json:"approved_photos"`
		Rejected []uint `json:"rejected_photos"`
	}

	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid input"})
		return
	}

	// Get reviewer name (optional)
	reviewer := c.GetString("user_email")

	if err := fc.FileService.ReviewPhotos(input.Approved, input.Rejected, reviewer); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Photo review updated"})
}

func (fc *FileController) GetPhotosByRequest(c *gin.Context) {
	requestIDParam := c.Param("requestId")
	requestIDInt, err := strconv.Atoi(requestIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request ID"})
		return
	}

	requestID := uint(requestIDInt)

	photos, err := fc.FileService.GetPhotosByRequest(requestID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"photos": photos,
	})
}

func (fc *FileController) GetDocsByRequest(c *gin.Context) {
	requestIDParam := c.Param("requestId")
	requestIDInt, err := strconv.Atoi(requestIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request ID"})
		return
	}

	requestID := uint(requestIDInt)

	docs, err := fc.FileService.GetDocsByRequest(requestID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"docs": docs,
	})
}

func (fc *FileController) GetPhotosByRow(c *gin.Context) {
	rowIDParam := c.Param("rowId")
	rowIDInt, err := strconv.Atoi(rowIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid row ID"})
		return
	}

	rowID := uint(rowIDInt)

	photos, err := fc.FileService.GetPhotosByRow(rowID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"photos": photos,
	})
}

func (fc *FileController) GetDocsByRow(c *gin.Context) {
	rowIDParam := c.Param("rowId")
	rowIDInt, err := strconv.Atoi(rowIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid row ID"})
		return
	}

	rowID := uint(rowIDInt)

	docs, err := fc.FileService.GetDocsByRow(rowID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"docs": docs,
	})
}

func (fc *FileController) GetPhoto(c *gin.Context) {
	photoIDParam := c.Param("photoId")
	photoID, err := strconv.Atoi(photoIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid photo ID"})
		return
	}

	data, contentType, err := fc.FileService.GetPhotoBytes(uint(photoID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Return as image
	c.Data(http.StatusOK, contentType, data)
}

func (fc *FileController) GetDoc(c *gin.Context) {
	docIDParam := c.Param("docId")
	docID, err := strconv.Atoi(docIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid doc ID"})
		return
	}

	data, contentType, filename, err := fc.FileService.GetDocBytes(uint(docID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Inline for PDF/images, attachment for others
	disposition := "attachment"
	if strings.HasPrefix(contentType, "image/") || contentType == "application/pdf" {
		disposition = "inline"
	}

	// Important headers
	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", fmt.Sprintf(`%s; filename="%s"`, disposition, sanitizeFilename(filename)))
	c.Header("X-Content-Type-Options", "nosniff")

	c.Data(http.StatusOK, contentType, data)
}

func sanitizeFilename(name string) string {
	if name == "" {
		return "document"
	}
	// remove path separators and quotes
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, `"`, "")
	return name
}

func (fc *FileController) DownloadMediaByID(c *gin.Context) {
	idStr := c.Param("id")
	id64, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || id64 == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	id := uint(id64)

	kind := strings.TrimSpace(c.Query("kind")) // optional: photo|doc|document
	handle, filename, contentType, disposition, err := fc.FileService.OpenMediaHandle(c.Request.Context(), id, kind)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	defer handle.Close()

	// If GCS content-type missing, sniff first bytes and stream
	if contentType == "" {
		buf := make([]byte, 512)
		n, _ := io.ReadFull(handle.Reader, buf)
		if n > 0 {
			contentType = http.DetectContentType(buf[:n])
		} else {
			contentType = "application/octet-stream"
		}

		disposition = "attachment"
		if strings.HasPrefix(contentType, "image/") || contentType == "application/pdf" {
			disposition = "inline"
		}

		c.Header("Content-Type", contentType)
		c.Header("Content-Disposition", fmt.Sprintf(`%s; filename="%s"`, disposition, sanitizeFilename(filename)))
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Cache-Control", "no-store")

		// write sniffed bytes first, then rest
		_, _ = c.Writer.Write(buf[:n])
		_, _ = io.Copy(c.Writer, handle.Reader)
		return
	}

	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", fmt.Sprintf(`%s; filename="%s"`, disposition, sanitizeFilename(filename)))
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Cache-Control", "no-store")

	_, _ = io.Copy(c.Writer, handle.Reader)
}
