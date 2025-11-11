package file

import (
	"fmt"
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

	if err := fc.LogService.Log("INFO", "file", "UPLOAD_FILE", fmt.Sprintf("File uploaded : %s", strings.Join(input.FileNames, "")), &uid, nil); err != nil {
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

	if err := fc.LogService.Log("INFO", "file", "ACCESS_FILE", fmt.Sprintf("File accessed : %s", fileName), &uid, nil); err != nil {
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

	if err := fc.LogService.Log("WARN", "file", "DELETE_FILE", fmt.Sprintf("File deleted : %s", file.Filename), &uid, nil); err != nil {
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

	if err := fc.LogService.Log("INFO", "file", "RESTORE_FILE", fmt.Sprintf("File restored : %s", file.Filename), &uid, nil); err != nil {
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

	if err := fc.LogService.Log("INFO", "file", "GRAND_FILE_ACCESS", "file access granted", &uid, nil); err != nil {
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

	if err := fc.LogService.Log("WARN", "file", "REVOKE_FILE_ACCESS", "file access revoked", &uid, nil); err != nil {
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

	if err := fc.LogService.Log("INFO", "file", "REPLACE_FILE", fmt.Sprintf("File replaced: %s", file.Filename), &uid, nil); err != nil {
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

	if err := fc.LogService.Log("INFO", "file", "REVERT_FILE", fmt.Sprintf("%s file reverted to %d version", input.Filename, input.Version), &uid, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("file reverted to version %d successfully", input.Version),
	})

}

func (fc *FileController) CreateEditRequest(c *gin.Context) {
	var input EditRequestInput

	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON input"})
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

	uid := uint(userID)
	if err := fc.LogService.Log("INFO", "file_edit", "CREATE_EDIT_REQUEST",
		fmt.Sprintf("Edit request created for file: %s", input.Filename), &uid, nil); err != nil {
		fmt.Printf("Failed to insert log: %v\n", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "edit request submitted",
		"request": request,
	})
}

func (fc *FileController) GetPendingEditRequests(c *gin.Context) {
	requests, err := fc.FileService.GetPendingEditRequests()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"requests": requests,
	})
}

func (fc *FileController) ApproveEditRequest(c *gin.Context) {
	var input struct {
		RequestID uint                     `json:"request_id"`
		Updates   []FileEditRequestDetails `json:"updates"`
	}

	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid input"})
		return
	}

	if err := fc.FileService.ApproveEditRequest(input.RequestID, input.Updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Request approved and file updated"})
}
