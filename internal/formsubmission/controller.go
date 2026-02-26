package formsubmission

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type FormSubmissionController struct {
	FormSubmissionService *FormSubmissionService
}

// GET /api/form/answers?row_id=...&form_key=...&file_id=...
func (cc *FormSubmissionController) GetFormSubmission(c *gin.Context) {
	rowID, err := parseRequiredInt64Query(c.Query("row_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid row_id is required"})
		return
	}

	formKey := strings.TrimSpace(c.Query("form_key"))
	if formKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "form_key is required"})
		return
	}

	var fileIDPtr *int64
	fileIDStr := strings.TrimSpace(c.Query("file_id"))
	if fileIDStr != "" {
		fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
		if err != nil || fileID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file_id"})
			return
		}
		fileIDPtr = &fileID
	}

	res, err := cc.FormSubmissionService.GetByRowAndForm(rowID, formKey, fileIDPtr)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, res)
}

// POST /api/form/answers
func (cc *FormSubmissionController) SaveFormSubmission(c *gin.Context) {
	var req SaveFormSubmissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	res, err := cc.FormSubmissionService.Upsert(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, res)
}

func (cc *FormSubmissionController) GetUpload(c *gin.Context) {
	idParam := strings.TrimSpace(c.Param("id"))
	id, err := strconv.Atoi(idParam)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}

	data, contentType, filename, err := cc.FormSubmissionService.GetUploadBytes(uint(id))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	disposition := "inline"
	if !strings.HasPrefix(contentType, "image/") && contentType != "application/pdf" {
		disposition = "attachment"
	}

	c.Header("Content-Disposition", fmt.Sprintf(`%s; filename="%s"`, disposition, filename))
	c.Data(http.StatusOK, contentType, data)
}

func parseRequiredInt64Query(v string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
}
