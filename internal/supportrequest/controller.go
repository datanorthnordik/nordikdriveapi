package supportrequest

import (
	"errors"
	"mime/multipart"
	"net/http"

	"github.com/gin-gonic/gin"
)

type SupportRequestController struct {
	SupportRequestService SupportRequestServiceInterface
}

func (sc *SupportRequestController) Create(c *gin.Context) {
	userIDVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user ID not found"})
		return
	}

	userIDFloat, ok := userIDVal.(float64)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
		return
	}

	var req CreateSupportRequestRequest
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var screenshotHeader *multipart.FileHeader
	fileHeader, err := c.FormFile("screenshot")
	switch {
	case err == nil:
		screenshotHeader = fileHeader
	case errors.Is(err, http.ErrMissingFile):
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	res, err := sc.SupportRequestService.Create(&req, int(userIDFloat), screenshotHeader)
	if err != nil {
		if errors.Is(err, ErrInvalidSupportRequest) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, res)
}
