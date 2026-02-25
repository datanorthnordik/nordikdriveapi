package dataconfig

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type DataConfigController struct {
	DataConfigService DataConfigServiceAPI
}

// GET /data-config?file_name=...&last_modified=...
//
// last_modified should be the timestamp of the config you have in IndexedDB.
// Accepted formats:
// - RFC3339 / RFC3339Nano (recommended)
// - unix milliseconds (e.g., 1708451234567)
func (cc *DataConfigController) GetConfig(c *gin.Context) {
	fileName := strings.TrimSpace(c.Query("file_name"))
	if fileName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file_name is required"})
		return
	}

	clientLM, err := parseOptionalTime(c.Query("last_modified"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid last_modified (use RFC3339 or unix ms)"})
		return
	}

	res, err := cc.DataConfigService.GetByFileNameIfModified(fileName, clientLM)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "config not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	cfg := res.Config

	// Useful headers for future (optional)
	c.Header("Last-Modified", cfg.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if cfg.Checksum != "" {
		c.Header("ETag", cfg.Checksum)
	}

	if res.NotModified {
		c.JSON(http.StatusOK, gin.H{
			"not_modified": true,
			"file_id":      cfg.FileID,
			"file_name":    cfg.FileName,
			"version":      cfg.Version,
			"checksum":     cfg.Checksum,
			"updated_at":   cfg.UpdatedAt,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"not_modified": false,
		"file_id":      cfg.FileID,
		"file_name":    cfg.FileName,
		"version":      cfg.Version,
		"checksum":     cfg.Checksum,
		"updated_at":   cfg.UpdatedAt,
		"config":       cfg.Config, // jsonb returned as JSON
	})
}

func parseOptionalTime(v string) (*time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, nil
	}

	// Try RFC3339/RFC3339Nano
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return &t, nil
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return &t, nil
	}

	// Try unix milliseconds
	if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
		t := time.Unix(0, ms*int64(time.Millisecond))
		return &t, nil
	}

	return nil, strconv.ErrSyntax
}
