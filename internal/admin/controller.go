package admin

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type AdminController struct {
	AdminService AdminServiceAPI
}

func (ac *AdminController) SearchFileEditRequests(c *gin.Context) {
	var req AdminFileEditSearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 200 {
		req.PageSize = 20
	}

	resp, err := ac.AdminService.SearchFileEditRequests(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// POST /api/admin/details  { "request_id": 123 }
func (ac *AdminController) GetFileEditRequestDetails(c *gin.Context) {
	var req AdminDetailsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.RequestID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request_id is required"})
		return
	}

	out, err := ac.AdminService.GetFileEditRequestDetails(uint(req.RequestID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "success",
		"request_id":    req.RequestID,
		"total_changes": len(out),
		"data":          out,
	})
}

// GET /api/admin/details/:request_id
func (ac *AdminController) GetFileEditRequestDetailsByParam(c *gin.Context) {
	idStr := c.Param("request_id")
	id, err := strconv.Atoi(idStr)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request_id"})
		return
	}

	out, err := ac.AdminService.GetFileEditRequestDetails(uint(id))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "success",
		"request_id":    id,
		"total_changes": len(out),
		"data":          out,
	})
}

func (ac *AdminController) DownloadUpdates(c *gin.Context) {
	var req AdminDownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	req.Format = strings.ToLower(strings.TrimSpace(req.Format))
	if req.Format == "" {
		req.Format = "excel"
	}

	contentType, filename, data, err := ac.AdminService.DownloadUpdates(req.Mode, req.Clauses, req.Format)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.Header("Content-Disposition", `attachment; filename="`+filename+`"`)
	c.Data(http.StatusOK, contentType, data)
}

// POST /api/admin/download-media
func (ac *AdminController) DownloadMediaZip(c *gin.Context) {
	var req AdminDownloadMediaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// request_ids is optional, so when it's missing => len == 0
	if len(req.RequestIDs) == 0 && len(req.Clauses) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "apply filter to download documents"})
		return
	}

	ts := time.Now().Format("20060102_150405")
	zipName := fmt.Sprintf("media_%s.zip", ts)

	// naming based on request_ids if present
	ids := dedupeAndFilterRequestIDs(req.RequestIDs)
	if len(ids) == 1 {
		zipName = fmt.Sprintf("request_%d_media_%s.zip", ids[0], ts)
	} else if len(ids) > 1 {
		zipName = fmt.Sprintf("requests_%d_media_%s.zip", len(ids), ts)
	}

	c.Header("Content-Type", "application/zip")
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, zipName))
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Cache-Control", "no-store")

	if err := ac.AdminService.StreamMediaZip(c.Request.Context(), c.Writer, req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
}

func dedupeAndFilterRequestIDs(in []uint) []uint {
	seen := make(map[uint]struct{}, len(in))
	out := make([]uint, 0, len(in))

	for _, id := range in {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
