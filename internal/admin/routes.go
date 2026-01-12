package admin

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, chatService *AdminService) {
	adminController := &AdminController{AdminService: chatService}

	adminGroup := r.Group("/api/admin")
	adminGroup.Use(middlewares.AuthMiddleware())
	{
		adminGroup.POST("", adminController.SearchFileEditRequests)
		adminGroup.POST("/download", adminController.DownloadUpdates)

		// ✅ NEW: details endpoint for "View All Details"
		adminGroup.POST("/details", adminController.GetFileEditRequestDetails)
		adminGroup.POST("/download_files", adminController.DownloadMediaZip)

		// (optional) also allow GET
		adminGroup.GET("/details/:request_id", adminController.GetFileEditRequestDetailsByParam)
	}
}
