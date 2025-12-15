package file

import (
	"nordik-drive-api/internal/logs"
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, fileService *FileService, logService *logs.LogService) {
	fileController := &FileController{FileService: fileService, LogService: logService}

	userGroup := r.Group("/api/file")
	userGroup.Use(middlewares.AuthMiddleware())
	{
		userGroup.GET("", fileController.GetAllFiles)
		userGroup.POST("/upload", fileController.UploadFiles)
		userGroup.GET("/data", fileController.GetFileData)
		userGroup.DELETE("", fileController.DeleteFile)
		userGroup.PUT("/reset", fileController.ResetFile)
		userGroup.GET("/access", fileController.GetAllAccess)
		userGroup.POST("/access", fileController.CreateAccess)
		userGroup.DELETE("/access", fileController.DeleteAccess)
		userGroup.GET("/history", fileController.GetFileHistory)
		userGroup.POST("/replace", fileController.ReplaceFile)
		userGroup.POST("/revert", fileController.RevertFile)
		userGroup.POST("/edit/request", fileController.CreateEditRequest)
		userGroup.GET("/edit/request", fileController.GetPendingEditRequests)
		userGroup.GET("/edit/photos/:requestId", fileController.GetPhotosByRequest)
		userGroup.GET("/photos/:rowId", fileController.GetPhotosByRow)
		userGroup.GET("/photo/:photoId", fileController.GetPhoto)
		userGroup.PUT("/approve/request", fileController.ApproveEditRequest)
		userGroup.POST("/photos/review", fileController.ReviewPhotos)
	}

}
