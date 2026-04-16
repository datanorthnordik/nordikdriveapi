package logocontent

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, service LogoContentServiceAPI) {
	controller := &LogoContentController{Service: service}

	userGroup := r.Group("/api/logo-content")
	userGroup.Use(middlewares.AuthMiddleware())
	{
		userGroup.GET("/:fileId", controller.GetHTMLByFileID)
	}
}
