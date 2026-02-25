package dataconfig

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, dataconfigService DataConfigServiceAPI) {
	communityController := &DataConfigController{DataConfigService: dataconfigService}

	dataConfigGroup := r.Group("/api/config")
	dataConfigGroup.Use(middlewares.AuthMiddleware())
	{
		dataConfigGroup.GET("", communityController.GetConfig)
	}
}
