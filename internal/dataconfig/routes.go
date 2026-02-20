package dataconfig

import (
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, dataconfigService *DataConfigService) {
	communityController := &DataConfigController{DataConfigService: dataconfigService}

	dataConfigGroup := r.Group("/api/config")
	{
		dataConfigGroup.GET("", communityController.GetConfig)
	}
}
