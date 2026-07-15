package honour

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, service ServiceAPI) {
	controller := &Controller{Service: service}

	group := r.Group("/api/file")
	group.Use(middlewares.AuthMiddleware())
	{
		group.GET("/honour", controller.GetToday)
	}
}
