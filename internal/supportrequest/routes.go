package supportrequest

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine, service SupportRequestServiceInterface) {
	controller := &SupportRequestController{
		SupportRequestService: service,
	}

	group := r.Group("/api/support-requests")
	group.Use(middlewares.AuthMiddleware())
	{
		group.POST("", controller.Create)
	}
}
