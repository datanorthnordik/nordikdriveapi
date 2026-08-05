package supportschedule

import (
	"nordik-drive-api/internal/middlewares"

	"github.com/gin-gonic/gin"
)

func supportScheduleAuthMiddleware() gin.HandlerFunc { return middlewares.AuthMiddleware() }
