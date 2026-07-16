package middlewares

import (
	"crypto/subtle"
	"net/http"
	"nordik-drive-api/config"
	"strings"

	"github.com/gin-gonic/gin"
)

const HonourJobSecretHeader = "X-Honour-Job-Secret"

func JobSecretMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		cfg := config.LoadConfig()
		expected := strings.TrimSpace(cfg.HonourJobSecret)
		if expected == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "honour job secret is not configured"})
			c.Abort()
			return
		}

		provided := strings.TrimSpace(c.GetHeader(HonourJobSecretHeader))
		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid honour job secret"})
			c.Abort()
			return
		}

		c.Next()
	}
}
