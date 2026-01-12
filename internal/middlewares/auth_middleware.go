package middlewares

import (
	"net/http"
	"nordik-drive-api/config"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		cfg := config.LoadConfig()
		accessToken, err := c.Cookie("access_token")
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing access token"})
			c.Abort()
			return
		}

		token, err := jwt.Parse(accessToken, func(token *jwt.Token) (interface{}, error) {
			return []byte(cfg.JWTSecret), nil
		})

		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			c.Abort()
			return
		}

		claims := token.Claims.(jwt.MapClaims)
		userIDVal := claims["user_id"]
		var userID float64
		switch v := userIDVal.(type) {
		case float64:
			userID = v
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
				c.Abort()
				return
			}
			userID = f
		default:
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID"})
			c.Abort()
			return
		}

		raw := claims["communities"]

		communities := []string{}
		if raw != nil {
			if arr, ok := raw.([]interface{}); ok {
				for _, v := range arr {
					if s, ok := v.(string); ok && s != "" {
						communities = append(communities, s)
					}
				}
			}
		}

		c.Set("userID", userID)
		c.Set("communities", communities)
		c.Next()
	}
}
