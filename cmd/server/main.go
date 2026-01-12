package main

import (
	"context"
	"log"
	"nordik-drive-api/config"
	"nordik-drive-api/internal/admin"
	"nordik-drive-api/internal/auth"
	"nordik-drive-api/internal/chat"
	"nordik-drive-api/internal/community"
	"nordik-drive-api/internal/file"
	"nordik-drive-api/internal/logs"
	"nordik-drive-api/internal/role"
	"os"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"google.golang.org/genai"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg := config.LoadConfig()

	dsn := "host=" + cfg.DBHost +
		" user=" + cfg.DBUser +
		" password=" + cfg.DBPassword +
		" dbname=" + cfg.DBName +
		" port=" + cfg.DBPort +
		" sslmode=disable"

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	r := gin.Default()

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:3000", "http://34.145.18.109/", "https://nordik-drive-react-724838782318.us-west1.run.app"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		AllowCredentials: true,
	}))

	logService := &logs.LogService{DB: db}
	userService := &auth.AuthService{DB: db, CFG: &cfg}
	auth.RegisterRoutes(r, userService, logService)

	fileService := &file.FileService{DB: db}
	file.RegisterRoutes(r, fileService, logService)

	roleService := &role.RoleService{DB: db}
	role.RegisterRoutes(r, roleService)

	logs.RegisterRoutes(r, logService)

	communityService := &community.CommunityService{DB: db}
	community.RegisterRoutes(r, communityService)

	// Create client with ADC (production)
	ctx := context.Background()
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend:  genai.BackendVertexAI,
		Project:  "planar-ray-472112-e8", // <-- REPLACE with your Project ID
		Location: "global",               // <-- REPLACE with your project location
		// Note: No APIKey is needed when using Vertex AI with ADC.
	})

	chatService := &chat.ChatService{DB: db, Client: client}
	chat.RegisterRoutes(r, chatService)

	adminService := &admin.AdminService{DB: db}
	admin.RegisterRoutes(r, adminService)

	// --- Cloud Run expects plain HTTP, on $PORT, bind to 0.0.0.0 ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Starting server on 0.0.0.0:%s ...", port)
	log.Fatal(r.Run("0.0.0.0:" + port))
}
