package logocontent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type testFile struct {
	ID              uint `gorm:"primaryKey"`
	Filename        string
	InsertedBy      uint
	CreatedAt       time.Time
	Private         bool
	IsDelete        bool
	Size            float64
	Version         int
	Rows            int
	CommunityFilter bool
}

func (testFile) TableName() string {
	return "file"
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if err := db.AutoMigrate(&testFile{}, &FileLogoContent{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}

	return db
}

func withFakeGCS(t *testing.T) (*fakestorage.Server, string) {
	t.Helper()

	srv, err := fakestorage.NewServerWithOptions(fakestorage.Options{Scheme: "http"})
	if err != nil {
		t.Fatalf("failed to start fake gcs: %v", err)
	}
	t.Cleanup(srv.Stop)

	bucket := "logo-content-test"
	srv.CreateBucket(bucket)

	prev := newGCSClientHook
	newGCSClientHook = func(ctx context.Context) (*storage.Client, error) {
		return srv.Client(), nil
	}
	t.Cleanup(func() { newGCSClientHook = prev })

	return srv, bucket
}

func putGCSObject(t *testing.T, bucket, objectPath string, content []byte, contentType string) {
	t.Helper()

	ctx := context.Background()
	client, err := newGCSClientHook(ctx)
	if err != nil {
		t.Fatalf("newGCSClientHook: %v", err)
	}
	defer client.Close()

	w := client.Bucket(bucket).Object(objectPath).NewWriter(ctx)
	if contentType != "" {
		w.ContentType = contentType
	}

	if _, err := w.Write(content); err != nil {
		_ = w.Close()
		t.Fatalf("write object %s/%s: %v", bucket, objectPath, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer %s/%s: %v", bucket, objectPath, err)
	}
}

func mockAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.TrimSpace(c.GetHeader("Authorization")) != "Bearer test" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		rawID := strings.TrimSpace(c.GetHeader("X-UserID"))
		if rawID == "" {
			rawID = "1"
		}
		f, _ := strconv.ParseFloat(rawID, 64)
		c.Set("userID", f)
		c.Next()
	}
}

func setupRouterForController(svc LogoContentServiceAPI) *gin.Engine {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	group := r.Group("/api/logo-content")
	group.Use(mockAuthMiddleware())
	{
		group.GET("/:fileId", (&LogoContentController{Service: svc}).GetHTMLByFileID)
	}

	return r
}

func doReq(r http.Handler, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}
