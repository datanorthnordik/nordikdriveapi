package file

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"nordik-drive-api/internal/logs"

	"github.com/gin-gonic/gin"
)

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
		typ := strings.ToLower(strings.TrimSpace(c.GetHeader("X-UserID-Type")))

		switch typ {
		case "uint":
			u, _ := strconv.ParseUint(rawID, 10, 64)
			c.Set("userID", uint(u))
		case "string":
			c.Set("userID", rawID) // invalid type intentionally
		default:
			// float64 like JWT claims
			f, _ := strconv.ParseFloat(rawID, 64)
			c.Set("userID", f)
		}

		com := strings.TrimSpace(c.GetHeader("X-Communities"))
		if com == "" {
			c.Set("communities", []string{})
		} else {
			parts := strings.Split(com, ",")
			out := make([]string, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					out = append(out, p)
				}
			}
			c.Set("communities", out)
		}

		if em := strings.TrimSpace(c.GetHeader("X-UserEmail")); em != "" {
			c.Set("user_email", em)
		}

		c.Next()
	}
}

func setupRouterForController(fc *FileController) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	g := r.Group("/api/file")
	g.Use(mockAuthMiddleware())
	{
		g.GET("", fc.GetAllFiles)
		g.POST("/upload", fc.UploadFiles)
		g.GET("/data", fc.GetFileData)
		g.DELETE("", fc.DeleteFile)
		g.PUT("/reset", fc.ResetFile)
		g.GET("/access", fc.GetAllAccess)
		g.POST("/access", fc.CreateAccess)
		g.DELETE("/access", fc.DeleteAccess)
		g.GET("/history", fc.GetFileHistory)
		g.POST("/replace", fc.ReplaceFile)
		g.POST("/revert", fc.RevertFile)
		g.POST("/edit/request", fc.CreateEditRequest)
		g.GET("/edit/request", fc.GetEditRequests)
		g.GET("/edit/photos/:requestId", fc.GetPhotosByRequest)
		g.GET("/edit/docs/:requestId", fc.GetDocsByRequest)
		g.GET("/photos/:rowId", fc.GetPhotosByRow)
		g.GET("/docs/:rowId", fc.GetDocsByRow)
		g.GET("/photo/:photoId", fc.GetPhoto)
		g.GET("/doc/:docId", fc.GetDoc)
		g.PUT("/approve/request", fc.ApproveEditRequest)
		g.POST("/photos/review", fc.ReviewPhotos)
		g.POST("/doc/download/:id", fc.DownloadMediaByID)
	}

	return r
}

func doReq(r http.Handler, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func newJSONReq(method, url string, body any, headers map[string]string) *http.Request {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

func newMultipartReq(method, url string, fields map[string][]string, fileField string, fileName string, fileBytes []byte, headers map[string]string) *http.Request {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	for k, vals := range fields {
		for _, v := range vals {
			_ = w.WriteField(k, v)
		}
	}

	if fileField != "" {
		fw, _ := w.CreateFormFile(fileField, fileName)
		_, _ = fw.Write(fileBytes)
	}

	_ = w.Close()

	req := httptest.NewRequest(method, url, &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// ---- fake log service ----

type fakeLogService struct {
	Calls   []logs.SystemLog
	Payload []any
	Err     error
}

func (f *fakeLogService) Log(l logs.SystemLog, payload interface{}) error {
	f.Calls = append(f.Calls, l)
	f.Payload = append(f.Payload, payload)
	return f.Err
}

// ---- fake file service ----

type fakeFileService struct {
	Called map[string]int

	LastSaveUserID uint
	LastDeleteID   string
	LastResetID    string

	LastReplaceID uint
	LastReplaceBy uint

	LastRevertFilename string
	LastRevertVersion  int
	LastRevertBy       uint

	LastApproveReqID uint
	LastApproveBy    uint

	LastReviewer string

	RoleOut string
	RoleErr error

	GetAllOut []FileWithUser
	GetAllErr error

	SaveOut []File
	SaveErr error

	FileDataOut []FileData
	FileDataErr error

	DeleteOut File
	DeleteErr error

	ResetOut File
	ResetErr error

	PhotoBytesOut []byte
	PhotoCTOut    string
	PhotoBytesErr error

	DocBytesOut []byte
	DocCTOut    string
	DocNameOut  string
	DocBytesErr error

	MediaRCOut   io.ReadCloser
	MediaFNOut   string
	MediaCTOut   string
	MediaDispOut string
	MediaErr     error
}

func (f *fakeFileService) bump(name string) {
	if f.Called == nil {
		f.Called = map[string]int{}
	}
	f.Called[name]++
}

func (f *fakeFileService) SaveFilesMultipart(uploadedFiles []*multipart.FileHeader, filenames FileUploadInput, userID uint) ([]File, error) {
	f.bump("SaveFilesMultipart")
	f.LastSaveUserID = userID
	return f.SaveOut, f.SaveErr
}
func (f *fakeFileService) GetUserRole(userID uint) (string, error) {
	f.bump("GetUserRole")
	return f.RoleOut, f.RoleErr
}
func (f *fakeFileService) GetAllFiles(userID uint, role string) ([]FileWithUser, error) {
	f.bump("GetAllFiles")
	return f.GetAllOut, f.GetAllErr
}
func (f *fakeFileService) GetFileData(filename string, version int) ([]FileData, error) {
	f.bump("GetFileData")
	return f.FileDataOut, f.FileDataErr
}
func (f *fakeFileService) DeleteFile(fileID string) (File, error) {
	f.bump("DeleteFile")
	f.LastDeleteID = fileID
	return f.DeleteOut, f.DeleteErr
}
func (f *fakeFileService) ResetFile(fileID string) (File, error) {
	f.bump("ResetFile")
	f.LastResetID = fileID
	return f.ResetOut, f.ResetErr
}

func (f *fakeFileService) CreateAccess(input []FileAccess) error { f.bump("CreateAccess"); return nil }
func (f *fakeFileService) DeleteAccess(accessId string) error    { f.bump("DeleteAccess"); return nil }
func (f *fakeFileService) GetFileAccess(fileId string) ([]FileAccessWithUser, error) {
	f.bump("GetFileAccess")
	return []FileAccessWithUser{}, nil
}
func (f *fakeFileService) GetFileHistory(fileId string) ([]FileVersionWithUser, error) {
	f.bump("GetFileHistory")
	return []FileVersionWithUser{}, nil
}

func (f *fakeFileService) ReplaceFiles(uploadedFile *multipart.FileHeader, fileID uint, userID uint) error {
	f.bump("ReplaceFiles")
	f.LastReplaceID = fileID
	f.LastReplaceBy = userID
	return nil
}
func (f *fakeFileService) RevertFile(filename string, version int, userID uint) error {
	f.bump("RevertFile")
	f.LastRevertFilename = filename
	f.LastRevertVersion = version
	f.LastRevertBy = userID
	return nil
}

func (f *fakeFileService) CreateEditRequest(input EditRequestInput, userID uint) (*FileEditRequest, error) {
	f.bump("CreateEditRequest")
	return &FileEditRequest{RequestID: 123}, nil
}
func (f *fakeFileService) GetEditRequests(statusCSV *string, userID *uint) ([]FileEditRequestWithUser, error) {
	f.bump("GetEditRequests")
	return []FileEditRequestWithUser{}, nil
}
func (f *fakeFileService) ApproveEditRequest(requestID uint, updates []FileEditRequestDetails, userId uint) error {
	f.bump("ApproveEditRequest")
	f.LastApproveReqID = requestID
	f.LastApproveBy = userId
	return nil
}

func (f *fakeFileService) ReviewPhotos(approved []uint, rejected []uint, reviewer string) error {
	f.bump("ReviewPhotos")
	f.LastReviewer = reviewer
	return nil
}

func (f *fakeFileService) GetPhotosByRequest(requestID uint) ([]FileEditRequestPhoto, error) {
	f.bump("GetPhotosByRequest")
	return []FileEditRequestPhoto{}, nil
}
func (f *fakeFileService) GetDocsByRequest(requestID uint) ([]FileEditRequestPhoto, error) {
	f.bump("GetDocsByRequest")
	return []FileEditRequestPhoto{}, nil
}
func (f *fakeFileService) GetPhotosByRow(rowID uint) ([]FileEditRequestPhoto, error) {
	f.bump("GetPhotosByRow")
	return []FileEditRequestPhoto{}, nil
}
func (f *fakeFileService) GetDocsByRow(rowID uint) ([]FileEditRequestPhoto, error) {
	f.bump("GetDocsByRow")
	return []FileEditRequestPhoto{}, nil
}

func (f *fakeFileService) GetPhotoBytes(photoID uint) ([]byte, string, error) {
	f.bump("GetPhotoBytes")
	return f.PhotoBytesOut, f.PhotoCTOut, f.PhotoBytesErr
}
func (f *fakeFileService) GetDocBytes(docID uint) ([]byte, string, string, error) {
	f.bump("GetDocBytes")
	return f.DocBytesOut, f.DocCTOut, f.DocNameOut, f.DocBytesErr
}

func (f *fakeFileService) OpenMediaHandle(ctx context.Context, id uint, kind string) (io.ReadCloser, string, string, string, error) {
	f.bump("OpenMediaHandle")
	return f.MediaRCOut, f.MediaFNOut, f.MediaCTOut, f.MediaDispOut, f.MediaErr
}
