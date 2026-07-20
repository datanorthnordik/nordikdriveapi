package supportrequest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"nordik-drive-api/internal/mailer"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var supportRequestTestDBSeq uint64

type fakeSupportRequestMailer struct {
	sendCalls           []supportRequestMailCall
	sendWithAttachCalls []supportRequestMailWithAttachmentCall
	sendErr             error
	sendWithAttachErr   error
}

type supportRequestMailCall struct {
	To      []string
	Subject string
	Body    string
}

type supportRequestMailWithAttachmentCall struct {
	To          []string
	Subject     string
	Body        string
	Attachments []mailer.Attachment
}

func (m *fakeSupportRequestMailer) Send(to []string, subject, body string) error {
	m.sendCalls = append(m.sendCalls, supportRequestMailCall{
		To:      append([]string(nil), to...),
		Subject: subject,
		Body:    body,
	})
	return m.sendErr
}

func (m *fakeSupportRequestMailer) SendWithAttachments(to []string, subject, body string, attachments []mailer.Attachment) error {
	m.sendWithAttachCalls = append(m.sendWithAttachCalls, supportRequestMailWithAttachmentCall{
		To:          append([]string(nil), to...),
		Subject:     subject,
		Body:        body,
		Attachments: append([]mailer.Attachment(nil), attachments...),
	})
	return m.sendWithAttachErr
}

func newTestService(t *testing.T) (*SupportRequestService, *fakeSupportRequestMailer) {
	t.Helper()

	id := atomic.AddUint64(&supportRequestTestDBSeq, 1)
	dsn := fmt.Sprintf("file:supportrequest_test_%d?mode=memory&cache=shared", id)

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	if err := db.AutoMigrate(&SupportRequestUserRef{}, &SupportRequest{}); err != nil {
		t.Fatalf("migrate db: %v", err)
	}

	users := []SupportRequestUserRef{
		{ID: 1, Email: "user1@example.com", FirstName: "User", LastName: "One"},
		{ID: 7, Email: "user7@example.com", FirstName: "User", LastName: "Seven"},
		{ID: 8, Email: "user8@example.com", FirstName: "User", LastName: "Eight"},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("seed users: %v", err)
	}

	t.Cleanup(func() { _ = sqlDB.Close() })

	mailerSvc := &fakeSupportRequestMailer{}
	return &SupportRequestService{
		DB:                     db,
		Mailer:                 mailerSvc,
		NotificationRecipients: []string{"support@example.com"},
	}, mailerSvc
}

func newGinRouter(userID interface{}, register func(r *gin.Engine)) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	if userID != nil {
		r.Use(func(c *gin.Context) {
			c.Set("userID", userID)
			c.Next()
		})
	}

	register(r)
	return r
}

func doReq(r http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	return rr
}

func doMultipartReq(
	t *testing.T,
	r http.Handler,
	method string,
	target string,
	fields map[string]string,
	fileField string,
	fileHeader *multipart.FileHeader,
) *httptest.ResponseRecorder {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			t.Fatalf("write field %s: %v", key, err)
		}
	}

	if fileHeader != nil {
		file, err := fileHeader.Open()
		if err != nil {
			t.Fatalf("open file header: %v", err)
		}

		part, err := writer.CreateFormFile(fileField, fileHeader.Filename)
		if err != nil {
			_ = file.Close()
			t.Fatalf("create form file: %v", err)
		}

		if _, err := io.Copy(part, file); err != nil {
			_ = file.Close()
			t.Fatalf("copy file: %v", err)
		}
		_ = file.Close()
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	req := httptest.NewRequest(method, target, &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return doReq(r, req)
}

func mustDecode[T any](t *testing.T, rr *httptest.ResponseRecorder) T {
	t.Helper()

	var out T
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode body: %v body=%s", err, rr.Body.String())
	}
	return out
}

func assertStatus(t *testing.T, rr *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rr.Code != want {
		t.Fatalf("expected status %d, got %d, body=%s", want, rr.Code, rr.Body.String())
	}
}

func assertErrorContains(t *testing.T, rr *httptest.ResponseRecorder, wantCode int, want string) {
	t.Helper()
	assertStatus(t, rr, wantCode)

	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !strings.Contains(body["error"], want) {
		t.Fatalf("expected error containing %q, got %q", want, body["error"])
	}
}

func multipartFileHeaderFromBytes(t *testing.T, fieldName string, filename string, contentType string, content []byte) *multipart.FileHeader {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, fieldName, filename))
	header.Set("Content-Type", contentType)

	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatalf("create part: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader := multipart.NewReader(bytes.NewReader(body.Bytes()), writer.Boundary())
	form, err := reader.ReadForm(int64(len(body.Bytes()) + 1024))
	if err != nil {
		t.Fatalf("read form: %v", err)
	}

	files := form.File[fieldName]
	if len(files) == 0 {
		t.Fatalf("expected file header")
	}

	return files[0]
}
