package supportrequest

import (
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

type mockSupportRequestService struct {
	createFn       func(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error)
	listMineFn     func(userID, page, pageSize int) (*SupportRequestListResponse, error)
	listForAdminFn func(userID, page, pageSize int) (*SupportRequestListResponse, error)
	updateFn       func(id int64, req *UpdateSupportRequestRequest, adminUserID int) (*SupportRequest, error)
}

func (m *mockSupportRequestService) Create(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error) {
	return m.createFn(req, userID, screenshot)
}

func (m *mockSupportRequestService) ListMine(userID, page, pageSize int) (*SupportRequestListResponse, error) {
	if m.listMineFn == nil {
		return nil, nil
	}
	return m.listMineFn(userID, page, pageSize)
}

func (m *mockSupportRequestService) ListForAdmin(userID, page, pageSize int) (*SupportRequestListResponse, error) {
	if m.listForAdminFn == nil {
		return nil, nil
	}
	return m.listForAdminFn(userID, page, pageSize)
}

func (m *mockSupportRequestService) Update(id int64, req *UpdateSupportRequestRequest, adminUserID int) (*SupportRequest, error) {
	if m.updateFn == nil {
		return nil, nil
	}
	return m.updateFn(id, req, adminUserID)
}

func TestRegisterRoutes(t *testing.T) {
	r := newGinRouter(nil, func(r *gin.Engine) {
		RegisterRoutes(r, &mockSupportRequestService{
			createFn: func(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error) {
				return nil, nil
			},
		})
	})

	if len(r.Routes()) != 4 {
		t.Fatalf("expected 4 routes, got %d", len(r.Routes()))
	}
}

func TestCreateSupportRequestController(t *testing.T) {
	fields := map[string]string{
		"request_type": "question",
		"subject":      "Need help",
		"message":      "Please call me back.",
	}

	t.Run("missing user", func(t *testing.T) {
		controller := &SupportRequestController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.POST("/api/support-requests", controller.Create)
		})

		rr := doMultipartReq(t, r, http.MethodPost, "/api/support-requests", fields, "", nil)
		assertErrorContains(t, rr, http.StatusUnauthorized, "user ID not found")
	})

	t.Run("invalid user type", func(t *testing.T) {
		controller := &SupportRequestController{}
		r := newGinRouter("bad", func(r *gin.Engine) {
			r.POST("/api/support-requests", controller.Create)
		})

		rr := doMultipartReq(t, r, http.MethodPost, "/api/support-requests", fields, "", nil)
		assertErrorContains(t, rr, http.StatusUnauthorized, "invalid user ID")
	})

	t.Run("bind error", func(t *testing.T) {
		controller := &SupportRequestController{}
		r := newGinRouter(float64(7), func(r *gin.Engine) {
			r.POST("/api/support-requests", controller.Create)
		})

		req := httptest.NewRequest(http.MethodPost, "/api/support-requests", strings.NewReader("{"))
		req.Header.Set("Content-Type", "application/json")
		rr := doReq(r, req)
		assertStatus(t, rr, http.StatusBadRequest)
	})

	t.Run("service validation error", func(t *testing.T) {
		controller := &SupportRequestController{
			SupportRequestService: &mockSupportRequestService{
				createFn: func(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error) {
					return nil, fmt.Errorf("%w: subject is required", ErrInvalidSupportRequest)
				},
			},
		}
		r := newGinRouter(float64(7), func(r *gin.Engine) {
			r.POST("/api/support-requests", controller.Create)
		})

		rr := doMultipartReq(t, r, http.MethodPost, "/api/support-requests", fields, "", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "subject is required")
	})

	t.Run("service error", func(t *testing.T) {
		controller := &SupportRequestController{
			SupportRequestService: &mockSupportRequestService{
				createFn: func(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error) {
					return nil, errors.New("db down")
				},
			},
		}
		r := newGinRouter(float64(7), func(r *gin.Engine) {
			r.POST("/api/support-requests", controller.Create)
		})

		rr := doMultipartReq(t, r, http.MethodPost, "/api/support-requests", fields, "", nil)
		assertErrorContains(t, rr, http.StatusInternalServerError, "db down")
	})

	t.Run("success", func(t *testing.T) {
		var gotUserID int
		var gotRequestType string
		var gotFileName string

		screenshot := multipartFileHeaderFromBytes(
			t,
			"screenshot",
			"preview.png",
			"image/png",
			[]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'},
		)

		controller := &SupportRequestController{
			SupportRequestService: &mockSupportRequestService{
				createFn: func(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error) {
					gotUserID = userID
					gotRequestType = req.RequestType
					if screenshot != nil {
						gotFileName = screenshot.Filename
					}
					return &CreateSupportRequestResponse{ID: 99, Message: "Support request received successfully."}, nil
				},
			},
		}
		r := newGinRouter(float64(7), func(r *gin.Engine) {
			r.POST("/api/support-requests", controller.Create)
		})

		rr := doMultipartReq(t, r, http.MethodPost, "/api/support-requests", fields, "screenshot", screenshot)
		assertStatus(t, rr, http.StatusCreated)
		if gotUserID != 7 {
			t.Fatalf("expected user 7, got %d", gotUserID)
		}
		if gotRequestType != "question" || gotFileName != "preview.png" {
			t.Fatalf("unexpected payload: %q %q", gotRequestType, gotFileName)
		}
	})
}
