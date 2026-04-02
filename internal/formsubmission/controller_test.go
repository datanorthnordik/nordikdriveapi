package formsubmission

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

type mockFormSubmissionService struct {
	getByIDFn               func(id int64) (*GetFormSubmissionResponse, error)
	getActiveByRowAndFormFn func(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error)
	getByRowAndFormFn       func(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error)
	upsertFn                func(req *SaveFormSubmissionRequest, userID int) (*GetFormSubmissionResponse, error)
	getUploadBytesFn        func(id uint) ([]byte, string, string, error)
	searchSubmissionsFn     func(ctx context.Context, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error)
	getFormsByFileIDFn      func(fileID int64) ([]FormFileMappingResponse, error)
	reviewSubmissionFn      func(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error)
	searchMyFn              func(ctx context.Context, userID int, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error)
}

func (m *mockFormSubmissionService) GetByID(id int64) (*GetFormSubmissionResponse, error) {
	return m.getByIDFn(id)
}
func (m *mockFormSubmissionService) GetActiveByRowAndForm(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error) {
	return m.getActiveByRowAndFormFn(rowID, formKey, fileID)
}
func (m *mockFormSubmissionService) GetByRowAndForm(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error) {
	return m.getByRowAndFormFn(rowID, formKey, fileID)
}
func (m *mockFormSubmissionService) Upsert(req *SaveFormSubmissionRequest, userID int) (*GetFormSubmissionResponse, error) {
	return m.upsertFn(req, userID)
}
func (m *mockFormSubmissionService) GetUploadBytes(id uint) ([]byte, string, string, error) {
	return m.getUploadBytesFn(id)
}
func (m *mockFormSubmissionService) SearchSubmissions(ctx context.Context, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
	return m.searchSubmissionsFn(ctx, req, page, pageSize)
}
func (m *mockFormSubmissionService) GetFormsByFileID(fileID int64) ([]FormFileMappingResponse, error) {
	return m.getFormsByFileIDFn(fileID)
}
func (m *mockFormSubmissionService) ReviewSubmission(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error) {
	return m.reviewSubmissionFn(req, reviewerID)
}
func (m *mockFormSubmissionService) SearchMySubmissions(ctx context.Context, userID int, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
	return m.searchMyFn(ctx, userID, req, page, pageSize)
}

func TestRegisterRoutes(t *testing.T) {
	r := newGinRouter(nil, func(r *gin.Engine) {
		RegisterRoutes(r, &mockFormSubmissionService{
			getByIDFn:               func(id int64) (*GetFormSubmissionResponse, error) { return nil, nil },
			getActiveByRowAndFormFn: func(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error) { return nil, nil },
			upsertFn:                func(req *SaveFormSubmissionRequest, userID int) (*GetFormSubmissionResponse, error) { return nil, nil },
			getUploadBytesFn:        func(id uint) ([]byte, string, string, error) { return nil, "", "", nil },
			searchSubmissionsFn: func(ctx context.Context, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
				return nil, nil
			},
			getFormsByFileIDFn: func(fileID int64) ([]FormFileMappingResponse, error) { return nil, nil },
			reviewSubmissionFn: func(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error) {
				return nil, nil
			},
			searchMyFn: func(ctx context.Context, userID int, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
				return nil, nil
			},
		})
	})

	if len(r.Routes()) != 8 {
		t.Fatalf("expected 8 routes, got %d", len(r.Routes()))
	}
}

func TestGetFormSubmission(t *testing.T) {
	t.Run("invalid id", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers", c.GetFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers?id=x", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "valid id is required")
	})

	t.Run("missing id", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers", c.GetFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "valid id is required")
	})

	t.Run("zero id", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers", c.GetFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers?id=0", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "valid id is required")
	})

	t.Run("service error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getByIDFn: func(id int64) (*GetFormSubmissionResponse, error) {
					return nil, errors.New("boom")
				},
			},
		}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers", c.GetFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers?id=1", nil)
		assertErrorContains(t, rr, http.StatusInternalServerError, "boom")
	})

	t.Run("success", func(t *testing.T) {
		var gotID int64

		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getByIDFn: func(id int64) (*GetFormSubmissionResponse, error) {
					gotID = id
					return &GetFormSubmissionResponse{Found: true, ID: id}, nil
				},
			},
		}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers", c.GetFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers?id=42", nil)
		assertStatus(t, rr, http.StatusOK)
		if gotID != 42 {
			t.Fatalf("expected id 42, got %d", gotID)
		}
	})
}

func TestGetActiveFormSubmission(t *testing.T) {
	t.Run("invalid row_id", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers/active", c.GetActiveFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers/active?row_id=x&form_key=f1", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "valid row_id is required")
	})

	t.Run("missing form_key", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers/active", c.GetActiveFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers/active?row_id=1", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "form_key is required")
	})

	t.Run("invalid file_id", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers/active", c.GetActiveFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers/active?row_id=1&form_key=f1&file_id=0", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "invalid file_id")
	})

	t.Run("service error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getActiveByRowAndFormFn: func(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error) {
					return nil, errors.New("boom")
				},
			},
		}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers/active", c.GetActiveFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers/active?row_id=1&form_key=f1", nil)
		assertErrorContains(t, rr, http.StatusInternalServerError, "boom")
	})

	t.Run("success with nil file id", func(t *testing.T) {
		var gotNil bool

		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getActiveByRowAndFormFn: func(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error) {
					gotNil = fileID == nil
					return &GetFormSubmissionResponse{Found: true, RowID: rowID, FormKey: formKey}, nil
				},
			},
		}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers/active", c.GetActiveFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers/active?row_id=1&form_key=f1", nil)
		assertStatus(t, rr, http.StatusOK)
		if !gotNil {
			t.Fatalf("expected nil fileID")
		}
	})

	t.Run("success with file id", func(t *testing.T) {
		var gotFileID int64

		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getActiveByRowAndFormFn: func(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error) {
					gotFileID = *fileID
					return &GetFormSubmissionResponse{Found: true, FileID: *fileID}, nil
				},
			},
		}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers/active", c.GetActiveFormSubmission)
		})

		rr := doReq(r, http.MethodGet, "/api/form/answers/active?row_id=1&form_key=f1&file_id=12", nil)
		assertStatus(t, rr, http.StatusOK)
		if gotFileID != 12 {
			t.Fatalf("expected fileID 12, got %d", gotFileID)
		}
	})
}

func TestSaveFormSubmission(t *testing.T) {
	body := SaveFormSubmissionRequest{
		FileID:    10,
		RowID:     20,
		FormKey:   "boarding",
		FormLabel: "Boarding",
	}

	t.Run("missing user", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.POST("/api/form/answers", c.SaveFormSubmission)
		})

		rr := doJSONReq(r, http.MethodPost, "/api/form/answers", body)
		assertErrorContains(t, rr, http.StatusUnauthorized, "user ID not found")
	})

	t.Run("invalid user type", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter("bad", func(r *gin.Engine) {
			r.POST("/api/form/answers", c.SaveFormSubmission)
		})

		rr := doJSONReq(r, http.MethodPost, "/api/form/answers", body)
		assertErrorContains(t, rr, http.StatusUnauthorized, "invalid user ID")
	})

	t.Run("bind error", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(float64(7), func(r *gin.Engine) {
			r.POST("/api/form/answers", c.SaveFormSubmission)
		})

		rr := doReq(r, http.MethodPost, "/api/form/answers", strings.NewReader("{"))
		assertStatus(t, rr, http.StatusBadRequest)
	})

	t.Run("service error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				upsertFn: func(req *SaveFormSubmissionRequest, userID int) (*GetFormSubmissionResponse, error) {
					return nil, errors.New("save failed")
				},
			},
		}
		r := newGinRouter(float64(7), func(r *gin.Engine) {
			r.POST("/api/form/answers", c.SaveFormSubmission)
		})

		rr := doJSONReq(r, http.MethodPost, "/api/form/answers", body)
		assertErrorContains(t, rr, http.StatusBadRequest, "save failed")
	})

	t.Run("success", func(t *testing.T) {
		var gotUser int

		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				upsertFn: func(req *SaveFormSubmissionRequest, userID int) (*GetFormSubmissionResponse, error) {
					gotUser = userID
					return &GetFormSubmissionResponse{ID: 1, Found: true}, nil
				},
			},
		}
		r := newGinRouter(float64(7), func(r *gin.Engine) {
			r.POST("/api/form/answers", c.SaveFormSubmission)
		})

		rr := doJSONReq(r, http.MethodPost, "/api/form/answers", body)
		assertStatus(t, rr, http.StatusOK)
		if gotUser != 7 {
			t.Fatalf("expected user 7, got %d", gotUser)
		}
	})
}

func TestGetUpload(t *testing.T) {
	t.Run("missing user", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form/answers/upload/:id", c.GetUpload)
		})
		rr := doReq(r, http.MethodGet, "/api/form/answers/upload/1", nil)
		assertErrorContains(t, rr, http.StatusUnauthorized, "user ID not found")
	})

	t.Run("invalid user type", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter("bad", func(r *gin.Engine) {
			r.GET("/api/form/answers/upload/:id", c.GetUpload)
		})
		rr := doReq(r, http.MethodGet, "/api/form/answers/upload/1", nil)
		assertErrorContains(t, rr, http.StatusUnauthorized, "invalid user ID")
	})

	t.Run("invalid id", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.GET("/api/form/answers/upload/:id", c.GetUpload)
		})
		rr := doReq(r, http.MethodGet, "/api/form/answers/upload/0", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "invalid id")
	})

	t.Run("service error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getUploadBytesFn: func(id uint) ([]byte, string, string, error) {
					return nil, "", "", errors.New("download failed")
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.GET("/api/form/answers/upload/:id", c.GetUpload)
		})
		rr := doReq(r, http.MethodGet, "/api/form/answers/upload/1", nil)
		assertErrorContains(t, rr, http.StatusInternalServerError, "download failed")
	})

	t.Run("inline image", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getUploadBytesFn: func(id uint) ([]byte, string, string, error) {
					return []byte("img"), "image/png", "x.png", nil
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.GET("/api/form/answers/upload/:id", c.GetUpload)
		})
		rr := doReq(r, http.MethodGet, "/api/form/answers/upload/1", nil)
		assertStatus(t, rr, http.StatusOK)
		if rr.Header().Get("Content-Disposition") != `inline; filename="x.png"` {
			t.Fatalf("unexpected disposition: %q", rr.Header().Get("Content-Disposition"))
		}
	})

	t.Run("inline pdf", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getUploadBytesFn: func(id uint) ([]byte, string, string, error) {
					return []byte("pdf"), "application/pdf", "x.pdf", nil
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.GET("/api/form/answers/upload/:id", c.GetUpload)
		})
		rr := doReq(r, http.MethodGet, "/api/form/answers/upload/1", nil)
		assertStatus(t, rr, http.StatusOK)
		if rr.Header().Get("Content-Disposition") != `inline; filename="x.pdf"` {
			t.Fatalf("unexpected disposition: %q", rr.Header().Get("Content-Disposition"))
		}
	})

	t.Run("attachment for non image non pdf", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getUploadBytesFn: func(id uint) ([]byte, string, string, error) {
					return []byte("doc"), "application/msword", "x.doc", nil
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.GET("/api/form/answers/upload/:id", c.GetUpload)
		})
		rr := doReq(r, http.MethodGet, "/api/form/answers/upload/1", nil)
		assertStatus(t, rr, http.StatusOK)
		if rr.Header().Get("Content-Disposition") != `attachment; filename="x.doc"` {
			t.Fatalf("unexpected disposition: %q", rr.Header().Get("Content-Disposition"))
		}
	})
}

func TestSearchFormSubmissions(t *testing.T) {
	t.Run("missing user", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.POST("/api/form/search", c.SearchFormSubmissions)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/search", SearchFormSubmissionsRequest{})
		assertErrorContains(t, rr, http.StatusUnauthorized, "user ID not found")
	})

	t.Run("invalid user type", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter("bad", func(r *gin.Engine) {
			r.POST("/api/form/search", c.SearchFormSubmissions)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/search", SearchFormSubmissionsRequest{})
		assertErrorContains(t, rr, http.StatusUnauthorized, "invalid user ID")
	})

	t.Run("bind error", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/search", c.SearchFormSubmissions)
		})
		rr := doReq(r, http.MethodPost, "/api/form/search", strings.NewReader("{"))
		assertStatus(t, rr, http.StatusBadRequest)
	})

	t.Run("service error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				searchSubmissionsFn: func(ctx context.Context, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
					return nil, errors.New("search failed")
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/search", c.SearchFormSubmissions)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/search", SearchFormSubmissionsRequest{})
		assertErrorContains(t, rr, http.StatusInternalServerError, "search failed")
	})

	t.Run("normalizes request", func(t *testing.T) {
		var gotReq SearchFormSubmissionsRequest
		var gotPage, gotSize int

		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				searchSubmissionsFn: func(ctx context.Context, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
					gotReq = req
					gotPage = page
					gotSize = pageSize
					return &PaginatedFormSubmissionsResponse{}, nil
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/search", c.SearchFormSubmissions)
		})

		req := SearchFormSubmissionsRequest{
			Page:      0,
			PageSize:  101,
			FormKey:   strPtr("  boarding  "),
			FirstName: strPtr("   "),
			LastName:  strPtr("  Doe "),
			Status:    []string{" pending ", "", "approved", "pending"},
		}

		rr := doJSONReq(r, http.MethodPost, "/api/form/search", req)
		assertStatus(t, rr, http.StatusOK)

		if gotPage != 1 || gotSize != 100 {
			t.Fatalf("expected page=1 size=100 got %d %d", gotPage, gotSize)
		}
		if gotReq.FormKey == nil || *gotReq.FormKey != "boarding" {
			t.Fatalf("unexpected form key: %#v", gotReq.FormKey)
		}
		if gotReq.FirstName != nil {
			t.Fatalf("expected nil first name")
		}
		if gotReq.LastName == nil || *gotReq.LastName != "Doe" {
			t.Fatalf("unexpected last name: %#v", gotReq.LastName)
		}
		if len(gotReq.Status) != 2 || gotReq.Status[0] != "pending" || gotReq.Status[1] != "approved" {
			t.Fatalf("unexpected status: %#v", gotReq.Status)
		}
	})
}

func TestGetFormsByFileID_Controller(t *testing.T) {
	t.Run("invalid file id", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form", c.GetFormsByFileID)
		})
		rr := doReq(r, http.MethodGet, "/api/form?file_id=x", nil)
		assertErrorContains(t, rr, http.StatusBadRequest, "valid file_id is required")
	})

	t.Run("service error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getFormsByFileIDFn: func(fileID int64) ([]FormFileMappingResponse, error) {
					return nil, errors.New("db error")
				},
			},
		}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form", c.GetFormsByFileID)
		})
		rr := doReq(r, http.MethodGet, "/api/form?file_id=1", nil)
		assertErrorContains(t, rr, http.StatusInternalServerError, "db error")
	})

	t.Run("success", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				getFormsByFileIDFn: func(fileID int64) ([]FormFileMappingResponse, error) {
					return []FormFileMappingResponse{{ID: 1, FileID: fileID, FormKey: "boarding"}}, nil
				},
			},
		}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.GET("/api/form", c.GetFormsByFileID)
		})
		rr := doReq(r, http.MethodGet, "/api/form?file_id=1", nil)
		assertStatus(t, rr, http.StatusOK)
	})
}

func TestReviewFormSubmission_Controller(t *testing.T) {
	req := ReviewFormSubmissionRequest{
		SubmissionID:     1,
		SubmissionReview: &SubmissionReviewInput{Status: "approved"},
	}

	t.Run("missing user", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/answers/review", req)
		assertErrorContains(t, rr, http.StatusUnauthorized, "user ID not found")
	})

	t.Run("invalid user type", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter("bad", func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/answers/review", req)
		assertErrorContains(t, rr, http.StatusUnauthorized, "invalid user ID")
	})

	t.Run("bind error", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doReq(r, http.MethodPost, "/api/form/answers/review", strings.NewReader("{"))
		assertStatus(t, rr, http.StatusBadRequest)
	})

	t.Run("invalid request error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				reviewSubmissionFn: func(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error) {
					return nil, ErrInvalidReviewRequest
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/answers/review", req)
		assertErrorContains(t, rr, http.StatusBadRequest, "invalid review request")
	})

	t.Run("not found error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				reviewSubmissionFn: func(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error) {
					return nil, ErrFormSubmissionNotFound
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/answers/review", req)
		assertErrorContains(t, rr, http.StatusNotFound, "form submission not found")
	})

	t.Run("upload not found error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				reviewSubmissionFn: func(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error) {
					return nil, ErrUploadNotFoundForSubmission
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/answers/review", req)
		assertErrorContains(t, rr, http.StatusBadRequest, "one or more uploads do not belong")
	})

	t.Run("internal error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				reviewSubmissionFn: func(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error) {
					return nil, errors.New("db down")
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/answers/review", req)
		assertErrorContains(t, rr, http.StatusInternalServerError, "db down")
	})

	t.Run("success", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				reviewSubmissionFn: func(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error) {
					return &GetFormSubmissionResponse{ID: 1, Found: true}, nil
				},
			},
		}
		r := newGinRouter(float64(1), func(r *gin.Engine) {
			r.POST("/api/form/answers/review", c.ReviewFormSubmission)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/answers/review", req)
		assertStatus(t, rr, http.StatusOK)
	})
}

func TestSearchMyFormSubmissions_Controller(t *testing.T) {
	t.Run("missing user", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(nil, func(r *gin.Engine) {
			r.POST("/api/form/my-requests", c.SearchMyFormSubmissions)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/my-requests", SearchFormSubmissionsRequest{})
		assertErrorContains(t, rr, http.StatusUnauthorized, "user ID not found")
	})

	t.Run("invalid user type", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter("bad", func(r *gin.Engine) {
			r.POST("/api/form/my-requests", c.SearchMyFormSubmissions)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/my-requests", SearchFormSubmissionsRequest{})
		assertErrorContains(t, rr, http.StatusUnauthorized, "invalid user ID")
	})

	t.Run("bind error", func(t *testing.T) {
		c := &FormSubmissionController{}
		r := newGinRouter(float64(42), func(r *gin.Engine) {
			r.POST("/api/form/my-requests", c.SearchMyFormSubmissions)
		})
		rr := doReq(r, http.MethodPost, "/api/form/my-requests", strings.NewReader("{"))
		assertStatus(t, rr, http.StatusBadRequest)
	})

	t.Run("service error", func(t *testing.T) {
		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				searchMyFn: func(ctx context.Context, userID int, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
					return nil, errors.New("search failed")
				},
			},
		}
		r := newGinRouter(float64(42), func(r *gin.Engine) {
			r.POST("/api/form/my-requests", c.SearchMyFormSubmissions)
		})
		rr := doJSONReq(r, http.MethodPost, "/api/form/my-requests", SearchFormSubmissionsRequest{})
		assertErrorContains(t, rr, http.StatusInternalServerError, "search failed")
	})

	t.Run("normalizes request", func(t *testing.T) {
		var gotUser int
		var gotReq SearchFormSubmissionsRequest
		var gotPage, gotSize int

		c := &FormSubmissionController{
			FormSubmissionService: &mockFormSubmissionService{
				searchMyFn: func(ctx context.Context, userID int, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error) {
					gotUser = userID
					gotReq = req
					gotPage = page
					gotSize = pageSize
					return &PaginatedFormSubmissionsResponse{}, nil
				},
			},
		}
		r := newGinRouter(float64(42), func(r *gin.Engine) {
			r.POST("/api/form/my-requests", c.SearchMyFormSubmissions)
		})

		req := SearchFormSubmissionsRequest{
			Page:      -1,
			PageSize:  0,
			FormKey:   strPtr(" boarding "),
			FirstName: strPtr("   "),
			LastName:  strPtr("  N "),
			Status:    []string{" pending ", "", "approved", "pending"},
		}

		rr := doJSONReq(r, http.MethodPost, "/api/form/my-requests", req)
		assertStatus(t, rr, http.StatusOK)

		if gotUser != 42 || gotPage != 1 || gotSize != 20 {
			t.Fatalf("unexpected user/page/size: %d %d %d", gotUser, gotPage, gotSize)
		}
		if gotReq.FormKey == nil || *gotReq.FormKey != "boarding" {
			t.Fatalf("unexpected form key: %#v", gotReq.FormKey)
		}
		if gotReq.FirstName != nil {
			t.Fatalf("expected nil first name")
		}
		if gotReq.LastName == nil || *gotReq.LastName != "N" {
			t.Fatalf("unexpected last name: %#v", gotReq.LastName)
		}
		if len(gotReq.Status) != 2 {
			t.Fatalf("unexpected status: %#v", gotReq.Status)
		}
	})
}
