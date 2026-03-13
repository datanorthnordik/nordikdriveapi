package formsubmission

import "context"

type FormSubmissionServiceInterface interface {
	GetByRowAndForm(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error)
	Upsert(req *SaveFormSubmissionRequest, userID int) (*GetFormSubmissionResponse, error)
	GetUploadBytes(id uint) ([]byte, string, string, error)
	SearchSubmissions(ctx context.Context, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error)
	GetFormsByFileID(fileID int64) ([]FormFileMappingResponse, error)
	ReviewSubmission(req *ReviewFormSubmissionRequest, reviewerID int) (*GetFormSubmissionResponse, error)
	SearchMySubmissions(ctx context.Context, userID int, req SearchFormSubmissionsRequest, page int, pageSize int) (*PaginatedFormSubmissionsResponse, error)
}

var _ FormSubmissionServiceInterface = (*FormSubmissionService)(nil)
