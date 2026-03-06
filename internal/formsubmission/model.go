package formsubmission

import (
	"time"

	"gorm.io/datatypes"
)

type FormSubmissionUserRef struct {
	ID    int    `gorm:"column:id;primaryKey"`
	Email string `gorm:"column:email"`
}

func (FormSubmissionUserRef) TableName() string { return "users" }

type FormSubmission struct {
	ID           int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	FileID       int64     `json:"file_id" gorm:"not null;uniqueIndex:uq_form_submissions_file_row_form"`
	RowID        int64     `json:"row_id" gorm:"not null;uniqueIndex:uq_form_submissions_file_row_form"`
	FileName     string    `json:"file_name" gorm:"type:text;not null;default:''"`
	FormKey      string    `json:"form_key" gorm:"type:varchar(150);not null;uniqueIndex:uq_form_submissions_file_row_form"`
	FormLabel    string    `json:"form_label" gorm:"type:text;not null"`
	ConsentText  string    `json:"consent_text" gorm:"type:text;not null;default:''"`
	ConsentGiven bool      `json:"consent_given" gorm:"not null;default:false"`
	CreatedAt    time.Time `json:"created_at" gorm:"not null;autoCreateTime"`
	UpdatedAt    time.Time `json:"updated_at" gorm:"not null;autoUpdateTime"`
	FirstName    string    `json:"firstname" gorm:"type:varchar(100);column:firstname;not null;default:''"`
	LastName     string    `json:"lastname" gorm:"type:varchar(100);column:lastname;not null;default:''"`

	CreatedByID  *int   `json:"-" gorm:"column:created_by"`
	EditedByID   *int   `json:"-" gorm:"column:edited_by"`
	ReviewedByID *int   `json:"-" gorm:"column:reviewed_by"`
	Status       string `json:"status" gorm:"column:status;not null;default:'pending'"`

	CreatedByUser  *FormSubmissionUserRef `json:"-" gorm:"foreignKey:CreatedByID;references:ID"`
	EditedByUser   *FormSubmissionUserRef `json:"-" gorm:"foreignKey:EditedByID;references:ID"`
	ReviewedByUser *FormSubmissionUserRef `json:"-" gorm:"foreignKey:ReviewedByID;references:ID"`
}

func (FormSubmission) TableName() string { return "form_submissions" }

type FormSubmissionDetail struct {
	ID              int64          `json:"id" gorm:"primaryKey;autoIncrement"`
	SubmissionID    int64          `json:"submission_id" gorm:"not null;index;uniqueIndex:uq_form_submission_details_submission_key"`
	DetailKey       string         `json:"detail_key" gorm:"type:varchar(200);not null;uniqueIndex:uq_form_submission_details_submission_key"`
	DetailLabel     string         `json:"detail_label" gorm:"type:text;not null"`
	FieldType       string         `json:"field_type" gorm:"type:varchar(50);not null"`
	ConsentRequired bool           `json:"consent_required" gorm:"not null;default:false"`
	ValueJSON       datatypes.JSON `json:"value_json" gorm:"type:jsonb"`
	CreatedAt       time.Time      `json:"created_at" gorm:"not null;autoCreateTime"`
	UpdatedAt       time.Time      `json:"updated_at" gorm:"not null;autoUpdateTime"`
}

func (FormSubmissionDetail) TableName() string { return "form_submission_details" }

type FormSubmissionUpload struct {
	ID            int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	SubmissionID  int64     `json:"submission_id" gorm:"not null;index"`
	DetailID      int64     `json:"detail_id" gorm:"not null;index"`
	UploadType    string    `json:"upload_type" gorm:"type:varchar(20);not null;index"`
	FileName      string    `json:"file_name" gorm:"type:text;not null"`
	MimeType      string    `json:"mime_type" gorm:"type:text;not null;default:''"`
	FileSizeBytes int64     `json:"file_size_bytes" gorm:"not null;default:0"`
	FileURL       string    `json:"file_url" gorm:"type:text"`
	FileCategory  string    `json:"file_category" gorm:"type:text;not null;default:''"`
	FileComment   string    `json:"file_comment" gorm:"type:text;not null;default:''"`
	CreatedAt     time.Time `json:"created_at" gorm:"not null;autoCreateTime"`
}

func (FormSubmissionUpload) TableName() string { return "form_submission_uploads" }

type FormSubmissionDetailInput struct {
	DetailKey       string      `json:"detail_key"`
	DetailLabel     string      `json:"detail_label"`
	FieldType       string      `json:"field_type"`
	ConsentRequired bool        `json:"consent_required"`
	Value           interface{} `json:"value"`
}

type FormSubmissionUploadInput struct {
	DetailKey     string `json:"detail_key"`
	FileName      string `json:"file_name"`
	MimeType      string `json:"mime_type"`
	FileSizeBytes int64  `json:"file_size_bytes"`
	FileURL       string `json:"file_url"`
	FileCategory  string `json:"file_category"`
	FileComment   string `json:"file_comment"`
	DataBase64    string `json:"data_base64"`
}

type SaveFormSubmissionRequest struct {
	FileID      int64                       `json:"file_id" binding:"required"`
	RowID       int64                       `json:"row_id" binding:"required"`
	FileName    string                      `json:"file_name"`
	FormKey     string                      `json:"form_key" binding:"required"`
	FormLabel   string                      `json:"form_label" binding:"required"`
	ConsentText string                      `json:"consent_text"`
	Consent     bool                        `json:"consent"`
	Details     []FormSubmissionDetailInput `json:"details"`
	Documents   []FormSubmissionUploadInput `json:"documents"`
	Photos      []FormSubmissionUploadInput `json:"photos"`
	FirstName   string                      `json:"firstname"`
	LastName    string                      `json:"lastname"`
}

type FormSubmissionDetailResponse struct {
	DetailKey       string      `json:"detail_key"`
	DetailLabel     string      `json:"detail_label"`
	FieldType       string      `json:"field_type"`
	ConsentRequired bool        `json:"consent_required"`
	Value           interface{} `json:"value"`
}

type FormSubmissionUploadResponse struct {
	ID            int64  `json:"id"`
	DetailKey     string `json:"detail_key"`
	FileName      string `json:"file_name"`
	MimeType      string `json:"mime_type"`
	FileSizeBytes int64  `json:"file_size_bytes"`
	FileURL       string `json:"file_url"`
	FileCategory  string `json:"file_category"`
	FileComment   string `json:"file_comment"`
}

type GetFormSubmissionResponse struct {
	Found       bool                           `json:"found"`
	FileID      int64                          `json:"file_id"`
	RowID       int64                          `json:"row_id"`
	FileName    string                         `json:"file_name"`
	FormKey     string                         `json:"form_key"`
	FormLabel   string                         `json:"form_label"`
	ConsentText string                         `json:"consent_text"`
	Consent     bool                           `json:"consent"`
	Details     []FormSubmissionDetailResponse `json:"details"`
	Documents   []FormSubmissionUploadResponse `json:"documents"`
	Photos      []FormSubmissionUploadResponse `json:"photos"`
	FirstName   string                         `json:"firstname"`
	LastName    string                         `json:"lastname"`
	CreatedBy   string                         `json:"created_by"`
	EditedBy    string                         `json:"edited_by"`
	ReviewedBy  string                         `json:"reviewed_by"`
	Status      string                         `json:"status"`
}

type SearchFormSubmissionsRequest struct {
	FileID       *int64  `json:"file_id"`
	FormKey      *string `json:"form_key"`
	FirstName    *string `json:"firstname"`
	LastName     *string `json:"lastname"`
	ConsentGiven *bool   `json:"consent_given"`
	CreatedBy    *int    `json:"created_by"`

	Page     int `json:"page"`
	PageSize int `json:"page_size"`
}

type FormSubmissionListItemResponse struct {
	ID           int64     `json:"id"`
	FileID       int64     `json:"file_id"`
	RowID        int64     `json:"row_id"`
	FileName     string    `json:"file_name"`
	FormKey      string    `json:"form_key"`
	FormLabel    string    `json:"form_label"`
	ConsentText  string    `json:"consent_text"`
	ConsentGiven bool      `json:"consent_given"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	FirstName    string    `json:"firstname"`
	LastName     string    `json:"lastname"`
	CreatedBy    string    `json:"created_by"`
	EditedBy     string    `json:"edited_by"`
	ReviewedBy   string    `json:"reviewed_by"`
	Status       string    `json:"status"`
}

type PaginatedFormSubmissionsResponse struct {
	Page       int                              `json:"page"`
	PageSize   int                              `json:"page_size"`
	TotalItems int64                            `json:"total_items"`
	TotalPages int                              `json:"total_pages"`
	Items      []FormSubmissionListItemResponse `json:"items"`
}

type FormFileMapping struct {
	ID       int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	FileName string `json:"file_name" gorm:"type:text;not null;default:''"`
	FileID   int64  `json:"file_id" gorm:"not null;index;uniqueIndex:uq_form_file_mappings_file_form"`
	FormKey  string `json:"form_key" gorm:"type:varchar(150);not null;uniqueIndex:uq_form_file_mappings_file_form"`
	FormName string `json:"form_name" gorm:"type:text;not null"`
}

func (FormFileMapping) TableName() string { return "form_file_mappings" }

type FormFileMappingResponse struct {
	ID       int64  `json:"id"`
	FileName string `json:"file_name"`
	FileID   int64  `json:"file_id"`
	FormKey  string `json:"form_key"`
	FormName string `json:"form_name"`
}
