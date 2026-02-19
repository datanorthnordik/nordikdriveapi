package file

import (
	"time"

	"cloud.google.com/go/storage"
	"github.com/lib/pq"
	"gorm.io/datatypes"
)

type File struct {
	ID              uint           `gorm:"primaryKey" json:"id"`
	Filename        string         `gorm:"unique;not null" json:"filename"`
	InsertedBy      uint           `gorm:"not null" json:"inserted_by"`
	CreatedAt       time.Time      `json:"created_at"`
	Private         bool           `json:"private"`
	IsDelete        bool           `json:"is_delete"`
	Size            float64        `json:"size"`
	Version         int            `json:"version"`
	Rows            int            `json:"rows"`
	ColumnsOrder    datatypes.JSON `gorm:"type:jsonb"`
	CommunityFilter bool           `json:"community_filter"`
}

type FileVersion struct {
	ID         uint      `gorm:"primaryKey;autoIncrement" jsoxn:"id"`
	FileID     uint      `gorm:"not null;index" json:"file_id"`
	Filename   string    `gorm:"size:255;not null" json:"filename"`
	InsertedBy uint      `gorm:"not null;index" json:"inserted_by"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	Private    bool      `gorm:"default:false" json:"private"`
	IsDelete   bool      `gorm:"default:false" json:"is_delete"`
	Size       float64   `gorm:"not null" json:"size"`
	Version    int       `gorm:"not null;default:1" json:"version"`
	Rows       int       `gorm:"not null" json:"rows"`
}

type FileVersionWithUser struct {
	ID        uint      `json:"id"`
	FileID    uint      `json:"file_id"`
	FileName  string    `json:"filename" gorm:"column:filename"`
	Firstname string    `json:"firstname" gorm:"column:firstname"`
	Lastname  string    `json:"lastname" gorm:"column:lastname"`
	CreatedAt time.Time `json:"created_at"`
	Private   bool      `json:"private"`
	IsDelete  bool      `json:"is_delete"`
	Size      float64   `json:"size"`
	Version   int       `json:"version"`
	Rows      int       `json:"rows"`
}

type FileData struct {
	ID         uint           `gorm:"primaryKey" json:"id"`
	FileID     uint           `gorm:"not null;index" json:"file_id"`
	RowData    datatypes.JSON `gorm:"type:json" json:"row_data"`
	InsertedBy uint           `gorm:"not null" json:"inserted_by"`
	CreatedAt  time.Time      `json:"created_at"`
	Version    int            `json:"version"`
}

type FileAccess struct {
	ID     uint `gorm:"primaryKey" json:"id"`
	UserID uint `gorm:"type:json" json:"user_id"`
	FileID uint `gorm:"not null;index" json:"file_id"`
}

type RevertFileInput struct {
	Filename string `json:"filename" binding:"required"`
	Version  int    `json:"version" binding:"required"`
}

type FileWithUser struct {
	File
	Firstname string `json:"firstname"`
	Lastname  string `json:"lastname"`
}

type FileEditRequest struct {
	RequestID         uint           `gorm:"primaryKey;autoIncrement" json:"request_id"`
	RowID             int            `gorm:"not null" json:"row_id"`
	UserID            uint           `gorm:"not null" json:"user_id"`
	Status            string         `gorm:"type:varchar(50);default:'pending'" json:"status"`
	FirstName         string         `gorm:"type:varchar(100);column:firstname" json:"firstname"`
	LastName          string         `gorm:"type:varchar(100);column:lastname" json:"lastname"`
	Consent           bool           `gorm:"default:false" json:"consent"`
	ArchiveConsent    bool           `gorm:"column:archive_consent;default:false" json:"archive_consent"`
	CreatedAt         time.Time      `gorm:"autoCreateTime" json:"created_at"`
	IsEdited          bool           `gorm:"default:false" json:"is_edited"`
	FileID            uint           `gorm:"column:file_id;not null;" json:"file_id"`
	ApprovedBy        *int           `gorm:"column:approved_by" json:"approved_by"`
	Community         pq.StringArray `gorm:"type:text[];column:community;default:'{}'" json:"community"`
	UploaderCommunity pq.StringArray `gorm:"type:text[];column:uploader_community;default:'{}'" json:"uploader_community"`
}
type FileEditRequestDetails struct {
	ID        uint      `gorm:"primaryKey;autoIncrement" json:"id"`
	RequestID uint      `gorm:"not null" json:"request_id"`
	FileID    uint      `gorm:"not null" json:"file_id"`
	Filename  string    `gorm:"type:varchar(255);not null" json:"filename"`
	RowID     int       `gorm:"not null" json:"row_id"`
	FieldName string    `gorm:"type:varchar(255);not null" json:"field_name"`
	OldValue  string    `gorm:"type:text" json:"old_value"`
	NewValue  string    `gorm:"type:text" json:"new_value"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

type FileEditRequestWithUser struct {
	RequestID  uint                     `json:"request_id"`
	RowID      int                      `json:"row_id"`
	UserID     uint                     `json:"user_id"`
	Firstname  string                   `json:"firstname"`
	Lastname   string                   `json:"lastname"`
	Status     string                   `json:"status"`
	CreatedAt  time.Time                `json:"created_at"`
	Details    []FileEditRequestDetails `json:"details"`
	EFirstName string                   `json:"efirstname"`
	ELastName  string                   `json:"elastname"`
	IsEdited   bool                     `gorm:"default:true" json:"is_edited"`
	Consent    bool                     `json:"consent"`
}

type EditRequestInput struct {
	FileID    uint                         `json:"file_id"`
	Filename  string                       `json:"filename"`
	Changes   map[string][]EditChangeInput `json:"changes"`
	FirstName string                       `json:"firstname"`
	LastName  string                       `json:"lastname"`
	RowID     int                          `json:"row_id"`

	PhotosInApp      []PhotoInput `json:"photos_in_app"`
	PhotosForGallery []PhotoInput `json:"photos_for_gallery_review"`

	Consent        bool `json:"consent"`
	ArchiveConsent bool `json:"archive_consent"`

	IsEdited bool `json:"is_edited"`

	Documents         []DocumentInput `json:"documents"`
	Community         pq.StringArray  `gorm:"type:text[];column:community;default:'{}'" json:"community"`
	UploaderCommunity pq.StringArray  `gorm:"type:text[];column:uploader_community;default:'{}'" json:"uploader_community"`
}

type DocumentInput struct {
	DocumentType     string `json:"document_type"`
	DocumentCategory string `json:"document_category"`
	Filename         string `json:"filename"`
	MimeType         string `json:"mime_type"`
	Size             int64  `json:"size"`
	DataBase64       string `json:"data_base64"`
}

type EditChangeInput struct {
	RowID     int    `json:"row_id"`
	FieldName string `json:"field_name"`
	OldValue  string `json:"old_value"`
	NewValue  string `json:"new_value"`
}

type FileEditRequestPhoto struct {
	ID               uint      `gorm:"primaryKey" json:"id"`
	RequestID        uint      `json:"request_id"`
	RowID            int       `json:"row_id"`
	PhotoURL         string    `json:"photo_url"`
	FileName         string    `json:"file_name"`
	SizeBytes        int64     `json:"size_bytes"`
	IsGalleryPhoto   bool      `json:"is_gallery_photo"`
	IsApproved       bool      `json:"is_approved"`
	ApprovedBy       string    `json:"approved_by"`
	ApprovedAt       time.Time `json:"approved_at"`
	CreatedAt        time.Time `json:"created_at"`
	SourceFile       string    `json:"source_file"`
	FileID           uint      `json:"file_id"`
	DocumentType     string    `gorm:"type:varchar(20);default:'photos'" json:"document_type"`
	DocumentCategory string    `gorm:"type:varchar(50)" json:"document_category"`
	PhotoComment     string    `gorm:"type:text;column:photo_comment" json:"photo_comment"`
}

type PhotoInput struct {
	Filename   string `json:"filename"`
	MimeType   string `json:"mime_type"`
	Size       int64  `json:"size"`
	DataBase64 string `json:"data_base64"`
	Comment    string `json:"comment"`
}

type gcsReadHandle struct {
	Client *storage.Client
	Reader *storage.Reader
}

func (FileEditRequest) TableName() string {
	return "file_edit_request"
}

func (FileEditRequestDetails) TableName() string {
	return "file_edit_request_details"
}

func (File) TableName() string {
	return "file"
}

func (FileData) TableName() string {
	return "file_data"
}

func (FileAccess) TableName() string {
	return "file_access"
}

func (FileVersion) TableName() string {
	return "file_version"
}

func (FileEditRequestPhoto) TableName() string {
	return "file_edit_request_photos"
}
