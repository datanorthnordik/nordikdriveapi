package file

import (
	"time"

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
	Filename   string    `gorm:"size:255;unique;not null" json:"filename"`
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
	RequestID uint      `gorm:"primaryKey;autoIncrement" json:"request_id"`
	UserID    uint      `gorm:"not null" json:"user_id"`
	Status    string    `gorm:"type:varchar(50);default:'pending'" json:"status"`
	FirstName string    `gorm:"type:varchar(100);column:firstname" json:"firstname"`
	LastName  string    `gorm:"type:varchar(100);column:lastname" json:"lastname"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
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
	UserID     uint                     `json:"user_id"`
	Firstname  string                   `json:"firstname"`
	Lastname   string                   `json:"lastname"`
	Status     string                   `json:"status"`
	CreatedAt  time.Time                `json:"created_at"`
	Details    []FileEditRequestDetails `json:"details"`
	EFirstName string                   `json:"efirstname"`
	ELastName  string                   `json:"elastname"`
}

type EditRequestInput struct {
	FileID    uint                       `json:"file_id"`
	Filename  string                     `json:"filename"`
	Changes   map[string]EditChangeInput `json:"changes"`
	FirstName string                     `json:"firstname"`
	LastName  string                     `json:"lastname"`
}

type EditChangeInput struct {
	RowID     int    `json:"row_id"`
	FieldName string `json:"field_name"`
	OldValue  string `json:"old_value"`
	NewValue  string `json:"new_value"`
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
