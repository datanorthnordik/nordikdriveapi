package logocontent

import "time"

type FileLogoContent struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	FileID    uint      `gorm:"not null;uniqueIndex" json:"file_id"`
	FileName  string    `gorm:"not null" json:"file_name"`
	LogoURL   string    `gorm:"column:logo_url;type:text;not null" json:"logo_url"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (FileLogoContent) TableName() string {
	return "file_logo_content"
}
