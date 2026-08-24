package file

import (
	"time"

	"github.com/lib/pq"
)

// AchieverStory is a row-scoped survivor/achiever story. A row can have more
// than one story and each story can be a stored document, written content, or
// a stored/externally hosted video.
type AchieverStory struct {
	ID          uint `gorm:"primaryKey" json:"id"`
	FileID      uint `gorm:"not null;index" json:"file_id"`
	FileVersion int  `gorm:"not null;default:1" json:"file_version"`
	RowID       uint `gorm:"not null;index" json:"row_id"`

	StoryType string `gorm:"type:varchar(20);not null" json:"story_type"`
	Status    string `gorm:"type:varchar(20);not null;default:approved" json:"status"`

	StoryText        string `gorm:"type:text" json:"story_text"`
	VideoURL         string `gorm:"type:text" json:"video_url"`
	StoryURL         string `gorm:"type:text" json:"story_url"`
	OriginalStoryURL string `gorm:"type:text" json:"original_story_url"`
	FileName         string `gorm:"type:varchar(512)" json:"file_name"`
	ContentType      string `gorm:"type:varchar(255)" json:"content_type"`
	SizeBytes        int64  `json:"size_bytes"`

	AchieverStoryIdentified string         `gorm:"type:text" json:"achiever_story_identified"`
	GoogleDetails           string         `gorm:"type:text" json:"google_details"`
	NewspapersDetails       string         `gorm:"type:text" json:"newspapers_details"`
	AncestryDetails         string         `gorm:"type:text" json:"ancestry_details"`
	DerivationSources       pq.StringArray `gorm:"type:text[];default:'{}'" json:"derivation_sources"`

	SourceWorkbook  string     `gorm:"type:varchar(512)" json:"source_workbook"`
	SourceSheet     string     `gorm:"type:varchar(255)" json:"source_sheet"`
	SourceRow       int        `json:"source_row"`
	RequestID       *uint      `gorm:"index" json:"request_id"`
	CreatedBy       *uint      `json:"created_by"`
	ReviewedBy      *uint      `json:"reviewed_by"`
	ReviewerComment string     `gorm:"type:text;not null;default:''" json:"reviewer_comment"`
	ReviewedAt      *time.Time `json:"reviewed_at"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (AchieverStory) TableName() string { return "file_row_achiever_stories" }
