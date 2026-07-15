package honour

import (
	"time"

	"gorm.io/gorm"
)

const (
	honourStatusProcessing = "processing"
	honourStatusReady      = "ready"
	honourStatusFailed     = "failed"
)

type DailyHonour struct {
	ID           uint       `gorm:"primaryKey" json:"id"`
	FileID       uint       `gorm:"not null;uniqueIndex:idx_file_daily_honours_file_date,priority:1;index:idx_file_daily_honours_file_cycle_row,priority:1;index" json:"file_id"`
	FileVersion  int        `gorm:"not null" json:"file_version"`
	SourceRowID  uint       `gorm:"not null;index:idx_file_daily_honours_file_cycle_row,priority:3;index" json:"source_row_id"`
	HonourDate   time.Time  `gorm:"not null;uniqueIndex:idx_file_daily_honours_file_date,priority:2;index" json:"honour_date"`
	CycleNumber  int        `gorm:"not null;default:1;index:idx_file_daily_honours_file_cycle_row,priority:2;index" json:"cycle_number"`
	HonourText   string     `gorm:"type:text;not null;default:''" json:"honour_text"`
	Status       string     `gorm:"type:varchar(20);not null;default:'ready';index" json:"status"`
	ErrorMessage string     `gorm:"type:text;not null;default:''" json:"error_message"`
	GeneratedAt  *time.Time `json:"generated_at"`
	CreatedAt    time.Time  `gorm:"not null;autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time  `gorm:"not null;autoUpdateTime" json:"updated_at"`
}

func (DailyHonour) TableName() string {
	return "file_daily_honours"
}

func ClearForFileTx(tx *gorm.DB, fileID uint) error {
	if tx == nil || fileID == 0 {
		return nil
	}
	if !tx.Migrator().HasTable(&DailyHonour{}) {
		return nil
	}
	return tx.Where("file_id = ?", fileID).Delete(&DailyHonour{}).Error
}
