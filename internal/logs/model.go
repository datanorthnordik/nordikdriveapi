package logs

import (
	"time"

	"github.com/lib/pq"
)

type SystemLog struct {
	ID          uint           `gorm:"primaryKey;autoIncrement" json:"id"`
	Level       string         `gorm:"size:20;not null" json:"level"`
	Service     string         `gorm:"size:100;not null" json:"service"`
	UserID      *uint          `gorm:"index" json:"user_id,omitempty"`
	Action      string         `gorm:"size:255;not null" json:"action"`
	Message     string         `gorm:"type:text;not null" json:"message"`
	Filename    *string        `gorm:"size:512" json:"filename,omitempty"`
	Communities pq.StringArray `gorm:"type:text[];column:communities" json:"communities"`
	Metadata    *string        `gorm:"type:jsonb" json:"metadata,omitempty"`
	CreatedAt   time.Time      `gorm:"autoCreateTime" json:"created_at"`
}

type LogFilterInput struct {
	UserID      *uint    `json:"user_id"`
	Level       *string  `json:"level"`
	Service     *string  `json:"service"`
	Action      *string  `json:"action"`
	Filename    *string  `json:"filename"`
	Communities []string `json:"communities"` // ✅ array input

	StartDate *string `json:"start_date"` // "YYYY-MM-DD"
	EndDate   *string `json:"end_date"`   // "YYYY-MM-DD"

	Search   *string `json:"search"`
	Page     int     `json:"page"`
	PageSize int     `json:"page_size"`
}

type AggItem struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
}

type PersonAggItem struct {
	UserID    *uint  `json:"user_id,omitempty"`
	Firstname string `json:"firstname"`
	Lastname  string `json:"lastname"`
	Label     string `json:"label"`
	Count     int64  `json:"count"`
}

type LogAggregates struct {
	ByCommunity []AggItem       `json:"by_community"`
	ByFilename  []AggItem       `json:"by_filename"`
	ByPerson    []PersonAggItem `json:"by_person"`
}

type LogRow struct {
	SystemLog
	Firstname string `json:"firstname" gorm:"column:firstname"`
	Lastname  string `json:"lastname" gorm:"column:lastname"`
}

func (SystemLog) TableName() string {
	return "logs"
}
