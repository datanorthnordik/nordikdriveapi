package dataconfig

import (
	"time"

	"gorm.io/datatypes"
)

type DataConfig struct {
	ID        int64          `json:"id" gorm:"primaryKey;autoIncrement"`
	FileID    int64          `json:"file_id" gorm:"uniqueIndex;not null"`
	FileName  string         `json:"file_name" gorm:"type:text;not null"`
	Version   int            `json:"version" gorm:"not null;default:1"`
	Checksum  string         `json:"checksum" gorm:"type:text;not null"`
	Config    datatypes.JSON `json:"config" gorm:"type:jsonb;not null"`
	IsActive  bool           `json:"is_active" gorm:"not null;default:true"`
	UpdatedAt time.Time      `json:"updated_at" gorm:"not null;autoUpdateTime"`
}

func (DataConfig) TableName() string { return "data_config" }
