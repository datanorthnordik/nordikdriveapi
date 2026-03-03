package dataconfig

import (
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

type DataConfigService struct {
	DB *gorm.DB
}

type GetConfigResult struct {
	NotModified bool
	Config      *DataConfig
}

// GetByFileNameIfModified:
// - Finds active config by file_name (case-insensitive), latest updated_at.
// - If clientLastModified is present and config not newer => NotModified=true.
func (s *DataConfigService) GetByFileNameIfModified(fileName string, clientLastModified *time.Time) (*GetConfigResult, error) {
	name := strings.TrimSpace(fileName)
	if name == "" {
		return nil, errors.New("file_name is required")
	}

	var cfg DataConfig
	err := s.DB.
		Where("is_active = ?", true).
		Where("lower(file_name) = lower(?)", name).
		Order("updated_at DESC").
		Order("id DESC").
		Take(&cfg).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, err
	}

	// Keep exact comparison. Frontend is sending RFC3339Nano already.
	if clientLastModified != nil {
		dbTime := cfg.UpdatedAt.UTC()
		clientTime := clientLastModified.UTC()

		// not modified when DB updated_at <= client timestamp
		if !dbTime.After(clientTime) {
			return &GetConfigResult{NotModified: true, Config: &cfg}, nil
		}
	}

	return &GetConfigResult{NotModified: false, Config: &cfg}, nil
}
