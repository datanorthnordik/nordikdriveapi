package honour

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
	"unicode"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TextGenerator interface {
	GenerateHonourText(rowID int) (string, error)
}

type Service struct {
	DB        *gorm.DB
	Generator TextGenerator
	Now       func() time.Time
	Random    *rand.Rand
	PickRow   func(rowIDs []uint) uint
}

type TodayResponse struct {
	Available   bool   `json:"available"`
	FileID      uint   `json:"file_id,omitempty"`
	FileName    string `json:"file_name,omitempty"`
	FileVersion int    `json:"file_version,omitempty"`
	SourceRowID uint   `json:"source_row_id,omitempty"`
	CycleNumber int    `json:"cycle_number,omitempty"`
	Date        string `json:"date"`
	HonourText  string `json:"honour_text,omitempty"`
}

type candidateFile struct {
	ID       uint   `gorm:"column:id"`
	Filename string `gorm:"column:filename"`
	Version  int    `gorm:"column:version"`
	IsDelete bool   `gorm:"column:is_delete"`
}

func (candidateFile) TableName() string { return "file" }

type candidateFileData struct {
	ID      uint `gorm:"column:id"`
	FileID  uint `gorm:"column:file_id"`
	Version int  `gorm:"column:version"`
}

func (candidateFileData) TableName() string { return "file_data" }

type candidateFileVersion struct {
	ID                   uint   `gorm:"column:id"`
	FileID               uint   `gorm:"column:file_id"`
	Version              int    `gorm:"column:version"`
	ReconciliationStatus string `gorm:"column:reconciliation_status"`
}

func (candidateFileVersion) TableName() string { return "file_version" }

type candidateConfig struct {
	FileID   int64          `gorm:"column:file_id"`
	FileName string         `gorm:"column:file_name"`
	Config   datatypes.JSON `gorm:"column:config"`
	IsActive bool           `gorm:"column:is_active"`
}

func (candidateConfig) TableName() string { return "data_config" }

func NewService(db *gorm.DB, generator TextGenerator) *Service {
	return &Service{
		DB:        db,
		Generator: generator,
		Now:       time.Now,
	}
}

func (s *Service) GetTodayByFilename(filename string) (*TodayResponse, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	name := strings.TrimSpace(filename)
	if name == "" {
		return nil, fmt.Errorf("filename is required")
	}

	var file candidateFile
	if err := s.DB.Where("filename = ? AND is_delete = ?", name, false).First(&file).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, err
	}

	date := honourDayValue(s.currentTime())
	resp := &TodayResponse{
		Available: false,
		FileID:    file.ID,
		FileName:  file.Filename,
		Date:      honourDayString(date),
	}

	if !s.DB.Migrator().HasTable(&DailyHonour{}) {
		return resp, nil
	}

	var record DailyHonour
	err := s.DB.
		Where("file_id = ? AND honour_date = ? AND status = ? AND honour_text <> ''", file.ID, date, honourStatusReady).
		Order("id DESC").
		First(&record).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return resp, nil
		}
		return nil, err
	}

	resp.Available = true
	resp.FileVersion = record.FileVersion
	resp.SourceRowID = record.SourceRowID
	resp.CycleNumber = record.CycleNumber
	resp.HonourText = record.HonourText
	return resp, nil
}

func (s *Service) RunDailyHonours() error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("db not initialized")
	}
	if !s.DB.Migrator().HasTable(&DailyHonour{}) {
		return nil
	}

	files, err := s.listHonourEnabledFiles()
	if err != nil {
		return err
	}

	var runErrs []string
	for _, file := range files {
		if err := s.ensureTodayHonour(file.ID); err != nil {
			runErrs = append(runErrs, fmt.Sprintf("file_id=%d: %v", file.ID, err))
		}
	}

	if len(runErrs) == 0 {
		return nil
	}
	return errors.New(strings.Join(runErrs, "; "))
}

func (s *Service) ensureTodayHonour(fileID uint) error {
	record, needsGeneration, err := s.prepareTodayHonour(fileID)
	if err != nil || record == nil || !needsGeneration {
		return err
	}
	if s.Generator == nil {
		runErr := fmt.Errorf("honour text generator not configured")
		if markErr := s.markHonourFailure(record.ID, runErr.Error()); markErr != nil {
			return fmt.Errorf("%v: %w", runErr, markErr)
		}
		return runErr
	}

	text, genErr := s.Generator.GenerateHonourText(int(record.SourceRowID))
	if genErr != nil {
		if markErr := s.markHonourFailure(record.ID, genErr.Error()); markErr != nil {
			return fmt.Errorf("%v: %w", genErr, markErr)
		}
		return genErr
	}

	text = strings.TrimSpace(text)
	if text == "" {
		runErr := fmt.Errorf("honour text generator returned empty text")
		if markErr := s.markHonourFailure(record.ID, runErr.Error()); markErr != nil {
			return fmt.Errorf("%v: %w", runErr, markErr)
		}
		return runErr
	}

	now := s.currentTime()
	return s.DB.Model(&DailyHonour{}).
		Where("id = ?", record.ID).
		Updates(map[string]any{
			"honour_text":   text,
			"status":        honourStatusReady,
			"error_message": "",
			"generated_at":  now,
		}).Error
}

func (s *Service) prepareTodayHonour(fileID uint) (*DailyHonour, bool, error) {
	if fileID == 0 {
		return nil, false, nil
	}

	var prepared *DailyHonour
	var needsGeneration bool
	err := s.DB.Transaction(func(tx *gorm.DB) error {
		var file candidateFile
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND is_delete = ?", fileID, false).
			First(&file).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				prepared = nil
				return nil
			}
			return err
		}

		if s.hasPendingVersionTransition(tx, file.ID) {
			prepared = nil
			return nil
		}

		rowIDs, err := s.listCurrentRowIDs(tx, file.ID, file.Version)
		if err != nil {
			return err
		}
		if len(rowIDs) == 0 {
			prepared = nil
			return nil
		}

		date := honourDayValue(s.currentTime())
		var today DailyHonour
		err = tx.Where("file_id = ? AND honour_date = ?", file.ID, date).First(&today).Error
		switch {
		case err == nil:
			if strings.TrimSpace(today.HonourText) != "" && today.Status == honourStatusReady {
				prepared = &today
				return nil
			}

			if reusedText, ok, reuseErr := s.lookupReusableText(tx, file.ID, today.SourceRowID); reuseErr != nil {
				return reuseErr
			} else if ok {
				now := s.currentTime()
				if err := tx.Model(&DailyHonour{}).
					Where("id = ?", today.ID).
					Updates(map[string]any{
						"honour_text":   reusedText,
						"status":        honourStatusReady,
						"error_message": "",
						"generated_at":  now,
					}).Error; err != nil {
					return err
				}
				today.HonourText = reusedText
				today.Status = honourStatusReady
				today.GeneratedAt = &now
				prepared = &today
				return nil
			}

			if today.Status == honourStatusProcessing {
				prepared = &today
				return nil
			}

			if err := tx.Model(&DailyHonour{}).
				Where("id = ?", today.ID).
				Updates(map[string]any{
					"status":        honourStatusProcessing,
					"error_message": "",
				}).Error; err != nil {
				return err
			}
			today.Status = honourStatusProcessing
			today.ErrorMessage = ""
			prepared = &today
			needsGeneration = true
			return nil

		case errors.Is(err, gorm.ErrRecordNotFound):
			cycleNumber, sourceRowID, selectErr := s.selectSourceRowForCycle(tx, file.ID, file.Version, rowIDs)
			if selectErr != nil {
				return selectErr
			}

			record := DailyHonour{
				FileID:      file.ID,
				FileVersion: file.Version,
				SourceRowID: sourceRowID,
				HonourDate:  date,
				CycleNumber: cycleNumber,
				Status:      honourStatusProcessing,
			}

			if reusedText, ok, reuseErr := s.lookupReusableText(tx, file.ID, sourceRowID); reuseErr != nil {
				return reuseErr
			} else if ok {
				now := s.currentTime()
				record.HonourText = reusedText
				record.Status = honourStatusReady
				record.GeneratedAt = &now
			}

			if err := tx.Create(&record).Error; err != nil {
				if isUniqueConstraintError(err) {
					var existing DailyHonour
					if lookupErr := tx.Where("file_id = ? AND honour_date = ?", file.ID, date).First(&existing).Error; lookupErr != nil {
						return lookupErr
					}
					prepared = &existing
					return nil
				}
				return err
			}

			prepared = &record
			needsGeneration = record.Status == honourStatusProcessing
			return nil

		default:
			return err
		}
	})
	if err != nil {
		return nil, false, err
	}
	return prepared, needsGeneration, nil
}

func (s *Service) selectSourceRowForCycle(tx *gorm.DB, fileID uint, version int, rowIDs []uint) (int, uint, error) {
	currentCycle := 0
	if err := tx.Model(&DailyHonour{}).
		Where("file_id = ?", fileID).
		Select("COALESCE(MAX(cycle_number), 0)").
		Scan(&currentCycle).Error; err != nil {
		return 0, 0, err
	}

	if currentCycle <= 0 {
		return 1, s.pickRandomRowID(rowIDs), nil
	}

	var usedRowIDs []uint
	if err := tx.Model(&DailyHonour{}).
		Where("file_id = ? AND cycle_number = ? AND status = ? AND honour_text <> ''", fileID, currentCycle, honourStatusReady).
		Distinct("source_row_id").
		Pluck("source_row_id", &usedRowIDs).Error; err != nil {
		return 0, 0, err
	}

	usedSet := make(map[uint]struct{}, len(usedRowIDs))
	for _, rowID := range usedRowIDs {
		usedSet[rowID] = struct{}{}
	}

	if len(usedSet) < len(rowIDs) {
		candidates := make([]uint, 0, len(rowIDs)-len(usedSet))
		for _, rowID := range rowIDs {
			if _, exists := usedSet[rowID]; exists {
				continue
			}
			candidates = append(candidates, rowID)
		}
		if len(candidates) == 0 {
			return currentCycle + 1, s.pickRandomRowID(rowIDs), nil
		}
		return currentCycle, s.pickRandomRowID(candidates), nil
	}

	return currentCycle + 1, s.pickRandomRowID(rowIDs), nil
}

func (s *Service) listCurrentRowIDs(tx *gorm.DB, fileID uint, version int) ([]uint, error) {
	var rows []candidateFileData
	if err := tx.
		Where("file_id = ? AND version = ?", fileID, version).
		Order("id ASC").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]uint, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.ID)
	}
	return out, nil
}

func (s *Service) lookupReusableText(tx *gorm.DB, fileID uint, sourceRowID uint) (string, bool, error) {
	if sourceRowID == 0 {
		return "", false, nil
	}

	var previous DailyHonour
	err := tx.
		Where("file_id = ? AND source_row_id = ? AND status = ? AND honour_text <> ''", fileID, sourceRowID, honourStatusReady).
		Order("generated_at ASC").
		Order("id ASC").
		First(&previous).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}

	return strings.TrimSpace(previous.HonourText), true, nil
}

func (s *Service) markHonourFailure(recordID uint, message string) error {
	if s == nil || s.DB == nil || recordID == 0 {
		return nil
	}
	return s.DB.Model(&DailyHonour{}).
		Where("id = ?", recordID).
		Updates(map[string]any{
			"status":        honourStatusFailed,
			"error_message": strings.TrimSpace(message),
		}).Error
}

func (s *Service) hasPendingVersionTransition(tx *gorm.DB, fileID uint) bool {
	if tx == nil || fileID == 0 {
		return false
	}
	if !tx.Migrator().HasTable(&candidateFileVersion{}) {
		return false
	}

	var count int64
	if err := tx.Model(&candidateFileVersion{}).
		Where("file_id = ? AND reconciliation_status = ?", fileID, "processing").
		Count(&count).Error; err != nil {
		return false
	}
	return count > 0
}

func (s *Service) listHonourEnabledFiles() ([]candidateFile, error) {
	var configs []candidateConfig
	if err := s.DB.
		Where("is_active = ?", true).
		Order("file_id ASC").
		Find(&configs).Error; err != nil {
		return nil, err
	}

	if len(configs) == 0 {
		return nil, nil
	}

	enabledIDs := make([]uint, 0, len(configs))
	for _, cfg := range configs {
		if cfg.FileID <= 0 || !honourEnabled(cfg.Config) {
			continue
		}
		enabledIDs = append(enabledIDs, uint(cfg.FileID))
	}
	if len(enabledIDs) == 0 {
		return nil, nil
	}

	var files []candidateFile
	if err := s.DB.
		Where("id IN ? AND is_delete = ?", enabledIDs, false).
		Order("id ASC").
		Find(&files).Error; err != nil {
		return nil, err
	}
	return files, nil
}

func honourEnabled(raw datatypes.JSON) bool {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return false
	}

	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}

	if preferred, ok := preferredConfigPayload(payload); ok && containsEnabledHonourFlag(preferred) {
		return true
	}
	return containsEnabledHonourFlag(payload)
}

func preferredConfigPayload(value any) (any, bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if normalizeConfigKey(key) != "source_file" {
				continue
			}
			sourceMap, ok := child.(map[string]any)
			if !ok {
				continue
			}
			for childKey, childValue := range sourceMap {
				switch normalizeConfigKey(childKey) {
				case "data_config", "config":
					return childValue, true
				}
			}
			if _, ok := sourceMap["fields"]; ok {
				return sourceMap, true
			}
		}
		for _, child := range v {
			if candidate, ok := preferredConfigPayload(child); ok {
				return candidate, true
			}
		}
	case []any:
		for _, item := range v {
			if candidate, ok := preferredConfigPayload(item); ok {
				return candidate, true
			}
		}
	}

	return nil, false
}

func containsEnabledHonourFlag(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		for key, raw := range v {
			switch normalizeConfigKey(key) {
			case "honour", "honor", "enablehonour", "enablehonor", "honourenabled", "honorenabled":
				if parseBoolish(raw) {
					return true
				}
			}
			if containsEnabledHonourFlag(raw) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if containsEnabledHonourFlag(item) {
				return true
			}
		}
	}

	return false
}

func parseBoolish(raw any) bool {
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		v = strings.TrimSpace(strings.ToLower(v))
		return v == "1" || v == "true" || v == "yes" || v == "on"
	case float64:
		return v != 0
	case int:
		return v != 0
	default:
		return false
	}
}

func normalizeConfigKey(value string) string {
	var builder strings.Builder
	for _, r := range strings.TrimSpace(strings.ToLower(value)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func honourDayValue(now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now()
	}
	loc := now.Location()
	if loc == nil {
		loc = time.Local
	}
	year, month, day := now.In(loc).Date()
	return time.Date(year, month, day, 0, 0, 0, 0, loc)
}

func honourDayString(date time.Time) string {
	return honourDayValue(date).Format("2006-01-02")
}

func (s *Service) currentTime() time.Time {
	if s != nil && s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) pickRandomRowID(rowIDs []uint) uint {
	if len(rowIDs) == 0 {
		return 0
	}
	if s != nil && s.PickRow != nil {
		if chosen := s.PickRow(rowIDs); chosen != 0 {
			return chosen
		}
	}
	if s != nil && s.Random != nil {
		return rowIDs[s.Random.Intn(len(rowIDs))]
	}
	r := rand.New(rand.NewSource(s.currentTime().UnixNano()))
	return rowIDs[r.Intn(len(rowIDs))]
}

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") || strings.Contains(msg, "duplicate key")
}
