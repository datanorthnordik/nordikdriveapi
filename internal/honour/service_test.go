package honour

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var honourTestDBSeq uint64

type testFile struct {
	ID       uint   `gorm:"primaryKey"`
	Filename string `gorm:"unique"`
	Version  int
	IsDelete bool
}

func (testFile) TableName() string { return "file" }

type testFileData struct {
	ID      uint           `gorm:"primaryKey"`
	FileID  uint           `gorm:"index"`
	RowData datatypes.JSON `gorm:"type:jsonb"`
	Version int
}

func (testFileData) TableName() string { return "file_data" }

type testFileVersion struct {
	ID                   uint `gorm:"primaryKey"`
	FileID               uint `gorm:"index"`
	Version              int
	ReconciliationStatus string
}

func (testFileVersion) TableName() string { return "file_version" }

type testConfig struct {
	ID       int64 `gorm:"primaryKey"`
	FileID   int64 `gorm:"uniqueIndex"`
	FileName string
	Config   datatypes.JSON `gorm:"type:jsonb"`
	IsActive bool
}

func (testConfig) TableName() string { return "data_config" }

type generatorStub struct {
	texts map[int]string
	err   error
	calls []int
}

func (g *generatorStub) GenerateHonourText(rowID int) (string, error) {
	g.calls = append(g.calls, rowID)
	if g.err != nil {
		return "", g.err
	}
	if text, ok := g.texts[rowID]; ok {
		return text, nil
	}
	return fmt.Sprintf("honour-%d", rowID), nil
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	id := atomic.AddUint64(&honourTestDBSeq, 1)
	dsn := fmt.Sprintf("file:honour_test_%d?mode=memory&cache=shared", id)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.AutoMigrate(&testFile{}, &testFileData{}, &testFileVersion{}, &testConfig{}, &DailyHonour{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func seedHonourEnabledFile(t *testing.T, db *gorm.DB, id uint, version int, rowCount int) {
	t.Helper()
	file := testFile{ID: id, Filename: fmt.Sprintf("survivors-%d.csv", id), Version: version}
	if err := db.Create(&file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := db.Create(&testFileVersion{FileID: id, Version: version, ReconciliationStatus: "ready"}).Error; err != nil {
		t.Fatalf("create file version: %v", err)
	}
	for i := 0; i < rowCount; i++ {
		row := testFileData{FileID: id, Version: version, RowData: datatypes.JSON([]byte(fmt.Sprintf(`{"Name":"Person %d"}`, i+1)))}
		if err := db.Create(&row).Error; err != nil {
			t.Fatalf("create row: %v", err)
		}
	}
	config := testConfig{
		FileID:   int64(id),
		FileName: file.Filename,
		Config:   datatypes.JSON([]byte(`{"source_file":{"data_config":{"honour":true}}}`)),
		IsActive: true,
	}
	if err := db.Create(&config).Error; err != nil {
		t.Fatalf("create config: %v", err)
	}
}

func TestServiceRunDailyHonoursGeneratesOncePerDay(t *testing.T) {
	db := newTestDB(t)
	seedHonourEnabledFile(t, db, 1, 1, 2)
	gen := &generatorStub{texts: map[int]string{1: "Today honour"}}
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.FixedZone("T", -4*60*60))

	svc := &Service{
		DB:        db,
		Generator: gen,
		Now:       func() time.Time { return now },
	}

	if err := svc.RunDailyHonours(); err != nil {
		t.Fatalf("run honours: %v", err)
	}
	if err := svc.RunDailyHonours(); err != nil {
		t.Fatalf("second run honours: %v", err)
	}

	var honours []DailyHonour
	if err := db.Order("id ASC").Find(&honours).Error; err != nil {
		t.Fatalf("load honours: %v", err)
	}
	if len(honours) != 1 {
		t.Fatalf("expected 1 honour row, got %d", len(honours))
	}
	if honours[0].Status != honourStatusReady || honours[0].HonourText == "" {
		t.Fatalf("unexpected honour row: %#v", honours[0])
	}
	if len(gen.calls) != 1 {
		t.Fatalf("expected generator once, got %v", gen.calls)
	}
}

func TestServiceRunDailyHonoursReusesExistingTextAcrossCycles(t *testing.T) {
	db := newTestDB(t)
	seedHonourEnabledFile(t, db, 1, 1, 2)
	gen := &generatorStub{texts: map[int]string{1: "First honour"}}
	dayOne := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.FixedZone("T", -4*60*60))
	var rows []testFileData
	if err := db.Order("id ASC").Find(&rows).Error; err != nil {
		t.Fatalf("load rows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	firstRowID := rows[0].ID
	secondRowID := rows[1].ID
	picks := []uint{firstRowID, secondRowID, firstRowID}

	svc := &Service{
		DB:        db,
		Generator: gen,
		Now:       func() time.Time { return dayOne },
		PickRow: func(rowIDs []uint) uint {
			if len(picks) == 0 {
				return rowIDs[0]
			}
			pick := picks[0]
			picks = picks[1:]
			for _, rowID := range rowIDs {
				if rowID == pick {
					return pick
				}
			}
			return rowIDs[0]
		},
	}

	if err := svc.RunDailyHonours(); err != nil {
		t.Fatalf("day one: %v", err)
	}

	var first DailyHonour
	if err := db.First(&first).Error; err != nil {
		t.Fatalf("load first honour: %v", err)
	}

	otherRowID := secondRowID
	if first.SourceRowID == secondRowID {
		otherRowID = firstRowID
	}
	gen.texts[int(otherRowID)] = "Second honour"

	svc.Now = func() time.Time { return dayOne.AddDate(0, 0, 1) }
	if err := svc.RunDailyHonours(); err != nil {
		t.Fatalf("day two: %v", err)
	}

	svc.Now = func() time.Time { return dayOne.AddDate(0, 0, 2) }
	if err := svc.RunDailyHonours(); err != nil {
		t.Fatalf("day three: %v", err)
	}

	var honours []DailyHonour
	if err := db.Order("id ASC").Find(&honours).Error; err != nil {
		t.Fatalf("load honours: %v", err)
	}
	if len(honours) != 3 {
		t.Fatalf("expected 3 honour rows, got %d", len(honours))
	}
	if honours[0].CycleNumber != 1 || honours[1].CycleNumber != 1 || honours[2].CycleNumber != 2 {
		t.Fatalf("unexpected cycle numbers: %#v", honours)
	}
	if honours[2].SourceRowID != honours[0].SourceRowID {
		t.Fatalf("expected cycle two to reuse first row, got %#v", honours)
	}
	if honours[2].HonourText != honours[0].HonourText {
		t.Fatalf("expected reused honour text, got %#v", honours)
	}
	if len(gen.calls) != 2 {
		t.Fatalf("expected generator to run only for unique rows, got %v", gen.calls)
	}
}

func TestServiceRunDailyHonoursSkipsFilesWithPendingTransitions(t *testing.T) {
	db := newTestDB(t)
	seedHonourEnabledFile(t, db, 1, 1, 1)
	if err := db.Create(&testFileVersion{FileID: 1, Version: 2, ReconciliationStatus: "processing"}).Error; err != nil {
		t.Fatalf("create processing version: %v", err)
	}
	gen := &generatorStub{texts: map[int]string{1: "Today honour"}}
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	svc := &Service{DB: db, Generator: gen, Now: func() time.Time { return now }}

	if err := svc.RunDailyHonours(); err != nil {
		t.Fatalf("run honours: %v", err)
	}

	var count int64
	if err := db.Model(&DailyHonour{}).Count(&count).Error; err != nil {
		t.Fatalf("count honours: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no honours while processing exists, got %d", count)
	}
	if len(gen.calls) != 0 {
		t.Fatalf("expected no generator calls, got %v", gen.calls)
	}
}

func TestServiceRunDailyHonoursMarksFailures(t *testing.T) {
	db := newTestDB(t)
	seedHonourEnabledFile(t, db, 1, 1, 1)
	gen := &generatorStub{err: errors.New("nia failed")}
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	svc := &Service{DB: db, Generator: gen, Now: func() time.Time { return now }}

	if err := svc.RunDailyHonours(); err == nil || !strings.Contains(err.Error(), "nia failed") {
		t.Fatalf("expected generation error, got %v", err)
	}

	var honour DailyHonour
	if err := db.First(&honour).Error; err != nil {
		t.Fatalf("load honour: %v", err)
	}
	if honour.Status != honourStatusFailed || !strings.Contains(honour.ErrorMessage, "nia failed") {
		t.Fatalf("unexpected failed honour: %#v", honour)
	}
}

func TestServiceGetTodayByFilename(t *testing.T) {
	db := newTestDB(t)
	seedHonourEnabledFile(t, db, 1, 2, 1)
	now := time.Date(2026, time.July, 14, 8, 0, 0, 0, time.UTC)
	date := honourDayValue(now)
	honour := DailyHonour{
		FileID:      1,
		FileVersion: 2,
		SourceRowID: 1,
		HonourDate:  date,
		CycleNumber: 1,
		HonourText:  "Remembered today",
		Status:      honourStatusReady,
	}
	if err := db.Create(&honour).Error; err != nil {
		t.Fatalf("create honour: %v", err)
	}

	svc := &Service{DB: db, Now: func() time.Time { return now }}
	resp, err := svc.GetTodayByFilename("survivors-1.csv")
	if err != nil {
		t.Fatalf("get today honour: %v", err)
	}
	if !resp.Available || resp.HonourText != "Remembered today" || resp.Date != "2026-07-14" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestServiceGetTodayByFilenameUsesConfiguredLocation(t *testing.T) {
	db := newTestDB(t)
	seedHonourEnabledFile(t, db, 1, 2, 1)

	toronto := time.FixedZone("EDT", -4*60*60)
	nowUTC := time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)
	localDate := honourDayValue(nowUTC.In(toronto))

	record := DailyHonour{
		FileID:      1,
		FileVersion: 2,
		SourceRowID: 1,
		HonourDate:  localDate,
		CycleNumber: 1,
		HonourText:  "Toronto day honour",
		Status:      honourStatusReady,
	}
	if err := db.Create(&record).Error; err != nil {
		t.Fatalf("create honour: %v", err)
	}

	svc := &Service{
		DB:       db,
		Now:      func() time.Time { return nowUTC },
		Location: toronto,
	}
	resp, err := svc.GetTodayByFilename("survivors-1.csv")
	if err != nil {
		t.Fatalf("get today honour: %v", err)
	}
	if !resp.Available || resp.HonourText != "Toronto day honour" || resp.Date != "2026-07-15" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}
