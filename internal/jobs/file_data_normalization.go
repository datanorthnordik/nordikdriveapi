package jobs

import (
	"fmt"
	"log"
	f "nordik-drive-api/internal/file"

	"gorm.io/gorm"
)

const (
	defaultNormalizationJobBatchSize  = 500
	defaultNormalizationJobMaxBatches = 10
)

type FileDataNormalizationJob struct {
	DB         *gorm.DB
	Logger     *log.Logger
	BatchSize  int
	MaxBatches int
}

func NewFileDataNormalizationJob(db *gorm.DB, logger *log.Logger) *FileDataNormalizationJob {
	return &FileDataNormalizationJob{
		DB:         db,
		Logger:     logger,
		BatchSize:  defaultNormalizationJobBatchSize,
		MaxBatches: defaultNormalizationJobMaxBatches,
	}
}

func (j *FileDataNormalizationJob) Run() error {
	if j.DB == nil {
		return fmt.Errorf("db not initialized")
	}

	batchSize := j.BatchSize
	if batchSize <= 0 {
		batchSize = defaultNormalizationJobBatchSize
	}

	maxBatches := j.MaxBatches
	if maxBatches <= 0 {
		maxBatches = defaultNormalizationJobMaxBatches
	}

	total, err := f.RunNormalizationSync(j.DB, f.NormalizationSyncOptions{
		BatchSize:  batchSize,
		MaxBatches: maxBatches,
	})
	if err != nil {
		return err
	}

	if j.Logger != nil && total.Processed > 0 {
		j.Logger.Printf(
			"file data normalization cron processed=%d inserted=%d updated=%d failed=%d",
			total.Processed,
			total.Inserted,
			total.Updated,
			total.Failed,
		)
	}

	return nil
}
