package jobs

import (
	"fmt"
	"log"
	filehelper "nordik-drive-api/internal/file"

	"gorm.io/gorm"
)

const defaultVersionReconciliationJobMaxRuns = 5

type FileVersionReconciliationJob struct {
	DB      *gorm.DB
	Logger  *log.Logger
	MaxJobs int
}

func NewFileVersionReconciliationJob(db *gorm.DB, logger *log.Logger) *FileVersionReconciliationJob {
	return &FileVersionReconciliationJob{
		DB:      db,
		Logger:  logger,
		MaxJobs: defaultVersionReconciliationJobMaxRuns,
	}
}

func (j *FileVersionReconciliationJob) Run() error {
	if j.DB == nil {
		return fmt.Errorf("db not initialized")
	}

	maxJobs := j.MaxJobs
	if maxJobs <= 0 {
		maxJobs = defaultVersionReconciliationJobMaxRuns
	}

	result, err := filehelper.RunVersionReconciliationJobs(j.DB, filehelper.VersionReconciliationRunOptions{
		MaxJobs: maxJobs,
	})
	if err != nil {
		return err
	}

	if j.Logger != nil && result.Claimed > 0 {
		j.Logger.Printf(
			"file version reconciliation cron claimed=%d completed=%d retried=%d failed=%d",
			result.Claimed,
			result.Completed,
			result.Retried,
			result.Failed,
		)
	}

	return nil
}
