package jobs

import (
	"fmt"
	"log"
	"nordik-drive-api/internal/mailer"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type Scheduler struct {
	cron *cron.Cron
}

func NewScheduler(
	db *gorm.DB,
	mailerSvc mailer.EmailSender,
	logger *log.Logger,
) (*Scheduler, error) {
	c := cron.New()

	fileEditReviewEmailJob := NewFileEditReviewEmailJob(db, mailerSvc, logger)

	// every 5 minutes
	_, err := c.AddFunc("*/5 * * * *", func() {
		if err := fileEditReviewEmailJob.Run(); err != nil && logger != nil {
			logger.Printf("file edit review email cron failed: %v", err)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register file edit review email cron: %w", err)
	}

	formSubmissionReviewEmailJob := NewFormSubmissionReviewEmailJob(db, mailerSvc, logger)

	_, err = c.AddFunc("*/5 * * * *", func() {
		if err := formSubmissionReviewEmailJob.Run(); err != nil && logger != nil {
			logger.Printf("form submission review email cron failed: %v", err)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register form submission review email cron: %w", err)
	}

	fileDataNormalizationJob := NewFileDataNormalizationJob(db, logger)

	_, err = c.AddFunc("*/10 * * * *", func() {
		if err := fileDataNormalizationJob.Run(); err != nil && logger != nil {
			logger.Printf("file data normalization cron failed: %v", err)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register file data normalization cron: %w", err)
	}

	return &Scheduler{cron: c}, nil
}

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}
