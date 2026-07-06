package file

import (
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const (
	fileEditRequestStatusCompleted = "completed"
	formSubmissionStatusRejected   = "rejected"
)

var ErrFileOperationBlocked = errors.New("file operation blocked by open requests")

type FileOperationBlockedError struct {
	Operation            string
	Filename             string
	FileEditRequestCount int64
	FormSubmissionCount  int64
}

func (e *FileOperationBlockedError) Error() string {
	blockers := make([]string, 0, 2)
	if e.FileEditRequestCount > 0 {
		blockers = append(
			blockers,
			fmt.Sprintf("%d open file edit request%s", e.FileEditRequestCount, pluralSuffix(e.FileEditRequestCount)),
		)
	}
	if e.FormSubmissionCount > 0 {
		blockers = append(
			blockers,
			fmt.Sprintf("%d open form submission request%s", e.FormSubmissionCount, pluralSuffix(e.FormSubmissionCount)),
		)
	}

	operation := strings.TrimSpace(e.Operation)
	if operation == "" {
		operation = "modify"
	}

	filename := strings.TrimSpace(e.Filename)
	if filename == "" {
		filename = "this file"
	} else {
		filename = fmt.Sprintf("file %q", filename)
	}

	return fmt.Sprintf(
		"cannot %s %s while %s exist for this file",
		operation,
		filename,
		strings.Join(blockers, " and "),
	)
}

func (e *FileOperationBlockedError) Is(target error) bool {
	return target == ErrFileOperationBlocked
}

func pluralSuffix(count int64) string {
	if count == 1 {
		return ""
	}
	return "s"
}

func (fs *FileService) ensureNoOpenRequestsForFile(file File, operation string) error {
	return ensureNoOpenRequestsForFileWithDB(fs.DB, file, operation)
}

func ensureNoOpenRequestsForFileWithDB(db *gorm.DB, file File, operation string) error {
	var fileEditRequestCount int64
	if err := db.
		Table((&FileEditRequest{}).TableName()).
		Where("file_id = ? AND LOWER(TRIM(CAST(status AS TEXT))) <> ?", file.ID, fileEditRequestStatusCompleted).
		Count(&fileEditRequestCount).Error; err != nil {
		return err
	}

	var formSubmissionCount int64
	if err := db.
		Table("form_submissions").
		Where("file_id = ? AND LOWER(TRIM(CAST(status AS TEXT))) <> ?", file.ID, formSubmissionStatusRejected).
		Count(&formSubmissionCount).Error; err != nil {
		return err
	}

	if fileEditRequestCount == 0 && formSubmissionCount == 0 {
		return nil
	}

	return &FileOperationBlockedError{
		Operation:            operation,
		Filename:             file.Filename,
		FileEditRequestCount: fileEditRequestCount,
		FormSubmissionCount:  formSubmissionCount,
	}
}
