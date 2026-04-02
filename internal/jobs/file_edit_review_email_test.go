package jobs

import (
	"errors"
	"io"
	"log"
	"regexp"
	"testing"

	filehelper "nordik-drive-api/internal/file"
	"nordik-drive-api/internal/mailer"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type fileJobMockEmailCall struct {
	To      string
	Subject string
	Body    string
}

type fileJobMockEmailSender struct {
	sendErr error
	calls   []fileJobMockEmailCall
}

func (m *fileJobMockEmailSender) Send(to []string, subject, body string) error {
	if len(to) == 0 {
		return nil
	}
	return m.SendOne(to[0], subject, body)
}

func (m *fileJobMockEmailSender) SendOne(to, subject, body string) error {
	m.calls = append(m.calls, fileJobMockEmailCall{
		To:      to,
		Subject: subject,
		Body:    body,
	})
	return m.sendErr
}

var _ mailer.EmailSender = (*fileJobMockEmailSender)(nil)

func newFileJobMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 sqlDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		t.Fatalf("failed to open gorm db: %v", err)
	}

	return db, mock
}

func fileEditSelectQueryRegex() string {
	return `(?s)SELECT\s+fer\.request_id,\s+fer\.user_id,\s+fer\.status,\s+fer\.firstname,\s+fer\.lastname,\s+fer\.reviewer_comment,\s+u\.email,\s+TRIM\(COALESCE\(u\.firstname, ''\) \|\| ' ' \|\| COALESCE\(u\.lastname, ''\)\) AS created_user_name\s+FROM file_edit_request fer JOIN users u ON u\.id = fer\.user_id WHERE fer\.status <> \$1 AND fer\.review_email_trigger_success = \$2 ORDER BY fer\.created_at ASC LIMIT \$3`
}

func fileEditUpdateQueryRegex() string {
	return regexp.QuoteMeta(`UPDATE "file_edit_request" SET "review_email_trigger_success"=$1 WHERE request_id = $2`)
}

func TestNewFileEditReviewEmailJob(t *testing.T) {
	db, _ := newFileJobMockDB(t)
	mailerSvc := &fileJobMockEmailSender{}
	logger := log.New(io.Discard, "", 0)

	job := NewFileEditReviewEmailJob(db, mailerSvc, logger)

	if job == nil {
		t.Fatal("expected non-nil job")
	}
	if job.DB != db {
		t.Fatal("expected DB to be assigned")
	}
	if job.Mailer != mailerSvc {
		t.Fatal("expected Mailer to be assigned")
	}
	if job.Logger != logger {
		t.Fatal("expected Logger to be assigned")
	}
	if job.BatchSize != 100 {
		t.Fatalf("expected BatchSize=100, got %d", job.BatchSize)
	}
}

func TestFileEditReviewEmailJob_Run_FetchError(t *testing.T) {
	db, mock := newFileJobMockDB(t)

	mock.
		ExpectQuery(fileEditSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnError(errors.New("db failed"))

	job := &FileEditReviewEmailJob{
		DB:        db,
		Mailer:    &fileJobMockEmailSender{},
		Logger:    log.New(io.Discard, "", 0),
		BatchSize: 100,
	}

	err := job.Run()
	if err == nil {
		t.Fatal("expected fetch error")
	}
	if err.Error() != "failed to fetch file edit requests pending review emails: db failed" {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestFileEditReviewEmailJob_Run_EmptyEmailSkipped(t *testing.T) {
	db, mock := newFileJobMockDB(t)
	mailerSvc := &fileJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"request_id",
		"user_id",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"email",
		"created_user_name",
	}).AddRow(
		101, 1, "approved", "John", "Doe", "Looks good", "", "Athul Narayanan",
	)

	mock.
		ExpectQuery(fileEditSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	job := &FileEditReviewEmailJob{
		DB:        db,
		Mailer:    mailerSvc,
		Logger:    log.New(io.Discard, "", 0),
		BatchSize: 100,
	}

	if err := job.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mailerSvc.calls) != 0 {
		t.Fatalf("expected no emails sent, got %d", len(mailerSvc.calls))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestFileEditReviewEmailJob_Run_SendFailure_DoesNotUpdateFlag(t *testing.T) {
	db, mock := newFileJobMockDB(t)
	mailerSvc := &fileJobMockEmailSender{sendErr: errors.New("smtp failed")}

	rows := sqlmock.NewRows([]string{
		"request_id",
		"user_id",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"email",
		"created_user_name",
	}).AddRow(
		102, 1, "approved", "John", "Doe", "Looks good", "athul@example.com", "Athul Narayanan",
	)

	mock.
		ExpectQuery(fileEditSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	job := &FileEditReviewEmailJob{
		DB:        db,
		Mailer:    mailerSvc,
		Logger:    log.New(io.Discard, "", 0),
		BatchSize: 100,
	}

	if err := job.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mailerSvc.calls) != 1 {
		t.Fatalf("expected 1 email attempt, got %d", len(mailerSvc.calls))
	}

	call := mailerSvc.calls[0]
	if call.To != "athul@example.com" {
		t.Fatalf("unexpected recipient: %s", call.To)
	}
	if call.Subject != "Update to your submission" {
		t.Fatalf("unexpected subject: %s", call.Subject)
	}

	expectedBody := filehelper.BuildFileEditRequestReviewEmailBody(
		"Athul Narayanan",
		"approved",
		"John",
		"Doe",
		"Looks good",
	)
	if call.Body != expectedBody {
		t.Fatalf("unexpected body\nexpected: %q\ngot:      %q", expectedBody, call.Body)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestFileEditReviewEmailJob_Run_Success_UpdatesFlag(t *testing.T) {
	db, mock := newFileJobMockDB(t)
	mailerSvc := &fileJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"request_id",
		"user_id",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"email",
		"created_user_name",
	}).AddRow(
		103, 1, "approved", "John", "Doe", "Looks good", "  athul@example.com  ", "Athul Narayanan",
	)

	mock.
		ExpectQuery(fileEditSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	mock.
		ExpectExec(fileEditUpdateQueryRegex()).
		WithArgs(true, 103).
		WillReturnResult(sqlmock.NewResult(0, 1))

	job := &FileEditReviewEmailJob{
		DB:        db,
		Mailer:    mailerSvc,
		Logger:    log.New(io.Discard, "", 0),
		BatchSize: 100,
	}

	if err := job.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mailerSvc.calls) != 1 {
		t.Fatalf("expected 1 email send, got %d", len(mailerSvc.calls))
	}

	call := mailerSvc.calls[0]
	if call.To != "athul@example.com" {
		t.Fatalf("expected trimmed email recipient, got %q", call.To)
	}

	expectedBody := filehelper.BuildFileEditRequestReviewEmailBody(
		"Athul Narayanan",
		"approved",
		"John",
		"Doe",
		"Looks good",
	)
	if call.Body != expectedBody {
		t.Fatalf("unexpected body\nexpected: %q\ngot:      %q", expectedBody, call.Body)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestFileEditReviewEmailJob_Run_UpdateFlagFailure_Continues(t *testing.T) {
	db, mock := newFileJobMockDB(t)
	mailerSvc := &fileJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"request_id",
		"user_id",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"email",
		"created_user_name",
	}).
		AddRow(104, 1, "approved", "John", "Doe", "First", "one@example.com", "Athul One").
		AddRow(105, 2, "rejected", "Jane", "Smith", "Second", "two@example.com", "Athul Two")

	mock.
		ExpectQuery(fileEditSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	mock.
		ExpectExec(fileEditUpdateQueryRegex()).
		WithArgs(true, 104).
		WillReturnError(errors.New("update failed"))

	mock.
		ExpectExec(fileEditUpdateQueryRegex()).
		WithArgs(true, 105).
		WillReturnResult(sqlmock.NewResult(0, 1))

	job := &FileEditReviewEmailJob{
		DB:        db,
		Mailer:    mailerSvc,
		Logger:    log.New(io.Discard, "", 0),
		BatchSize: 100,
	}

	if err := job.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mailerSvc.calls) != 2 {
		t.Fatalf("expected 2 email sends, got %d", len(mailerSvc.calls))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestFileEditReviewEmailJob_Run_BatchSizeLimit(t *testing.T) {
	db, mock := newFileJobMockDB(t)
	mailerSvc := &fileJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"request_id",
		"user_id",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"email",
		"created_user_name",
	}).AddRow(
		201, 1, "approved", "John", "Doe", "First", "one@example.com", "Athul One",
	)

	mock.
		ExpectQuery(fileEditSelectQueryRegex()).
		WithArgs("pending", false, 1).
		WillReturnRows(rows)

	mock.
		ExpectExec(fileEditUpdateQueryRegex()).
		WithArgs(true, 201).
		WillReturnResult(sqlmock.NewResult(0, 1))

	job := &FileEditReviewEmailJob{
		DB:        db,
		Mailer:    mailerSvc,
		Logger:    log.New(io.Discard, "", 0),
		BatchSize: 1,
	}

	if err := job.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mailerSvc.calls) != 1 {
		t.Fatalf("expected 1 email because batch size is 1, got %d", len(mailerSvc.calls))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
