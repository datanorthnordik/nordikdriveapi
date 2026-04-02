package jobs

import (
	"errors"
	"io"
	"log"
	"regexp"
	"testing"

	"nordik-drive-api/internal/formsubmission"
	"nordik-drive-api/internal/mailer"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type formJobMockEmailCall struct {
	To      string
	Subject string
	Body    string
}

type formJobMockEmailSender struct {
	sendErr error
	calls   []formJobMockEmailCall
}

func (m *formJobMockEmailSender) Send(to []string, subject, body string) error {
	if len(to) == 0 {
		return nil
	}
	return m.SendOne(to[0], subject, body)
}

func (m *formJobMockEmailSender) SendOne(to, subject, body string) error {
	m.calls = append(m.calls, formJobMockEmailCall{
		To:      to,
		Subject: subject,
		Body:    body,
	})
	return m.sendErr
}

var _ mailer.EmailSender = (*formJobMockEmailSender)(nil)

func newFormJobMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
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

func formSubmissionSelectQueryRegex() string {
	return `(?s)SELECT\s+form_submissions\.id,\s+form_submissions\.form_label,\s+form_submissions\.status,\s+form_submissions\.firstname,\s+form_submissions\.lastname,\s+form_submissions\.reviewer_comment,\s+u\.email AS created_user_email,\s+TRIM\(COALESCE\(u\.firstname, ''\) \|\| ' ' \|\| COALESCE\(u\.lastname, ''\)\) AS created_user_name\s+FROM "form_submissions" JOIN users u ON u\.id = form_submissions\.created_by WHERE form_submissions\.status <> \$1 AND form_submissions\.review_email_trigger_success = \$2 ORDER BY form_submissions\.created_at ASC LIMIT \$3`
}

func formSubmissionUpdateQueryRegex() string {
	return regexp.QuoteMeta(`UPDATE "form_submissions" SET "review_email_trigger_success"=$1,"updated_at"=$2 WHERE id = $3`)
}

func TestNewFormSubmissionReviewEmailJob(t *testing.T) {
	db, _ := newFormJobMockDB(t)
	mailerSvc := &formJobMockEmailSender{}
	logger := log.New(io.Discard, "", 0)

	job := NewFormSubmissionReviewEmailJob(db, mailerSvc, logger)

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

func TestFormSubmissionReviewEmailJob_Run_FetchError(t *testing.T) {
	db, mock := newFormJobMockDB(t)

	mock.
		ExpectQuery(formSubmissionSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnError(errors.New("db failed"))

	job := &FormSubmissionReviewEmailJob{
		DB:        db,
		Mailer:    &formJobMockEmailSender{},
		Logger:    log.New(io.Discard, "", 0),
		BatchSize: 100,
	}

	err := job.Run()
	if err == nil {
		t.Fatal("expected fetch error")
	}
	if err.Error() != "failed to fetch form submissions pending review emails: db failed" {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestFormSubmissionReviewEmailJob_Run_EmptyEmailSkipped(t *testing.T) {
	db, mock := newFormJobMockDB(t)
	mailerSvc := &formJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"id",
		"form_label",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"created_user_email",
		"created_user_name",
	}).AddRow(
		101, "Passport", formsubmission.ReviewStatusApproved, "John", "Doe", "Looks good", "", "Athul Narayanan",
	)

	mock.
		ExpectQuery(formSubmissionSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	job := &FormSubmissionReviewEmailJob{
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

func TestFormSubmissionReviewEmailJob_Run_SendFailure_DoesNotUpdateFlag(t *testing.T) {
	db, mock := newFormJobMockDB(t)
	mailerSvc := &formJobMockEmailSender{sendErr: errors.New("smtp failed")}

	rows := sqlmock.NewRows([]string{
		"id",
		"form_label",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"created_user_email",
		"created_user_name",
	}).AddRow(
		102, "Driver License", formsubmission.ReviewStatusApproved, "John", "Doe", "Looks good", "athul@example.com", "Athul Narayanan",
	)

	mock.
		ExpectQuery(formSubmissionSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	job := &FormSubmissionReviewEmailJob{
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
	if call.Subject != "Update to your submission for Driver License" {
		t.Fatalf("unexpected subject: %s", call.Subject)
	}

	expectedBody := formsubmission.BuildFormSubmissionReviewEmailBody(
		"Athul Narayanan",
		"Driver License",
		formsubmission.ReviewStatusApproved,
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

func TestFormSubmissionReviewEmailJob_Run_Success_UpdatesFlag(t *testing.T) {
	db, mock := newFormJobMockDB(t)
	mailerSvc := &formJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"id",
		"form_label",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"created_user_email",
		"created_user_name",
	}).AddRow(
		103, "Health Card", formsubmission.ReviewStatusRejected, "Jane", "Smith", "Missing document", "  athul@example.com  ", "Athul Narayanan",
	)

	mock.
		ExpectQuery(formSubmissionSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	mock.
		ExpectExec(formSubmissionUpdateQueryRegex()).
		WithArgs(true, sqlmock.AnyArg(), int64(103)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	job := &FormSubmissionReviewEmailJob{
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
	if call.Subject != "Update to your submission for Health Card" {
		t.Fatalf("unexpected subject: %s", call.Subject)
	}

	expectedBody := formsubmission.BuildFormSubmissionReviewEmailBody(
		"Athul Narayanan",
		"Health Card",
		formsubmission.ReviewStatusRejected,
		"Jane",
		"Smith",
		"Missing document",
	)
	if call.Body != expectedBody {
		t.Fatalf("unexpected body\nexpected: %q\ngot:      %q", expectedBody, call.Body)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestFormSubmissionReviewEmailJob_Run_UpdateFlagFailure_Continues(t *testing.T) {
	db, mock := newFormJobMockDB(t)
	mailerSvc := &formJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"id",
		"form_label",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"created_user_email",
		"created_user_name",
	}).
		AddRow(104, "SIN", formsubmission.ReviewStatusApproved, "John", "Doe", "First", "one@example.com", "Athul One").
		AddRow(105, "PR Card", formsubmission.ReviewStatusRejected, "Jane", "Smith", "Second", "two@example.com", "Athul Two")

	mock.
		ExpectQuery(formSubmissionSelectQueryRegex()).
		WithArgs("pending", false, 100).
		WillReturnRows(rows)

	mock.
		ExpectExec(formSubmissionUpdateQueryRegex()).
		WithArgs(true, sqlmock.AnyArg(), int64(104)).
		WillReturnError(errors.New("update failed"))

	mock.
		ExpectExec(formSubmissionUpdateQueryRegex()).
		WithArgs(true, sqlmock.AnyArg(), int64(105)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	job := &FormSubmissionReviewEmailJob{
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

func TestFormSubmissionReviewEmailJob_Run_BatchSizeLimit(t *testing.T) {
	db, mock := newFormJobMockDB(t)
	mailerSvc := &formJobMockEmailSender{}

	rows := sqlmock.NewRows([]string{
		"id",
		"form_label",
		"status",
		"firstname",
		"lastname",
		"reviewer_comment",
		"created_user_email",
		"created_user_name",
	}).AddRow(
		201, "Passport", formsubmission.ReviewStatusApproved, "John", "Doe", "First", "one@example.com", "Athul One",
	)

	mock.
		ExpectQuery(formSubmissionSelectQueryRegex()).
		WithArgs("pending", false, 1).
		WillReturnRows(rows)

	mock.
		ExpectExec(formSubmissionUpdateQueryRegex()).
		WithArgs(true, sqlmock.AnyArg(), int64(201)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	job := &FormSubmissionReviewEmailJob{
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
