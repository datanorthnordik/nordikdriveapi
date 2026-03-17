package mailer

type EmailSender interface {
	SendOne(to, subject, body string) error
}
