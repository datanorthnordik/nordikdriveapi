package mailer

type EmailSender interface {
	SendOne(to, subject, body string) error
}

type Attachment struct {
	Filename    string
	ContentType string
	Data        []byte
}
