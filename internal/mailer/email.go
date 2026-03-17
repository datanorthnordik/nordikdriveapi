package mailer

import (
	"fmt"
	"net/smtp"
	"strings"
)

type Service struct {
	From     string
	Password string
	SMTPHost string
	SMTPPort string

	// keep this overridable for tests
	sendMail func(addr string, a smtp.Auth, from string, to []string, msg []byte) error
}

func NewService(from, password, smtpHost, smtpPort string) *Service {
	return &Service{
		From:     from,
		Password: password,
		SMTPHost: smtpHost,
		SMTPPort: smtpPort,
		sendMail: smtp.SendMail,
	}
}

func (s *Service) Send(to []string, subject, body string) error {
	if len(to) == 0 {
		return fmt.Errorf("recipient list is empty")
	}
	if strings.TrimSpace(subject) == "" {
		return fmt.Errorf("subject is required")
	}
	if strings.TrimSpace(body) == "" {
		return fmt.Errorf("body is required")
	}

	auth := smtp.PlainAuth("", s.From, s.Password, s.SMTPHost)

	message := []byte(fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: %s\r\n"+
			"MIME-Version: 1.0\r\n"+
			"Content-Type: text/plain; charset=\"UTF-8\"\r\n"+
			"\r\n"+
			"%s",
		s.From,
		strings.Join(to, ", "),
		subject,
		body,
	))

	return s.sendMail(
		s.SMTPHost+":"+s.SMTPPort,
		auth,
		s.From,
		to,
		message,
	)
}

func (s *Service) SendOne(to, subject, body string) error {
	return s.Send([]string{to}, subject, body)
}
