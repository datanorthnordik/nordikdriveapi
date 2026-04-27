package auth

import (
	"fmt"
	"strings"
)

func BuildSignupEmailBody(firstName string, lastName string, email string) string {
	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)
	email = strings.TrimSpace(email)

	fullName := strings.TrimSpace(firstName + " " + lastName)
	if fullName == "" {
		fullName = "User"
	}

	return fmt.Sprintf(
		"<p>Hi %s,</p>"+
			"<p>Your account to access the database has been created.</p>"+
			"<p><b>Username:</b> %s</p>"+
			"<p>Please use your registered password to log in to your account.</p>"+
			"<p>Thank you.</p>",
		fullName,
		email,
	)
}

func BuildPasswordChangedEmailBody(firstName string, lastName string) string {
	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)

	fullName := strings.TrimSpace(firstName + " " + lastName)
	if fullName == "" {
		fullName = "User"
	}

	return fmt.Sprintf(
		"<p>Hi %s,</p>"+
			"<p>Your password has been changed.</p>"+
			"<p>If you did not make this change, please report it to the administrator immediately.</p>"+
			"<p>Thank you.</p>",
		fullName,
	)
}
