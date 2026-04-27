package auth

import (
	"strings"
	"testing"
)

func TestBuildSignupEmailBody(t *testing.T) {
	body := BuildSignupEmailBody(" Athul ", " Narayanan ", " user@example.com ")

	for _, want := range []string{
		"Hi Athul Narayanan,",
		"Your account to access the database has been created.",
		"<b>Username:</b> user@example.com",
		"Please use your registered password to log in to your account.",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected body to contain %q, got %s", want, body)
		}
	}

	if strings.Contains(strings.ToLower(body), "password:") {
		t.Fatalf("signup email must not include a password field, got %s", body)
	}
}

func TestBuildSignupEmailBody_DefaultName(t *testing.T) {
	body := BuildSignupEmailBody("", "", "user@example.com")

	if !strings.Contains(body, "Hi User,") {
		t.Fatalf("expected default greeting, got %s", body)
	}
}
