package config

import (
	"os"
	"testing"
)

func TestLoadConfig_ReadsEnvVars(t *testing.T) {
	env := map[string]string{
		"DB_HOST":            "localhost",
		"DB_PORT":            "5432",
		"DB_USER":            "user1",
		"DB_PASSWORD":        "pass1",
		"DB_NAME":            "db1",
		"JWT_SECRET":         "secret",
		"GEMINI_KEY":         "gem-key",
		"GMAIL_USER":         "mail@test.com",
		"GMAIL_APP_PASSWORD": "app-pass",
	}

	for k, v := range env {
		os.Setenv(k, v)
		t.Cleanup(func(key string) func() {
			return func() { os.Unsetenv(key) }
		}(k))
	}

	cfg := LoadConfig()

	if cfg.DBHost != env["DB_HOST"] {
		t.Fatalf("DBHost=%q want %q", cfg.DBHost, env["DB_HOST"])
	}
	if cfg.DBPort != env["DB_PORT"] {
		t.Fatalf("DBPort=%q want %q", cfg.DBPort, env["DB_PORT"])
	}
	if cfg.DBUser != env["DB_USER"] {
		t.Fatalf("DBUser=%q want %q", cfg.DBUser, env["DB_USER"])
	}
	if cfg.DBPassword != env["DB_PASSWORD"] {
		t.Fatalf("DBPassword=%q want %q", cfg.DBPassword, env["DB_PASSWORD"])
	}
	if cfg.DBName != env["DB_NAME"] {
		t.Fatalf("DBName=%q want %q", cfg.DBName, env["DB_NAME"])
	}
	if cfg.JWTSecret != env["JWT_SECRET"] {
		t.Fatalf("JWTSecret=%q want %q", cfg.JWTSecret, env["JWT_SECRET"])
	}
	if cfg.GeminiKey != env["GEMINI_KEY"] {
		t.Fatalf("GeminiKey=%q want %q", cfg.GeminiKey, env["GEMINI_KEY"])
	}
	if cfg.GmailUser != env["GMAIL_USER"] {
		t.Fatalf("GmailUser=%q want %q", cfg.GmailUser, env["GMAIL_USER"])
	}
	if cfg.GmailPass != env["GMAIL_APP_PASSWORD"] {
		t.Fatalf("GmailPass=%q want %q", cfg.GmailPass, env["GMAIL_APP_PASSWORD"])
	}
}

func TestLoadConfig_MissingVars_ReturnEmptyStrings(t *testing.T) {
	keys := []string{
		"DB_HOST",
		"DB_PORT",
		"DB_USER",
		"DB_PASSWORD",
		"DB_NAME",
		"JWT_SECRET",
		"GEMINI_KEY",
		"GMAIL_USER",
		"GMAIL_APP_PASSWORD",
	}

	for _, k := range keys {
		os.Unsetenv(k)
	}

	cfg := LoadConfig()

	if cfg.DBHost != "" || cfg.DBPort != "" || cfg.DBUser != "" || cfg.DBPassword != "" || cfg.DBName != "" ||
		cfg.JWTSecret != "" || cfg.GeminiKey != "" || cfg.GmailUser != "" || cfg.GmailPass != "" {
		t.Fatalf("expected all empty strings, got: %+v", cfg)
	}
}

func TestLoadConfig_DoesNotReadUnusedEnvVars(t *testing.T) {
	os.Setenv("SMTP_KEY", "should-not-be-read")
	t.Cleanup(func() { os.Unsetenv("SMTP_KEY") })

	cfg := LoadConfig()

	if cfg.SMTPKey != "" {
		t.Fatalf("expected SMTPKey empty (not loaded), got %q", cfg.SMTPKey)
	}
}
