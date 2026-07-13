package main

import "testing"

func TestValidateConfig(t *testing.T) {
	cfg := syncConfig{
		DBHost:     "127.0.0.1",
		DBPort:     "5432",
		DBName:     "postgres",
		DBUser:     "postgres",
		DBPass:     "secret",
		BatchSize:  100,
		MaxBatches: 5,
	}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("validateConfig returned error: %v", err)
	}

	cfg.BatchSize = 0
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected batch size validation error")
	}
}

func TestFormatOptionalUint(t *testing.T) {
	if got := formatOptionalUint(nil); got != "null" {
		t.Fatalf("formatOptionalUint(nil) = %q want null", got)
	}

	value := uint(42)
	if got := formatOptionalUint(&value); got != "42" {
		t.Fatalf("formatOptionalUint(&42) = %q want 42", got)
	}
}
