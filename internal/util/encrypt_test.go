package util

import (
	"testing"
)

func TestHashPassword_AndVerifyPassword_OK(t *testing.T) {
	plain := "MyStrongPassword123!"

	hashed, err := HashPassword(plain)
	if err != nil {
		t.Fatalf("HashPassword err: %v", err)
	}
	if hashed == "" {
		t.Fatalf("expected non-empty hash")
	}
	if hashed == plain {
		t.Fatalf("hash should not equal plain password")
	}

	if err := VerifyPassword(plain, hashed); err != nil {
		t.Fatalf("VerifyPassword should succeed, got: %v", err)
	}
}

func TestVerifyPassword_WrongPassword_ReturnsError(t *testing.T) {
	hashed, err := HashPassword("correct-password")
	if err != nil {
		t.Fatalf("HashPassword err: %v", err)
	}

	if err := VerifyPassword("wrong-password", hashed); err == nil {
		t.Fatalf("expected error for wrong password, got nil")
	}
}

func TestVerifyPassword_InvalidHash_ReturnsError(t *testing.T) {
	if err := VerifyPassword("anything", "not-a-bcrypt-hash"); err == nil {
		t.Fatalf("expected error for invalid hash, got nil")
	}
}

func TestRandomInt_InRange_Inclusive(t *testing.T) {
	min, max := 5, 10
	for i := 0; i < 200; i++ {
		n := RandomInt(min, max)
		if n < min || n > max {
			t.Fatalf("out of range: got=%d expected [%d,%d]", n, min, max)
		}
	}
}

func TestRandomInt_MinEqualsMax(t *testing.T) {
	min, max := 7, 7
	for i := 0; i < 50; i++ {
		n := RandomInt(min, max)
		if n != 7 {
			t.Fatalf("expected %d, got %d", 7, n)
		}
	}
}

func TestRandomInt_MinGreaterThanMax_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when min > max, got none")
		}
	}()
	_ = RandomInt(10, 5)
}
