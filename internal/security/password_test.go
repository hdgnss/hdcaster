package security

import "testing"

func TestHashPassword(t *testing.T) {
	hash, err := HashPassword("secret")
	if err != nil {
		t.Fatalf("HashPassword() error = %v", err)
	}
	if !CheckPassword(hash, "secret") {
		t.Fatal("expected password verification success")
	}
	if CheckPassword(hash, "wrong") {
		t.Fatal("expected password verification failure")
	}
}
