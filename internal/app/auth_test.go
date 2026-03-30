package app

import (
	"errors"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestLoginRateLimit(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	svc, err := Open(filepath.Join(t.TempDir(), "state.db"), AuthConfig{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for i := 0; i < loginRateMaxFailures; i++ {
		if _, err := svc.Login("admin", "wrong-password", "127.0.0.1:12345"); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("attempt %d: expected unauthorized, got %v", i+1, err)
		}
	}

	if _, err := svc.Login("admin", "admin123456", "127.0.0.1:12345"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected rate limited after repeated failures, got %v", err)
	}
}

func TestSessionInfoRefreshesIdleExpiry(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	svc, err := Open(filepath.Join(t.TempDir(), "state.db"), AuthConfig{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	token, err := svc.Login("admin", "admin123456", "127.0.0.1:12345")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	svc.mu.Lock()
	sess := svc.sessions[token]
	sess.Expiry = time.Now().UTC().Add(5 * time.Second)
	svc.sessions[token] = sess
	svc.mu.Unlock()

	info := svc.SessionInfo(token)
	if authenticated, _ := info["authenticated"].(bool); !authenticated {
		t.Fatalf("expected authenticated session info, got %+v", info)
	}
	if remaining, _ := info["remainingSeconds"].(int); remaining < int((11 * time.Hour).Seconds()) {
		t.Fatalf("expected refreshed session expiry, remaining=%d", remaining)
	}
}

func TestExpiredSessionIsRejected(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	svc, err := Open(filepath.Join(t.TempDir(), "state.db"), AuthConfig{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	token, err := svc.Login("admin", "admin123456", "127.0.0.1:12345")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	svc.mu.Lock()
	sess := svc.sessions[token]
	sess.Expiry = time.Now().UTC().Add(-1 * time.Minute)
	svc.sessions[token] = sess
	svc.mu.Unlock()

	if svc.CheckSession(token) {
		t.Fatal("expected expired session to be rejected")
	}
}
