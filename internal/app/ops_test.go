package app

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNewBuildInfoDefaults(t *testing.T) {
	info := NewBuildInfo("", "", "")
	if info.Version != "dev" {
		t.Fatalf("unexpected version: %q", info.Version)
	}
	if info.Commit != "unknown" {
		t.Fatalf("unexpected commit: %q", info.Commit)
	}
	if info.GoVersion == "" {
		t.Fatal("expected go version")
	}
}

func TestServiceReadinessReport(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	svc, err := Open(filepath.Join(t.TempDir(), "state.db"), AuthConfig{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	report := svc.ReadinessReport(context.Background())
	if report.Status != "not_ready" {
		t.Fatalf("expected not_ready before ntrip listener, got %+v", report)
	}

	svc.SetNTRIPReady(true)
	report = svc.ReadinessReport(context.Background())
	if report.Status != "ready" {
		t.Fatalf("expected ready after ntrip listener, got %+v", report)
	}
}
