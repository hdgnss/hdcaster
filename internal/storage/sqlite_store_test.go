package storage

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hdcaster/internal/model"
)

func TestSQLiteStoreSaveLoad(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	store := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	cfg := model.NewAppConfig()
	cfg.RuntimeLimits = model.RuntimeLimits{MaxClients: 10, MaxSources: 4, MaxPendingConnections: 7}
	cfg.AdminUsers = []model.AdminUser{{Username: "admin", PasswordHash: "hash", DisplayName: "Admin", Enabled: true}}
	cfg.SourceUsers = []model.SourceUser{{Username: "src", PasswordHash: "hash2", Enabled: true, AllowedMountpoints: []string{"M1"}}}
	cfg.ClientUsers = []model.ClientUser{{Username: "cli", PasswordHash: "hash3", Enabled: true, AllowedMountpoints: []string{"M1"}}}
	cfg.Mountpoints = []model.Mountpoint{{Name: "M1", Enabled: true, SourceUsername: "src", AllowedClientUsers: []string{"cli"}, SupportedConstellations: []string{"GPS", "BDS"}, RTCMMessages: []string{"1005", "1077"}, DataRateBps: 1200, DecodeCandidate: true}}
	cfg.Relays = []model.Relay{{
		Name:               "relay1",
		Enabled:            true,
		LocalMount:         "M1",
		UpstreamHost:       "upstream.example.com",
		UpstreamPort:       2101,
		UpstreamMount:      "SRC1",
		Username:           "relay-user",
		Password:           "relay-pass",
		NTRIPVersion:       1,
		GGAIntervalSeconds: 5,
		ClusterRadiusKM:    30,
		ClusterSlots:       2,
		AccountPool: []model.RelayAccount{
			{Name: "pool-a", Username: "relay-a", Password: "secret-a", Enabled: true},
			{Name: "pool-b", Username: "relay-b", Password: "secret-b", Enabled: true},
		},
	}}
	cfg.BlockedIPRules = []model.BlockedIPRule{{Value: "203.0.113.0/24", Kind: "cidr", Enabled: true, Reason: "test"}}

	if err := store.Save(context.Background(), &cfg); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.AdminUsers) != 1 || loaded.AdminUsers[0].Username != "admin" {
		t.Fatalf("unexpected admins: %+v", loaded.AdminUsers)
	}
	if len(loaded.Mountpoints) != 1 || loaded.Mountpoints[0].Name != "M1" {
		t.Fatalf("unexpected mountpoints: %+v", loaded.Mountpoints)
	}
	if len(loaded.Relays) != 1 || loaded.Relays[0].Name != "relay1" {
		t.Fatalf("unexpected relays: %+v", loaded.Relays)
	}
	if len(loaded.Relays[0].AccountPool) != 2 || loaded.Relays[0].ClusterRadiusKM != 30 {
		t.Fatalf("unexpected relay pool config: %+v", loaded.Relays[0])
	}
	if loaded.RuntimeLimits.MaxPendingConnections != 7 {
		t.Fatalf("unexpected limits: %+v", loaded.RuntimeLimits)
	}
	if loaded.SchemaVersion != model.SchemaVersion {
		t.Fatalf("unexpected schema version: %d", loaded.SchemaVersion)
	}
	if loaded.UpdatedAt.IsZero() {
		t.Fatal("expected config updated_at to be stored in meta")
	}
}

func TestOpenStoreAlwaysReturnsSQLite(t *testing.T) {
	store := OpenStore(filepath.Join(t.TempDir(), "state.anything"))
	if _, ok := store.(*SQLiteStore); !ok {
		t.Fatalf("expected OpenStore to return SQLiteStore, got %T", store)
	}
}

func TestSQLiteStoreBackup(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	store := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	cfg := model.NewAppConfig()
	cfg.AdminUsers = []model.AdminUser{{Username: "admin", PasswordHash: "hash", Enabled: true}}
	if err := store.Save(context.Background(), &cfg); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	backupPath := filepath.Join(t.TempDir(), "backup.sqlite3")
	if err := store.Backup(context.Background(), backupPath); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	info, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("backup stat error = %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty sqlite backup")
	}
}

func TestSQLiteRuntimeHistory(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	store := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	now := time.Now().UTC()
	err := store.AppendRuntimeHistory(context.Background(), []RuntimeHistoryPoint{
		{
			Mount:          "M1",
			Username:       "src",
			RemoteAddr:     "127.0.0.1:1234",
			SampleTime:     now,
			ConnectedAt:    now.Add(-time.Minute),
			LastActive:     now,
			BytesIn:        1000,
			BytesOut:       800,
			ClientCount:    2,
			MessageTypes:   []int{1005, 1077},
			Constellations: []string{"GPS", "BDS"},
			FramesObserved: 12,
		},
	})
	if err != nil {
		t.Fatalf("AppendRuntimeHistory() error = %v", err)
	}
	rows, err := store.ListRuntimeHistory(context.Background(), "M1", 10)
	if err != nil {
		t.Fatalf("ListRuntimeHistory() error = %v", err)
	}
	if len(rows) != 1 || rows[0].Mount != "M1" || rows[0].ClientCount != 2 {
		t.Fatalf("unexpected history rows: %+v", rows)
	}
}

func TestSQLiteAuditEvents(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	store := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	now := time.Now().UTC()
	err := store.AppendAuditEvents(context.Background(), []AuditEvent{
		{
			At:         now,
			Actor:      "admin",
			Action:     "auth.login",
			Resource:   "admin_session",
			ResourceID: "admin",
			Status:     "ok",
			RemoteAddr: "127.0.0.1:1234",
			Message:    "admin login succeeded",
			Details: map[string]string{
				"method": "password",
			},
		},
	})
	if err != nil {
		t.Fatalf("AppendAuditEvents() error = %v", err)
	}
	rows, err := store.ListAuditEvents(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListAuditEvents() error = %v", err)
	}
	if len(rows) != 1 || rows[0].Action != "auth.login" || rows[0].Details["method"] != "password" {
		t.Fatalf("unexpected audit rows: %+v", rows)
	}
}

func TestSQLiteStoreUsesNormalizedSchema(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	store := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	if _, err := store.Load(context.Background()); err != nil {
		t.Fatalf("initial load error = %v", err)
	}
	checks := []struct {
		sql      string
		unwanted string
	}{
		{"SELECT group_concat(name, ',') AS cols FROM pragma_table_info('auth_settings');", "oidc_scopes_json"},
		{"SELECT group_concat(name, ',') AS cols FROM pragma_table_info('source_users');", "allowed_mountpoints_json"},
		{"SELECT group_concat(name, ',') AS cols FROM pragma_table_info('client_users');", "allowed_mountpoints_json"},
		{"SELECT group_concat(name, ',') AS cols FROM pragma_table_info('mountpoints');", "position_json"},
		{"SELECT group_concat(name, ',') AS cols FROM pragma_table_info('relays');", "account_pool_json"},
		{"SELECT group_concat(name, ',') AS cols FROM pragma_table_info('runtime_source_history');", "message_types_json"},
	}
	for _, check := range checks {
		rows, err := store.queryRows(context.Background(), check.sql)
		if err != nil {
			t.Fatalf("query schema %q error = %v", check.sql, err)
		}
		if len(rows) != 1 || strings.Contains(rowString(rows[0], "cols"), check.unwanted) {
			t.Fatalf("unexpected legacy column %q in %q: %+v", check.unwanted, check.sql, rows)
		}
	}
}

func TestSQLiteStoreQueryRowsUsesCSV(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not installed")
	}

	store := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	if _, err := store.Load(context.Background()); err != nil {
		t.Fatalf("initial load error = %v", err)
	}
	rows, err := store.queryRows(context.Background(), "SELECT 'hello,world' AS text, 42 AS answer, '' AS empty;")
	if err != nil {
		t.Fatalf("queryRows() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if rowString(rows[0], "text") != "hello,world" || rowInt(rows[0], "answer") != 42 || rowString(rows[0], "empty") != "" {
		t.Fatalf("unexpected parsed csv row: %+v", rows[0])
	}
}
