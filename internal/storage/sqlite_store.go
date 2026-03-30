package storage

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"hdcaster/internal/model"
)

type SQLiteStore struct {
	path string
	mu   sync.Mutex
}

func NewSQLiteStore(path string) *SQLiteStore {
	return &SQLiteStore{path: path}
}

func (s *SQLiteStore) Backup(ctx context.Context, dstPath string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	_ = os.Remove(dstPath)
	script := fmt.Sprintf("VACUUM INTO %s;\n", sqliteText(dstPath))
	_, err := s.runSQLite(ctx, script, false)
	return err
}

func (s *SQLiteStore) Load(ctx context.Context) (*model.AppConfig, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return nil, err
	}
	return s.loadLocked(ctx)
}

func (s *SQLiteStore) Save(ctx context.Context, cfg *model.AppConfig) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if cfg == nil {
		return ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return err
	}
	return s.saveLocked(ctx, cfg)
}

func (s *SQLiteStore) AppendAuditEvents(ctx context.Context, events []AuditEvent) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return err
	}
	var script strings.Builder
	script.WriteString("BEGIN IMMEDIATE;\n")
	for _, event := range events {
		fmt.Fprintf(&script, "INSERT INTO audit_events (at, actor, action, resource, resource_id, status, remote_addr, message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);\n",
			sqliteTime(event.At), sqliteText(event.Actor), sqliteText(event.Action), sqliteText(event.Resource), sqliteText(event.ResourceID), sqliteText(event.Status), sqliteText(event.RemoteAddr), sqliteText(event.Message))
		for key, value := range event.Details {
			fmt.Fprintf(&script, "INSERT INTO audit_event_details (event_id, key, value) VALUES ((SELECT max(id) FROM audit_events), %s, %s);\n",
				sqliteText(key), sqliteText(value))
		}
	}
	script.WriteString("DELETE FROM audit_event_details WHERE event_id NOT IN (SELECT id FROM audit_events ORDER BY at DESC, id DESC LIMIT 20000);\n")
	script.WriteString("DELETE FROM audit_events WHERE id NOT IN (SELECT id FROM audit_events ORDER BY at DESC, id DESC LIMIT 20000);\n")
	script.WriteString("COMMIT;\n")
	_, err := s.runSQLite(ctx, script.String(), false)
	return err
}

func (s *SQLiteStore) ListAuditEvents(ctx context.Context, limit int) ([]AuditEvent, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.queryRows(ctx, fmt.Sprintf("SELECT id, at, actor, action, resource, resource_id, status, remote_addr, message FROM audit_events ORDER BY at DESC, id DESC LIMIT %d;", limit))
	if err != nil {
		return nil, err
	}
	detailRows, err := s.queryRows(ctx, fmt.Sprintf("SELECT event_id, key, value FROM audit_event_details WHERE event_id IN (SELECT id FROM audit_events ORDER BY at DESC, id DESC LIMIT %d) ORDER BY event_id, key;", limit))
	if err != nil {
		return nil, err
	}
	details := groupStringMap(detailRows, "event_id", "key", "value")
	out := make([]AuditEvent, 0, len(rows))
	for _, row := range rows {
		id := rowInt64(row, "id")
		out = append(out, AuditEvent{
			ID:         id,
			At:         rowTime(row, "at"),
			Actor:      rowString(row, "actor"),
			Action:     rowString(row, "action"),
			Resource:   rowString(row, "resource"),
			ResourceID: rowString(row, "resource_id"),
			Status:     rowString(row, "status"),
			RemoteAddr: rowString(row, "remote_addr"),
			Message:    rowString(row, "message"),
			Details:    details[normalizeKey(strconv.FormatInt(id, 10))],
		})
	}
	return out, nil
}

func (s *SQLiteStore) ListAdminUsers(ctx context.Context) ([]model.AdminUser, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	return append([]model.AdminUser(nil), cfg.AdminUsers...), nil
}

func (s *SQLiteStore) GetAdminUser(ctx context.Context, username string) (model.AdminUser, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return model.AdminUser{}, err
	}
	for _, user := range cfg.AdminUsers {
		if equalKey(user.Username, username) {
			return user, nil
		}
	}
	return model.AdminUser{}, ErrNotFound
}

func (s *SQLiteStore) UpsertAdminUser(ctx context.Context, user model.AdminUser) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		user.Username = strings.TrimSpace(user.Username)
		user.DisplayName = strings.TrimSpace(user.DisplayName)
		if user.Username == "" {
			return ErrInvalidArgument
		}
		upsertAdminUser(cfg, user)
		return nil
	})
}

func (s *SQLiteStore) DeleteAdminUser(ctx context.Context, username string) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		var removed bool
		cfg.AdminUsers, removed = deleteBy(cfg.AdminUsers, func(u model.AdminUser) bool {
			return equalKey(u.Username, username)
		})
		if !removed {
			return ErrNotFound
		}
		return nil
	})
}

func (s *SQLiteStore) ListSourceUsers(ctx context.Context) ([]model.SourceUser, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	return append([]model.SourceUser(nil), cfg.SourceUsers...), nil
}

func (s *SQLiteStore) GetSourceUser(ctx context.Context, username string) (model.SourceUser, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return model.SourceUser{}, err
	}
	for _, user := range cfg.SourceUsers {
		if equalKey(user.Username, username) {
			return user, nil
		}
	}
	return model.SourceUser{}, ErrNotFound
}

func (s *SQLiteStore) UpsertSourceUser(ctx context.Context, user model.SourceUser) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		user.Username = strings.TrimSpace(user.Username)
		user.DisplayName = strings.TrimSpace(user.DisplayName)
		user.Description = strings.TrimSpace(user.Description)
		if user.Username == "" {
			return ErrInvalidArgument
		}
		user.AllowedMountpoints = normalizeList(user.AllowedMountpoints)
		upsertSourceUser(cfg, user)
		return nil
	})
}

func (s *SQLiteStore) DeleteSourceUser(ctx context.Context, username string) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		var removed bool
		cfg.SourceUsers, removed = deleteBy(cfg.SourceUsers, func(u model.SourceUser) bool {
			return equalKey(u.Username, username)
		})
		if !removed {
			return ErrNotFound
		}
		return nil
	})
}

func (s *SQLiteStore) ListClientUsers(ctx context.Context) ([]model.ClientUser, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	return append([]model.ClientUser(nil), cfg.ClientUsers...), nil
}

func (s *SQLiteStore) GetClientUser(ctx context.Context, username string) (model.ClientUser, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return model.ClientUser{}, err
	}
	for _, user := range cfg.ClientUsers {
		if equalKey(user.Username, username) {
			return user, nil
		}
	}
	return model.ClientUser{}, ErrNotFound
}

func (s *SQLiteStore) UpsertClientUser(ctx context.Context, user model.ClientUser) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		user.Username = strings.TrimSpace(user.Username)
		user.DisplayName = strings.TrimSpace(user.DisplayName)
		user.Description = strings.TrimSpace(user.Description)
		if user.Username == "" {
			return ErrInvalidArgument
		}
		user.AllowedMountpoints = normalizeList(user.AllowedMountpoints)
		upsertClientUser(cfg, user)
		return nil
	})
}

func (s *SQLiteStore) DeleteClientUser(ctx context.Context, username string) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		var removed bool
		cfg.ClientUsers, removed = deleteBy(cfg.ClientUsers, func(u model.ClientUser) bool {
			return equalKey(u.Username, username)
		})
		if !removed {
			return ErrNotFound
		}
		return nil
	})
}

func (s *SQLiteStore) ListMountpoints(ctx context.Context) ([]model.Mountpoint, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	return append([]model.Mountpoint(nil), cfg.Mountpoints...), nil
}

func (s *SQLiteStore) GetMountpoint(ctx context.Context, name string) (model.Mountpoint, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return model.Mountpoint{}, err
	}
	for _, item := range cfg.Mountpoints {
		if equalKey(item.Name, name) {
			return item, nil
		}
	}
	return model.Mountpoint{}, ErrNotFound
}

func (s *SQLiteStore) UpsertMountpoint(ctx context.Context, mountpoint model.Mountpoint) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		mountpoint.Name = strings.TrimSpace(mountpoint.Name)
		mountpoint.Description = strings.TrimSpace(mountpoint.Description)
		mountpoint.SourceUsername = strings.TrimSpace(mountpoint.SourceUsername)
		if mountpoint.Name == "" {
			return ErrInvalidArgument
		}
		mountpoint.AllowedSourceUsers = normalizeList(mountpoint.AllowedSourceUsers)
		mountpoint.AllowedClientUsers = normalizeList(mountpoint.AllowedClientUsers)
		mountpoint.SupportedConstellations = normalizeList(mountpoint.SupportedConstellations)
		mountpoint.RTCMMessages = normalizeList(mountpoint.RTCMMessages)
		upsertMountpoint(cfg, mountpoint)
		return nil
	})
}

func (s *SQLiteStore) DeleteMountpoint(ctx context.Context, name string) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		var removed bool
		cfg.Mountpoints, removed = deleteBy(cfg.Mountpoints, func(m model.Mountpoint) bool {
			return equalKey(m.Name, name)
		})
		if !removed {
			return ErrNotFound
		}
		return nil
	})
}

func (s *SQLiteStore) ListBlockedIPRules(ctx context.Context) ([]model.BlockedIPRule, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	return append([]model.BlockedIPRule(nil), cfg.BlockedIPRules...), nil
}

func (s *SQLiteStore) GetBlockedIPRule(ctx context.Context, value, kind string) (model.BlockedIPRule, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return model.BlockedIPRule{}, err
	}
	for _, item := range cfg.BlockedIPRules {
		if equalKey(item.Value, value) && equalKey(item.Kind, kind) {
			return item, nil
		}
	}
	return model.BlockedIPRule{}, ErrNotFound
}

func (s *SQLiteStore) UpsertBlockedIPRule(ctx context.Context, rule model.BlockedIPRule) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		rule.Value = strings.TrimSpace(rule.Value)
		rule.Kind = strings.TrimSpace(rule.Kind)
		rule.Reason = strings.TrimSpace(rule.Reason)
		if rule.Value == "" || rule.Kind == "" {
			return ErrInvalidArgument
		}
		upsertBlockedIPRule(cfg, rule)
		return nil
	})
}

func (s *SQLiteStore) DeleteBlockedIPRule(ctx context.Context, value, kind string) error {
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		var removed bool
		cfg.BlockedIPRules, removed = deleteBy(cfg.BlockedIPRules, func(r model.BlockedIPRule) bool {
			return equalKey(r.Value, value) && equalKey(r.Kind, kind)
		})
		if !removed {
			return ErrNotFound
		}
		return nil
	})
}

func (s *SQLiteStore) GetRuntimeLimits(ctx context.Context) (model.RuntimeLimits, error) {
	cfg, err := s.Load(ctx)
	if err != nil {
		return model.RuntimeLimits{}, err
	}
	return cfg.RuntimeLimits, nil
}

func (s *SQLiteStore) SetRuntimeLimits(ctx context.Context, limits model.RuntimeLimits) error {
	limits.Normalize()
	return s.mutate(ctx, func(cfg *model.AppConfig) error {
		cfg.RuntimeLimits = limits
		return nil
	})
}

func (s *SQLiteStore) mutate(ctx context.Context, fn func(cfg *model.AppConfig) error) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return err
	}
	cfg, err := s.loadLocked(ctx)
	if err != nil {
		return err
	}
	if err := fn(cfg); err != nil {
		return err
	}
	return s.saveLocked(ctx, cfg)
}

func (s *SQLiteStore) initLocked(ctx context.Context) error {
	script := `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=OFF;
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS admin_users (
  username TEXT PRIMARY KEY,
  password_hash TEXT NOT NULL,
  display_name TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS admin_user_settings (
  username TEXT PRIMARY KEY,
  require_password_change INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_settings (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  initialized INTEGER NOT NULL,
  local_enabled INTEGER NOT NULL,
  oidc_enabled INTEGER NOT NULL,
  oidc_provider TEXT NOT NULL,
  oidc_issuer_url TEXT NOT NULL,
  oidc_client_id TEXT NOT NULL,
  oidc_client_secret TEXT NOT NULL,
  oidc_redirect_url TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_oidc_values (
  kind TEXT NOT NULL,
  value TEXT NOT NULL,
  ord INTEGER NOT NULL,
  PRIMARY KEY (kind, ord)
);
CREATE TABLE IF NOT EXISTS source_users (
  username TEXT PRIMARY KEY,
  password_hash TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_user_mounts (
  username TEXT NOT NULL,
  mountpoint TEXT NOT NULL,
  ord INTEGER NOT NULL,
  PRIMARY KEY (username, ord)
);
CREATE TABLE IF NOT EXISTS source_user_metadata (
  username TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (username, key)
);
CREATE TABLE IF NOT EXISTS client_users (
  username TEXT PRIMARY KEY,
  password_hash TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS client_user_mounts (
  username TEXT NOT NULL,
  mountpoint TEXT NOT NULL,
  ord INTEGER NOT NULL,
  PRIMARY KEY (username, ord)
);
CREATE TABLE IF NOT EXISTS client_user_metadata (
  username TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (username, key)
);
CREATE TABLE IF NOT EXISTS mountpoints (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  source_username TEXT NOT NULL,
  data_rate_bps INTEGER NOT NULL,
  decode_candidate INTEGER NOT NULL,
  last_seen_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mountpoint_values (
  mount_name TEXT NOT NULL,
  kind TEXT NOT NULL,
  value TEXT NOT NULL,
  ord INTEGER NOT NULL,
  PRIMARY KEY (mount_name, kind, ord)
);
CREATE TABLE IF NOT EXISTS mountpoint_positions (
  mount_name TEXT PRIMARY KEY,
  latitude REAL NOT NULL,
  longitude REAL NOT NULL,
  altitude REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS relays (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  local_mount TEXT NOT NULL,
  upstream_host TEXT NOT NULL,
  upstream_port INTEGER NOT NULL,
  upstream_mount TEXT NOT NULL,
  username TEXT NOT NULL,
  password TEXT NOT NULL,
  ntrip_version INTEGER NOT NULL,
  gga_sentence TEXT NOT NULL,
  gga_interval_seconds INTEGER NOT NULL,
  cluster_radius_km REAL NOT NULL DEFAULT 30,
  cluster_slots INTEGER NOT NULL DEFAULT 2,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS relay_accounts (
  relay_name TEXT NOT NULL,
  ord INTEGER NOT NULL,
  name TEXT NOT NULL,
  username TEXT NOT NULL,
  password TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  expires_at TEXT NOT NULL,
  PRIMARY KEY (relay_name, ord)
);
CREATE TABLE IF NOT EXISTS blocked_ip_rules (
  value TEXT NOT NULL,
  kind TEXT NOT NULL,
  reason TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (value, kind)
);
CREATE TABLE IF NOT EXISTS runtime_limits (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  max_admins INTEGER NOT NULL,
  max_source_users INTEGER NOT NULL,
  max_client_users INTEGER NOT NULL,
  max_clients INTEGER NOT NULL,
  max_sources INTEGER NOT NULL,
  max_mountpoints INTEGER NOT NULL,
  max_blocked_ip_rules INTEGER NOT NULL,
  max_pending_connections INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_source_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mount TEXT NOT NULL,
  username TEXT NOT NULL,
  remote_addr TEXT NOT NULL,
  sample_time TEXT NOT NULL,
  connected_at TEXT NOT NULL,
  last_active TEXT NOT NULL,
  bytes_in INTEGER NOT NULL,
  bytes_out INTEGER NOT NULL,
  client_count INTEGER NOT NULL,
  frames_observed INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_source_history_message_types (
  history_id INTEGER NOT NULL,
  ord INTEGER NOT NULL,
  message_type INTEGER NOT NULL,
  PRIMARY KEY (history_id, ord)
);
CREATE TABLE IF NOT EXISTS runtime_source_history_constellations (
  history_id INTEGER NOT NULL,
  ord INTEGER NOT NULL,
  constellation TEXT NOT NULL,
  PRIMARY KEY (history_id, ord)
);
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  resource TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  status TEXT NOT NULL,
  remote_addr TEXT NOT NULL,
  message TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_event_details (
  event_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (event_id, key)
);
CREATE INDEX IF NOT EXISTS idx_runtime_source_history_mount_time
ON runtime_source_history (mount, sample_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_at_desc
ON audit_events (at DESC, id DESC);
INSERT OR IGNORE INTO runtime_limits (
  id, max_admins, max_source_users, max_client_users, max_clients, max_sources, max_mountpoints, max_blocked_ip_rules, max_pending_connections
) VALUES (1, 0, 0, 0, 0, 0, 0, 0, 0);
INSERT OR IGNORE INTO auth_settings (
  id, initialized, local_enabled, oidc_enabled, oidc_provider, oidc_issuer_url, oidc_client_id, oidc_client_secret, oidc_redirect_url
) VALUES (1, 0, 1, 0, '', '', '', '', '');
`
	_, err := s.runSQLite(ctx, script, false)
	return err
}

func (s *SQLiteStore) AppendRuntimeHistory(ctx context.Context, points []RuntimeHistoryPoint) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return err
	}
	var script strings.Builder
	script.WriteString("BEGIN IMMEDIATE;\n")
	for _, p := range points {
		fmt.Fprintf(&script, "INSERT INTO runtime_source_history (mount, username, remote_addr, sample_time, connected_at, last_active, bytes_in, bytes_out, client_count, frames_observed) VALUES (%s, %s, %s, %s, %s, %s, %d, %d, %d, %d);\n",
			sqliteText(p.Mount), sqliteText(p.Username), sqliteText(p.RemoteAddr), sqliteTime(p.SampleTime), sqliteTime(p.ConnectedAt), sqliteTime(p.LastActive),
			p.BytesIn, p.BytesOut, p.ClientCount, p.FramesObserved)
		for i, messageType := range p.MessageTypes {
			fmt.Fprintf(&script, "INSERT INTO runtime_source_history_message_types (history_id, ord, message_type) VALUES ((SELECT max(id) FROM runtime_source_history), %d, %d);\n", i, messageType)
		}
		for i, constellation := range p.Constellations {
			fmt.Fprintf(&script, "INSERT INTO runtime_source_history_constellations (history_id, ord, constellation) VALUES ((SELECT max(id) FROM runtime_source_history), %d, %s);\n", i, sqliteText(constellation))
		}
	}
	script.WriteString("DELETE FROM runtime_source_history WHERE id NOT IN (SELECT id FROM runtime_source_history ORDER BY sample_time DESC LIMIT 10000);\n")
	script.WriteString("COMMIT;\n")
	_, err := s.runSQLite(ctx, script.String(), false)
	return err
}

func (s *SQLiteStore) ListRuntimeHistory(ctx context.Context, mount string, limit int) ([]RuntimeHistoryPoint, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 60
	}
	rows, err := s.queryRows(ctx, fmt.Sprintf("SELECT id, mount, username, remote_addr, sample_time, connected_at, last_active, bytes_in, bytes_out, client_count, frames_observed FROM runtime_source_history WHERE mount = %s ORDER BY sample_time DESC LIMIT %d;", sqliteText(mount), limit))
	if err != nil {
		return nil, err
	}
	historyMessageRows, err := s.queryRows(ctx, fmt.Sprintf("SELECT history_id, ord, message_type FROM runtime_source_history_message_types WHERE history_id IN (SELECT id FROM runtime_source_history WHERE mount = %s ORDER BY sample_time DESC LIMIT %d) ORDER BY history_id, ord;", sqliteText(mount), limit))
	if err != nil {
		return nil, err
	}
	historyConstellationRows, err := s.queryRows(ctx, fmt.Sprintf("SELECT history_id, ord, constellation FROM runtime_source_history_constellations WHERE history_id IN (SELECT id FROM runtime_source_history WHERE mount = %s ORDER BY sample_time DESC LIMIT %d) ORDER BY history_id, ord;", sqliteText(mount), limit))
	if err != nil {
		return nil, err
	}
	messageTypesByHistory := groupHistoryMessageTypes(historyMessageRows)
	constellationsByHistory := groupHistoryConstellations(historyConstellationRows)
	out := make([]RuntimeHistoryPoint, 0, len(rows))
	for _, row := range rows {
		historyID := rowInt64(row, "id")
		out = append(out, RuntimeHistoryPoint{
			Mount:          rowString(row, "mount"),
			Username:       rowString(row, "username"),
			RemoteAddr:     rowString(row, "remote_addr"),
			SampleTime:     rowTime(row, "sample_time"),
			ConnectedAt:    rowTime(row, "connected_at"),
			LastActive:     rowTime(row, "last_active"),
			BytesIn:        uint64(rowInt64(row, "bytes_in")),
			BytesOut:       uint64(rowInt64(row, "bytes_out")),
			ClientCount:    rowInt(row, "client_count"),
			MessageTypes:   messageTypesByHistory[historyID],
			Constellations: constellationsByHistory[historyID],
			FramesObserved: uint64(rowInt64(row, "frames_observed")),
		})
	}
	return out, nil
}

func (s *SQLiteStore) loadLocked(ctx context.Context) (*model.AppConfig, error) {
	cfg := model.NewAppConfig()
	cfg.AdminUsers = nil
	cfg.SourceUsers = nil
	cfg.ClientUsers = nil
	cfg.Mountpoints = nil
	cfg.Relays = nil
	cfg.BlockedIPRules = nil

	adminSettingRows, err := s.queryRows(ctx, "SELECT username, require_password_change FROM admin_user_settings;")
	if err != nil {
		return nil, err
	}
	adminSettings := make(map[string]bool, len(adminSettingRows))
	for _, row := range adminSettingRows {
		adminSettings[strings.ToLower(rowString(row, "username"))] = rowBool(row, "require_password_change")
	}

	authRows, err := s.queryRows(ctx, "SELECT initialized, local_enabled, oidc_enabled, oidc_provider, oidc_issuer_url, oidc_client_id, oidc_client_secret, oidc_redirect_url FROM auth_settings WHERE id = 1;")
	if err != nil {
		return nil, err
	}
	authOIDCValues, err := s.queryRows(ctx, "SELECT kind, value, ord FROM auth_oidc_values ORDER BY kind, ord;")
	if err != nil {
		return nil, err
	}
	authScopes, authAllowedEmails, authAllowedDomains := splitAuthOIDCValues(authOIDCValues)
	if len(authRows) > 0 {
		row := authRows[0]
		cfg.Auth = model.AuthSettings{
			Initialized:  rowBool(row, "initialized"),
			LocalEnabled: rowBool(row, "local_enabled"),
			OIDC: model.OIDCAuthSettings{
				Enabled:        rowBool(row, "oidc_enabled"),
				Provider:       rowString(row, "oidc_provider"),
				IssuerURL:      rowString(row, "oidc_issuer_url"),
				ClientID:       rowString(row, "oidc_client_id"),
				ClientSecret:   rowString(row, "oidc_client_secret"),
				RedirectURL:    rowString(row, "oidc_redirect_url"),
				Scopes:         authScopes,
				AllowedEmails:  authAllowedEmails,
				AllowedDomains: authAllowedDomains,
			},
		}
	}
	cfg.SchemaVersion = model.SchemaVersion
	if schemaVersion, err := s.metaInt(ctx, "schema_version"); err == nil && schemaVersion > 0 {
		cfg.SchemaVersion = schemaVersion
	}
	if updatedAt, err := s.metaTime(ctx, "config_updated_at"); err == nil {
		cfg.UpdatedAt = updatedAt
	}

	adminRows, err := s.queryRows(ctx, "SELECT username, password_hash, display_name, enabled, created_at, updated_at FROM admin_users ORDER BY username;")
	if err != nil {
		return nil, err
	}
	for _, row := range adminRows {
		cfg.AdminUsers = append(cfg.AdminUsers, model.AdminUser{
			Username:              rowString(row, "username"),
			PasswordHash:          rowString(row, "password_hash"),
			DisplayName:           rowString(row, "display_name"),
			Enabled:               rowBool(row, "enabled"),
			RequirePasswordChange: adminSettings[strings.ToLower(rowString(row, "username"))],
			CreatedAt:             rowTime(row, "created_at"),
			UpdatedAt:             rowTime(row, "updated_at"),
		})
	}

	sourceRows, err := s.queryRows(ctx, "SELECT username, password_hash, display_name, description, enabled, created_at, updated_at FROM source_users ORDER BY username;")
	if err != nil {
		return nil, err
	}
	sourceMounts, err := s.queryRows(ctx, "SELECT username, mountpoint, ord FROM source_user_mounts ORDER BY username, ord;")
	if err != nil {
		return nil, err
	}
	sourceMountMap := groupOrderedValues(sourceMounts, "username", "mountpoint")
	sourceMetadataRows, err := s.queryRows(ctx, "SELECT username, key, value FROM source_user_metadata ORDER BY username, key;")
	if err != nil {
		return nil, err
	}
	sourceMetadataMap := groupStringMap(sourceMetadataRows, "username", "key", "value")
	for _, row := range sourceRows {
		username := rowString(row, "username")
		cfg.SourceUsers = append(cfg.SourceUsers, model.SourceUser{
			Username:           username,
			PasswordHash:       rowString(row, "password_hash"),
			DisplayName:        rowString(row, "display_name"),
			Description:        rowString(row, "description"),
			Enabled:            rowBool(row, "enabled"),
			AllowedMountpoints: sourceMountMap[normalizeKey(username)],
			Metadata:           sourceMetadataMap[normalizeKey(username)],
			CreatedAt:          rowTime(row, "created_at"),
			UpdatedAt:          rowTime(row, "updated_at"),
		})
	}

	clientRows, err := s.queryRows(ctx, "SELECT username, password_hash, display_name, description, enabled, created_at, updated_at FROM client_users ORDER BY username;")
	if err != nil {
		return nil, err
	}
	clientMounts, err := s.queryRows(ctx, "SELECT username, mountpoint, ord FROM client_user_mounts ORDER BY username, ord;")
	if err != nil {
		return nil, err
	}
	clientMountMap := groupOrderedValues(clientMounts, "username", "mountpoint")
	clientMetadataRows, err := s.queryRows(ctx, "SELECT username, key, value FROM client_user_metadata ORDER BY username, key;")
	if err != nil {
		return nil, err
	}
	clientMetadataMap := groupStringMap(clientMetadataRows, "username", "key", "value")
	for _, row := range clientRows {
		username := rowString(row, "username")
		cfg.ClientUsers = append(cfg.ClientUsers, model.ClientUser{
			Username:           username,
			PasswordHash:       rowString(row, "password_hash"),
			DisplayName:        rowString(row, "display_name"),
			Description:        rowString(row, "description"),
			Enabled:            rowBool(row, "enabled"),
			AllowedMountpoints: clientMountMap[normalizeKey(username)],
			Metadata:           clientMetadataMap[normalizeKey(username)],
			CreatedAt:          rowTime(row, "created_at"),
			UpdatedAt:          rowTime(row, "updated_at"),
		})
	}

	mountRows, err := s.queryRows(ctx, "SELECT name, description, enabled, source_username, data_rate_bps, decode_candidate, last_seen_at, created_at, updated_at FROM mountpoints ORDER BY name;")
	if err != nil {
		return nil, err
	}
	mountValueRows, err := s.queryRows(ctx, "SELECT mount_name, kind, value, ord FROM mountpoint_values ORDER BY mount_name, kind, ord;")
	if err != nil {
		return nil, err
	}
	mountValueMap := groupMountValues(mountValueRows)
	mountPositionRows, err := s.queryRows(ctx, "SELECT mount_name, latitude, longitude, altitude FROM mountpoint_positions ORDER BY mount_name;")
	if err != nil {
		return nil, err
	}
	mountPositionMap := groupMountPositions(mountPositionRows)
	for _, row := range mountRows {
		name := rowString(row, "name")
		cfg.Mountpoints = append(cfg.Mountpoints, model.Mountpoint{
			Name:                    name,
			Description:             rowString(row, "description"),
			Enabled:                 rowBool(row, "enabled"),
			SourceUsername:          rowString(row, "source_username"),
			AllowedSourceUsers:      mountValueMap[normalizeKey(name)]["allowed_source_user"],
			AllowedClientUsers:      mountValueMap[normalizeKey(name)]["allowed_client_user"],
			SupportedConstellations: mountValueMap[normalizeKey(name)]["constellation"],
			RTCMMessages:            mountValueMap[normalizeKey(name)]["rtcm_message"],
			Position:                mountPositionMap[normalizeKey(name)],
			DataRateBps:             rowInt64(row, "data_rate_bps"),
			DecodeCandidate:         rowBool(row, "decode_candidate"),
			LastSeenAt:              rowTime(row, "last_seen_at"),
			CreatedAt:               rowTime(row, "created_at"),
			UpdatedAt:               rowTime(row, "updated_at"),
		})
	}

	relayRows, err := s.queryRows(ctx, "SELECT name, description, enabled, local_mount, upstream_host, upstream_port, upstream_mount, username, password, ntrip_version, gga_sentence, gga_interval_seconds, cluster_radius_km, cluster_slots, created_at, updated_at FROM relays ORDER BY name;")
	if err != nil {
		return nil, err
	}
	relayAccountRows, err := s.queryRows(ctx, "SELECT relay_name, ord, name, username, password, enabled, expires_at FROM relay_accounts ORDER BY relay_name, ord;")
	if err != nil {
		return nil, err
	}
	relayAccountMap := groupRelayAccounts(relayAccountRows)
	for _, row := range relayRows {
		name := rowString(row, "name")
		cfg.Relays = append(cfg.Relays, model.Relay{
			Name:               name,
			Description:        rowString(row, "description"),
			Enabled:            rowBool(row, "enabled"),
			LocalMount:         rowString(row, "local_mount"),
			UpstreamHost:       rowString(row, "upstream_host"),
			UpstreamPort:       rowInt(row, "upstream_port"),
			UpstreamMount:      rowString(row, "upstream_mount"),
			Username:           rowString(row, "username"),
			Password:           rowString(row, "password"),
			NTRIPVersion:       rowInt(row, "ntrip_version"),
			AccountPool:        relayAccountMap[normalizeKey(name)],
			GGASentence:        rowString(row, "gga_sentence"),
			GGAIntervalSeconds: rowInt(row, "gga_interval_seconds"),
			ClusterRadiusKM:    rowFloat(row, "cluster_radius_km"),
			ClusterSlots:       rowInt(row, "cluster_slots"),
			CreatedAt:          rowTime(row, "created_at"),
			UpdatedAt:          rowTime(row, "updated_at"),
		})
	}

	blockRows, err := s.queryRows(ctx, "SELECT value, kind, reason, enabled, created_at, updated_at FROM blocked_ip_rules ORDER BY value, kind;")
	if err != nil {
		return nil, err
	}
	for _, row := range blockRows {
		cfg.BlockedIPRules = append(cfg.BlockedIPRules, model.BlockedIPRule{
			Value:     rowString(row, "value"),
			Kind:      rowString(row, "kind"),
			Reason:    rowString(row, "reason"),
			Enabled:   rowBool(row, "enabled"),
			CreatedAt: rowTime(row, "created_at"),
			UpdatedAt: rowTime(row, "updated_at"),
		})
	}

	limitRows, err := s.queryRows(ctx, "SELECT max_admins, max_source_users, max_client_users, max_clients, max_sources, max_mountpoints, max_blocked_ip_rules, max_pending_connections FROM runtime_limits WHERE id = 1;")
	if err != nil {
		return nil, err
	}
	if len(limitRows) > 0 {
		row := limitRows[0]
		cfg.RuntimeLimits = model.RuntimeLimits{
			MaxAdmins:             rowInt(row, "max_admins"),
			MaxSourceUsers:        rowInt(row, "max_source_users"),
			MaxClientUsers:        rowInt(row, "max_client_users"),
			MaxClients:            rowInt(row, "max_clients"),
			MaxSources:            rowInt(row, "max_sources"),
			MaxMountpoints:        rowInt(row, "max_mountpoints"),
			MaxBlockedIPRules:     rowInt(row, "max_blocked_ip_rules"),
			MaxPendingConnections: rowInt(row, "max_pending_connections"),
		}
	}

	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s *SQLiteStore) saveLocked(ctx context.Context, cfg *model.AppConfig) error {
	clone := cfg.Clone()
	clone.Normalize()
	clone.UpdatedAt = time.Now().UTC()
	if err := clone.Validate(); err != nil {
		return err
	}

	var script strings.Builder
	script.WriteString("BEGIN IMMEDIATE;\n")
	script.WriteString("DELETE FROM admin_users;\nDELETE FROM admin_user_settings;\nDELETE FROM auth_settings;\nDELETE FROM auth_oidc_values;\nDELETE FROM source_users;\nDELETE FROM source_user_mounts;\nDELETE FROM source_user_metadata;\nDELETE FROM client_users;\nDELETE FROM client_user_mounts;\nDELETE FROM client_user_metadata;\nDELETE FROM mountpoints;\nDELETE FROM mountpoint_values;\nDELETE FROM mountpoint_positions;\nDELETE FROM relays;\nDELETE FROM relay_accounts;\nDELETE FROM blocked_ip_rules;\nDELETE FROM runtime_limits;\n")
	for _, user := range clone.AdminUsers {
		fmt.Fprintf(&script, "INSERT INTO admin_users (username, password_hash, display_name, enabled, created_at, updated_at) VALUES (%s, %s, %s, %d, %s, %s);\n",
			sqliteText(user.Username), sqliteText(user.PasswordHash), sqliteText(user.DisplayName), boolInt(user.Enabled), sqliteTime(user.CreatedAt), sqliteTime(user.UpdatedAt))
		fmt.Fprintf(&script, "INSERT INTO admin_user_settings (username, require_password_change) VALUES (%s, %d);\n",
			sqliteText(user.Username), boolInt(user.RequirePasswordChange))
	}
	fmt.Fprintf(&script, "INSERT INTO auth_settings (id, initialized, local_enabled, oidc_enabled, oidc_provider, oidc_issuer_url, oidc_client_id, oidc_client_secret, oidc_redirect_url) VALUES (1, %d, %d, %d, %s, %s, %s, %s, %s);\n",
		boolInt(clone.Auth.Initialized), boolInt(clone.Auth.LocalEnabled), boolInt(clone.Auth.OIDC.Enabled),
		sqliteText(clone.Auth.OIDC.Provider), sqliteText(clone.Auth.OIDC.IssuerURL), sqliteText(clone.Auth.OIDC.ClientID), sqliteText(clone.Auth.OIDC.ClientSecret), sqliteText(clone.Auth.OIDC.RedirectURL))
	for i, value := range clone.Auth.OIDC.Scopes {
		fmt.Fprintf(&script, "INSERT INTO auth_oidc_values (kind, value, ord) VALUES ('scope', %s, %d);\n", sqliteText(value), i)
	}
	for i, value := range clone.Auth.OIDC.AllowedEmails {
		fmt.Fprintf(&script, "INSERT INTO auth_oidc_values (kind, value, ord) VALUES ('allowed_email', %s, %d);\n", sqliteText(value), i)
	}
	for i, value := range clone.Auth.OIDC.AllowedDomains {
		fmt.Fprintf(&script, "INSERT INTO auth_oidc_values (kind, value, ord) VALUES ('allowed_domain', %s, %d);\n", sqliteText(value), i)
	}
	for _, user := range clone.SourceUsers {
		fmt.Fprintf(&script, "INSERT INTO source_users (username, password_hash, display_name, description, enabled, created_at, updated_at) VALUES (%s, %s, %s, %s, %d, %s, %s);\n",
			sqliteText(user.Username), sqliteText(user.PasswordHash), sqliteText(user.DisplayName), sqliteText(user.Description), boolInt(user.Enabled), sqliteTime(user.CreatedAt), sqliteTime(user.UpdatedAt))
		for i, mount := range user.AllowedMountpoints {
			fmt.Fprintf(&script, "INSERT INTO source_user_mounts (username, mountpoint, ord) VALUES (%s, %s, %d);\n", sqliteText(user.Username), sqliteText(mount), i)
		}
		for key, value := range user.Metadata {
			fmt.Fprintf(&script, "INSERT INTO source_user_metadata (username, key, value) VALUES (%s, %s, %s);\n", sqliteText(user.Username), sqliteText(key), sqliteText(value))
		}
	}
	for _, user := range clone.ClientUsers {
		fmt.Fprintf(&script, "INSERT INTO client_users (username, password_hash, display_name, description, enabled, created_at, updated_at) VALUES (%s, %s, %s, %s, %d, %s, %s);\n",
			sqliteText(user.Username), sqliteText(user.PasswordHash), sqliteText(user.DisplayName), sqliteText(user.Description), boolInt(user.Enabled), sqliteTime(user.CreatedAt), sqliteTime(user.UpdatedAt))
		for i, mount := range user.AllowedMountpoints {
			fmt.Fprintf(&script, "INSERT INTO client_user_mounts (username, mountpoint, ord) VALUES (%s, %s, %d);\n", sqliteText(user.Username), sqliteText(mount), i)
		}
		for key, value := range user.Metadata {
			fmt.Fprintf(&script, "INSERT INTO client_user_metadata (username, key, value) VALUES (%s, %s, %s);\n", sqliteText(user.Username), sqliteText(key), sqliteText(value))
		}
	}
	for _, mp := range clone.Mountpoints {
		fmt.Fprintf(&script, "INSERT INTO mountpoints (name, description, enabled, source_username, data_rate_bps, decode_candidate, last_seen_at, created_at, updated_at) VALUES (%s, %s, %d, %s, %d, %d, %s, %s, %s);\n",
			sqliteText(mp.Name), sqliteText(mp.Description), boolInt(mp.Enabled), sqliteText(mp.SourceUsername), mp.DataRateBps, boolInt(mp.DecodeCandidate), sqliteTime(mp.LastSeenAt), sqliteTime(mp.CreatedAt), sqliteTime(mp.UpdatedAt))
		for i, value := range mp.AllowedSourceUsers {
			fmt.Fprintf(&script, "INSERT INTO mountpoint_values (mount_name, kind, value, ord) VALUES (%s, 'allowed_source_user', %s, %d);\n", sqliteText(mp.Name), sqliteText(value), i)
		}
		for i, value := range mp.AllowedClientUsers {
			fmt.Fprintf(&script, "INSERT INTO mountpoint_values (mount_name, kind, value, ord) VALUES (%s, 'allowed_client_user', %s, %d);\n", sqliteText(mp.Name), sqliteText(value), i)
		}
		for i, value := range mp.SupportedConstellations {
			fmt.Fprintf(&script, "INSERT INTO mountpoint_values (mount_name, kind, value, ord) VALUES (%s, 'constellation', %s, %d);\n", sqliteText(mp.Name), sqliteText(value), i)
		}
		for i, value := range mp.RTCMMessages {
			fmt.Fprintf(&script, "INSERT INTO mountpoint_values (mount_name, kind, value, ord) VALUES (%s, 'rtcm_message', %s, %d);\n", sqliteText(mp.Name), sqliteText(value), i)
		}
		if mp.Position != nil {
			fmt.Fprintf(&script, "INSERT INTO mountpoint_positions (mount_name, latitude, longitude, altitude) VALUES (%s, %s, %s, %s);\n", sqliteText(mp.Name), sqliteFloat(mp.Position.Latitude), sqliteFloat(mp.Position.Longitude), sqliteFloat(0))
		}
	}
	for _, relay := range clone.Relays {
		fmt.Fprintf(&script, "INSERT INTO relays (name, description, enabled, local_mount, upstream_host, upstream_port, upstream_mount, username, password, ntrip_version, gga_sentence, gga_interval_seconds, cluster_radius_km, cluster_slots, created_at, updated_at) VALUES (%s, %s, %d, %s, %s, %d, %s, %s, %s, %d, %s, %d, %s, %d, %s, %s);\n",
			sqliteText(relay.Name), sqliteText(relay.Description), boolInt(relay.Enabled), sqliteText(relay.LocalMount), sqliteText(relay.UpstreamHost), relay.UpstreamPort, sqliteText(relay.UpstreamMount),
			sqliteText(relay.Username), sqliteText(relay.Password), relay.NTRIPVersion, sqliteText(relay.GGASentence), relay.GGAIntervalSeconds, sqliteFloat(relay.ClusterRadiusKM), relay.ClusterSlots, sqliteTime(relay.CreatedAt), sqliteTime(relay.UpdatedAt))
		for i, account := range relay.AccountPool {
			fmt.Fprintf(&script, "INSERT INTO relay_accounts (relay_name, ord, name, username, password, enabled, expires_at) VALUES (%s, %d, %s, %s, %s, %d, %s);\n",
				sqliteText(relay.Name), i, sqliteText(account.Name), sqliteText(account.Username), sqliteText(account.Password), boolInt(account.Enabled), sqliteTime(account.ExpiresAt))
		}
	}
	for _, rule := range clone.BlockedIPRules {
		fmt.Fprintf(&script, "INSERT INTO blocked_ip_rules (value, kind, reason, enabled, created_at, updated_at) VALUES (%s, %s, %s, %d, %s, %s);\n",
			sqliteText(rule.Value), sqliteText(rule.Kind), sqliteText(rule.Reason), boolInt(rule.Enabled), sqliteTime(rule.CreatedAt), sqliteTime(rule.UpdatedAt))
	}
	limits := clone.RuntimeLimits
	fmt.Fprintf(&script, "INSERT INTO runtime_limits (id, max_admins, max_source_users, max_client_users, max_clients, max_sources, max_mountpoints, max_blocked_ip_rules, max_pending_connections) VALUES (1, %d, %d, %d, %d, %d, %d, %d, %d);\n",
		limits.MaxAdmins, limits.MaxSourceUsers, limits.MaxClientUsers, limits.MaxClients, limits.MaxSources, limits.MaxMountpoints, limits.MaxBlockedIPRules, limits.MaxPendingConnections)
	fmt.Fprintf(&script, "INSERT INTO meta (key, value) VALUES ('schema_version', %s) ON CONFLICT(key) DO UPDATE SET value=excluded.value;\n", sqliteText(fmt.Sprintf("%d", clone.SchemaVersion)))
	fmt.Fprintf(&script, "INSERT INTO meta (key, value) VALUES ('config_updated_at', %s) ON CONFLICT(key) DO UPDATE SET value=excluded.value;\n", sqliteTime(clone.UpdatedAt))
	script.WriteString("COMMIT;\n")

	_, err := s.runSQLite(ctx, script.String(), false)
	return err
}

func (s *SQLiteStore) queryRows(ctx context.Context, sql string) ([]map[string]any, error) {
	out, err := s.runSQLite(ctx, sql, true)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out) == "" {
		return nil, nil
	}
	reader := csv.NewReader(strings.NewReader(out))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("storage: parse sqlite csv output: %w", err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	headers := records[0]
	rows := make([]map[string]any, 0, len(records)-1)
	for _, record := range records[1:] {
		row := make(map[string]any, len(headers))
		for i, header := range headers {
			if i < len(record) {
				row[header] = record[i]
			} else {
				row[header] = ""
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s *SQLiteStore) metaValue(ctx context.Context, key string) (string, error) {
	rows, err := s.queryRows(ctx, fmt.Sprintf("SELECT value FROM meta WHERE key = %s LIMIT 1;", sqliteText(key)))
	if err != nil || len(rows) == 0 {
		return "", err
	}
	return rowString(rows[0], "value"), nil
}

func (s *SQLiteStore) metaInt(ctx context.Context, key string) (int, error) {
	rows, err := s.queryRows(ctx, fmt.Sprintf("SELECT value FROM meta WHERE key = %s LIMIT 1;", sqliteText(key)))
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	return rowInt(rows[0], "value"), nil
}

func (s *SQLiteStore) metaTime(ctx context.Context, key string) (time.Time, error) {
	rows, err := s.queryRows(ctx, fmt.Sprintf("SELECT value FROM meta WHERE key = %s LIMIT 1;", sqliteText(key)))
	if err != nil || len(rows) == 0 {
		return time.Time{}, err
	}
	return rowTime(rows[0], "value"), nil
}

func (s *SQLiteStore) runSQLite(ctx context.Context, script string, jsonMode bool) (string, error) {
	args := []string{}
	if jsonMode {
		args = append(args, "-csv", "-header")
	}
	args = append(args, filepath.Clean(s.path))
	cmd := exec.CommandContext(ctx, "sqlite3", args...)
	cmd.Stdin = strings.NewReader(script)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("storage: sqlite3 failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func sqliteText(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func sqliteTime(t time.Time) string {
	if t.IsZero() {
		return "''"
	}
	return sqliteText(t.UTC().Format(time.RFC3339Nano))
}

func sqliteFloat(v float64) string {
	return fmt.Sprintf("%.6f", v)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func rowString(row map[string]any, key string) string {
	if v, ok := row[key]; ok && v != nil {
		return fmt.Sprint(v)
	}
	return ""
}

func rowBool(row map[string]any, key string) bool {
	switch v := row[key].(type) {
	case float64:
		return v != 0
	case int:
		return v != 0
	case string:
		return v == "1" || strings.EqualFold(v, "true")
	case bool:
		return v
	default:
		return false
	}
}

func rowInt(row map[string]any, key string) int {
	out, _ := strconv.Atoi(rowString(row, key))
	return out
}

func rowInt64(row map[string]any, key string) int64 {
	out, _ := strconv.ParseInt(rowString(row, key), 10, 64)
	return out
}

func rowFloat(row map[string]any, key string) float64 {
	out, _ := strconv.ParseFloat(rowString(row, key), 64)
	return out
}

func rowTime(row map[string]any, key string) time.Time {
	value := rowString(row, key)
	if value == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

func splitAuthOIDCValues(rows []map[string]any) (scopes, emails, domains []string) {
	for _, row := range rows {
		switch rowString(row, "kind") {
		case "scope":
			scopes = append(scopes, rowString(row, "value"))
		case "allowed_email":
			emails = append(emails, rowString(row, "value"))
		case "allowed_domain":
			domains = append(domains, rowString(row, "value"))
		}
	}
	return scopes, emails, domains
}

func groupOrderedValues(rows []map[string]any, keyField, valueField string) map[string][]string {
	out := make(map[string][]string)
	for _, row := range rows {
		key := normalizeKey(rowString(row, keyField))
		if key == "" {
			continue
		}
		out[key] = append(out[key], rowString(row, valueField))
	}
	return out
}

func groupStringMap(rows []map[string]any, ownerField, keyField, valueField string) map[string]map[string]string {
	out := make(map[string]map[string]string)
	for _, row := range rows {
		owner := normalizeKey(rowString(row, ownerField))
		if owner == "" {
			continue
		}
		if out[owner] == nil {
			out[owner] = make(map[string]string)
		}
		out[owner][rowString(row, keyField)] = rowString(row, valueField)
	}
	return out
}

func groupMountValues(rows []map[string]any) map[string]map[string][]string {
	out := make(map[string]map[string][]string)
	for _, row := range rows {
		mount := normalizeKey(rowString(row, "mount_name"))
		kind := rowString(row, "kind")
		if mount == "" || kind == "" {
			continue
		}
		if out[mount] == nil {
			out[mount] = make(map[string][]string)
		}
		out[mount][kind] = append(out[mount][kind], rowString(row, "value"))
	}
	return out
}

func groupMountPositions(rows []map[string]any) map[string]*model.GeoPoint {
	out := make(map[string]*model.GeoPoint)
	for _, row := range rows {
		mount := normalizeKey(rowString(row, "mount_name"))
		if mount == "" {
			continue
		}
		out[mount] = &model.GeoPoint{
			Latitude:  rowFloat(row, "latitude"),
			Longitude: rowFloat(row, "longitude"),
		}
	}
	return out
}

func groupRelayAccounts(rows []map[string]any) map[string][]model.RelayAccount {
	out := make(map[string][]model.RelayAccount)
	for _, row := range rows {
		relayName := normalizeKey(rowString(row, "relay_name"))
		if relayName == "" {
			continue
		}
		out[relayName] = append(out[relayName], model.RelayAccount{
			Name:      rowString(row, "name"),
			Username:  rowString(row, "username"),
			Password:  rowString(row, "password"),
			Enabled:   rowBool(row, "enabled"),
			ExpiresAt: rowTime(row, "expires_at"),
		})
	}
	return out
}

func groupHistoryMessageTypes(rows []map[string]any) map[int64][]int {
	out := make(map[int64][]int)
	for _, row := range rows {
		historyID := rowInt64(row, "history_id")
		if historyID == 0 {
			continue
		}
		out[historyID] = append(out[historyID], rowInt(row, "message_type"))
	}
	return out
}

func groupHistoryConstellations(rows []map[string]any) map[int64][]string {
	out := make(map[int64][]string)
	for _, row := range rows {
		historyID := rowInt64(row, "history_id")
		if historyID == 0 {
			continue
		}
		out[historyID] = append(out[historyID], rowString(row, "constellation"))
	}
	return out
}
