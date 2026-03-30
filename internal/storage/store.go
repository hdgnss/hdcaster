package storage

import (
	"context"
	"errors"
	"time"

	"hdcaster/internal/model"
)

var (
	ErrNotFound        = errors.New("storage: not found")
	ErrAlreadyExists   = errors.New("storage: already exists")
	ErrInvalidArgument = errors.New("storage: invalid argument")
)

type Store interface {
	Load(ctx context.Context) (*model.AppConfig, error)
	Save(ctx context.Context, cfg *model.AppConfig) error

	ListAdminUsers(ctx context.Context) ([]model.AdminUser, error)
	GetAdminUser(ctx context.Context, username string) (model.AdminUser, error)
	UpsertAdminUser(ctx context.Context, user model.AdminUser) error
	DeleteAdminUser(ctx context.Context, username string) error

	ListSourceUsers(ctx context.Context) ([]model.SourceUser, error)
	GetSourceUser(ctx context.Context, username string) (model.SourceUser, error)
	UpsertSourceUser(ctx context.Context, user model.SourceUser) error
	DeleteSourceUser(ctx context.Context, username string) error

	ListClientUsers(ctx context.Context) ([]model.ClientUser, error)
	GetClientUser(ctx context.Context, username string) (model.ClientUser, error)
	UpsertClientUser(ctx context.Context, user model.ClientUser) error
	DeleteClientUser(ctx context.Context, username string) error

	ListMountpoints(ctx context.Context) ([]model.Mountpoint, error)
	GetMountpoint(ctx context.Context, name string) (model.Mountpoint, error)
	UpsertMountpoint(ctx context.Context, mountpoint model.Mountpoint) error
	DeleteMountpoint(ctx context.Context, name string) error

	ListBlockedIPRules(ctx context.Context) ([]model.BlockedIPRule, error)
	GetBlockedIPRule(ctx context.Context, value, kind string) (model.BlockedIPRule, error)
	UpsertBlockedIPRule(ctx context.Context, rule model.BlockedIPRule) error
	DeleteBlockedIPRule(ctx context.Context, value, kind string) error

	GetRuntimeLimits(ctx context.Context) (model.RuntimeLimits, error)
	SetRuntimeLimits(ctx context.Context, limits model.RuntimeLimits) error
}

type BackupCapable interface {
	Backup(ctx context.Context, dstPath string) error
}

type RuntimeHistoryPoint struct {
	Mount          string    `json:"mount"`
	Username       string    `json:"username"`
	RemoteAddr     string    `json:"remote_addr"`
	SampleTime     time.Time `json:"sample_time"`
	ConnectedAt    time.Time `json:"connected_at"`
	LastActive     time.Time `json:"last_active"`
	BytesIn        uint64    `json:"bytes_in"`
	BytesOut       uint64    `json:"bytes_out"`
	ClientCount    int       `json:"client_count"`
	MessageTypes   []int     `json:"message_types"`
	Constellations []string  `json:"constellations"`
	FramesObserved uint64    `json:"frames_observed"`
}

type RuntimeHistoryCapable interface {
	AppendRuntimeHistory(ctx context.Context, points []RuntimeHistoryPoint) error
	ListRuntimeHistory(ctx context.Context, mount string, limit int) ([]RuntimeHistoryPoint, error)
}

type AuditEvent struct {
	ID         int64             `json:"id"`
	At         time.Time         `json:"at"`
	Actor      string            `json:"actor,omitempty"`
	Action     string            `json:"action"`
	Resource   string            `json:"resource"`
	ResourceID string            `json:"resource_id,omitempty"`
	Status     string            `json:"status"`
	RemoteAddr string            `json:"remote_addr,omitempty"`
	Message    string            `json:"message,omitempty"`
	Details    map[string]string `json:"details,omitempty"`
}

type AuditCapable interface {
	AppendAuditEvents(ctx context.Context, events []AuditEvent) error
	ListAuditEvents(ctx context.Context, limit int) ([]AuditEvent, error)
}

func OpenStore(path string) Store {
	return NewSQLiteStore(path)
}

func deleteBy[T any](items []T, match func(T) bool) ([]T, bool) {
	out := items[:0]
	removed := false
	for _, item := range items {
		if match(item) {
			removed = true
			continue
		}
		out = append(out, item)
	}
	return out, removed
}
