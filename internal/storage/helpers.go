package storage

import (
	"context"
	"strings"
	"time"

	"hdcaster/internal/model"
)

func upsertAdminUser(cfg *model.AppConfig, user model.AdminUser) {
	now := time.Now().UTC()
	for i := range cfg.AdminUsers {
		if equalKey(cfg.AdminUsers[i].Username, user.Username) {
			user.CreatedAt = cfg.AdminUsers[i].CreatedAt
			if user.CreatedAt.IsZero() {
				user.CreatedAt = now
			}
			user.UpdatedAt = now
			cfg.AdminUsers[i] = user
			return
		}
	}
	user.CreatedAt = now
	user.UpdatedAt = now
	cfg.AdminUsers = append(cfg.AdminUsers, user)
}

func upsertSourceUser(cfg *model.AppConfig, user model.SourceUser) {
	now := time.Now().UTC()
	for i := range cfg.SourceUsers {
		if equalKey(cfg.SourceUsers[i].Username, user.Username) {
			user.CreatedAt = cfg.SourceUsers[i].CreatedAt
			if user.CreatedAt.IsZero() {
				user.CreatedAt = now
			}
			user.UpdatedAt = now
			cfg.SourceUsers[i] = user
			return
		}
	}
	user.CreatedAt = now
	user.UpdatedAt = now
	cfg.SourceUsers = append(cfg.SourceUsers, user)
}

func upsertClientUser(cfg *model.AppConfig, user model.ClientUser) {
	now := time.Now().UTC()
	for i := range cfg.ClientUsers {
		if equalKey(cfg.ClientUsers[i].Username, user.Username) {
			user.CreatedAt = cfg.ClientUsers[i].CreatedAt
			if user.CreatedAt.IsZero() {
				user.CreatedAt = now
			}
			user.UpdatedAt = now
			cfg.ClientUsers[i] = user
			return
		}
	}
	user.CreatedAt = now
	user.UpdatedAt = now
	cfg.ClientUsers = append(cfg.ClientUsers, user)
}

func upsertMountpoint(cfg *model.AppConfig, mountpoint model.Mountpoint) {
	now := time.Now().UTC()
	for i := range cfg.Mountpoints {
		if equalKey(cfg.Mountpoints[i].Name, mountpoint.Name) {
			mountpoint.CreatedAt = cfg.Mountpoints[i].CreatedAt
			if mountpoint.CreatedAt.IsZero() {
				mountpoint.CreatedAt = now
			}
			mountpoint.UpdatedAt = now
			cfg.Mountpoints[i] = mountpoint
			return
		}
	}
	mountpoint.CreatedAt = now
	mountpoint.UpdatedAt = now
	cfg.Mountpoints = append(cfg.Mountpoints, mountpoint)
}

func upsertBlockedIPRule(cfg *model.AppConfig, rule model.BlockedIPRule) {
	now := time.Now().UTC()
	for i := range cfg.BlockedIPRules {
		if equalKey(cfg.BlockedIPRules[i].Value, rule.Value) && equalKey(cfg.BlockedIPRules[i].Kind, rule.Kind) {
			rule.CreatedAt = cfg.BlockedIPRules[i].CreatedAt
			if rule.CreatedAt.IsZero() {
				rule.CreatedAt = now
			}
			rule.UpdatedAt = now
			cfg.BlockedIPRules[i] = rule
			return
		}
	}
	rule.CreatedAt = now
	rule.UpdatedAt = now
	cfg.BlockedIPRules = append(cfg.BlockedIPRules, rule)
}

func normalizeList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func equalKey(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

func normalizeKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
