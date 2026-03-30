package model

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const SchemaVersion = 1

var (
	ErrDuplicateEntry = errors.New("model: duplicate entry")
	ErrInvalidState   = errors.New("model: invalid state")
)

type AppConfig struct {
	SchemaVersion  int             `json:"schema_version"`
	Auth           AuthSettings    `json:"auth"`
	AdminUsers     []AdminUser     `json:"admin_users,omitempty"`
	SourceUsers    []SourceUser    `json:"source_users,omitempty"`
	ClientUsers    []ClientUser    `json:"client_users,omitempty"`
	Mountpoints    []Mountpoint    `json:"mountpoints,omitempty"`
	Relays         []Relay         `json:"relays,omitempty"`
	BlockedIPRules []BlockedIPRule `json:"blocked_ip_rules,omitempty"`
	RuntimeLimits  RuntimeLimits   `json:"runtime_limits"`
	UpdatedAt      time.Time       `json:"updated_at,omitempty"`
}

func NewAppConfig() AppConfig {
	return AppConfig{SchemaVersion: SchemaVersion}
}

func (c *AppConfig) Normalize() {
	if c == nil {
		return
	}
	if c.SchemaVersion == 0 {
		c.SchemaVersion = SchemaVersion
	}
	c.Auth.Normalize()
	c.AdminUsers = normalizeAdminUsers(c.AdminUsers)
	c.SourceUsers = normalizeSourceUsers(c.SourceUsers)
	c.ClientUsers = normalizeClientUsers(c.ClientUsers)
	c.Mountpoints = normalizeMountpoints(c.Mountpoints)
	c.Relays = normalizeRelays(c.Relays)
	c.BlockedIPRules = normalizeBlockedIPRules(c.BlockedIPRules)
	c.RuntimeLimits.Normalize()
}

func (c *AppConfig) Validate() error {
	if c == nil {
		return ErrInvalidState
	}
	if c.SchemaVersion <= 0 {
		return fmt.Errorf("%w: schema version must be positive", ErrInvalidState)
	}
	if err := validateAdminUsers(c.AdminUsers); err != nil {
		return err
	}
	if err := validateSourceUsers(c.SourceUsers); err != nil {
		return err
	}
	if err := validateClientUsers(c.ClientUsers); err != nil {
		return err
	}
	if err := validateMountpoints(c.Mountpoints); err != nil {
		return err
	}
	if err := validateRelays(c.Relays); err != nil {
		return err
	}
	if err := validateBlockedIPRules(c.BlockedIPRules); err != nil {
		return err
	}
	return c.RuntimeLimits.Validate()
}

func (c *AppConfig) Clone() AppConfig {
	if c == nil {
		return AppConfig{SchemaVersion: SchemaVersion}
	}
	out := *c
	out.Auth = c.Auth.Clone()
	out.AdminUsers = cloneAdminUsers(c.AdminUsers)
	out.SourceUsers = cloneSourceUsers(c.SourceUsers)
	out.ClientUsers = cloneClientUsers(c.ClientUsers)
	out.Mountpoints = cloneMountpoints(c.Mountpoints)
	out.Relays = cloneRelays(c.Relays)
	out.BlockedIPRules = cloneBlockedIPRules(c.BlockedIPRules)
	return out
}

type AdminUser struct {
	Username              string    `json:"username"`
	PasswordHash          string    `json:"password_hash"`
	DisplayName           string    `json:"display_name,omitempty"`
	Enabled               bool      `json:"enabled"`
	RequirePasswordChange bool      `json:"require_password_change,omitempty"`
	CreatedAt             time.Time `json:"created_at,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
}

type AuthSettings struct {
	Initialized  bool             `json:"initialized"`
	LocalEnabled bool             `json:"local_enabled"`
	OIDC         OIDCAuthSettings `json:"oidc"`
}

func (a *AuthSettings) Normalize() {
	if a == nil {
		return
	}
	a.OIDC.Normalize()
}

func (a AuthSettings) Clone() AuthSettings {
	out := a
	out.OIDC = a.OIDC.Clone()
	return out
}

type OIDCAuthSettings struct {
	Enabled        bool     `json:"enabled"`
	Provider       string   `json:"provider"`
	IssuerURL      string   `json:"issuer_url"`
	ClientID       string   `json:"client_id"`
	ClientSecret   string   `json:"client_secret"`
	RedirectURL    string   `json:"redirect_url"`
	Scopes         []string `json:"scopes,omitempty"`
	AllowedEmails  []string `json:"allowed_emails,omitempty"`
	AllowedDomains []string `json:"allowed_domains,omitempty"`
}

func (o *OIDCAuthSettings) Normalize() {
	if o == nil {
		return
	}
	o.Provider = strings.TrimSpace(o.Provider)
	o.IssuerURL = strings.TrimSpace(o.IssuerURL)
	o.ClientID = strings.TrimSpace(o.ClientID)
	o.ClientSecret = strings.TrimSpace(o.ClientSecret)
	o.RedirectURL = strings.TrimSpace(o.RedirectURL)
	o.Scopes = normalizeStringList(o.Scopes)
	o.AllowedEmails = normalizeStringList(o.AllowedEmails)
	o.AllowedDomains = normalizeStringList(o.AllowedDomains)
}

func (o OIDCAuthSettings) Clone() OIDCAuthSettings {
	out := o
	out.Scopes = append([]string(nil), o.Scopes...)
	out.AllowedEmails = append([]string(nil), o.AllowedEmails...)
	out.AllowedDomains = append([]string(nil), o.AllowedDomains...)
	return out
}

type SourceUser struct {
	Username           string            `json:"username"`
	PasswordHash       string            `json:"password_hash"`
	DisplayName        string            `json:"display_name,omitempty"`
	Description        string            `json:"description,omitempty"`
	Enabled            bool              `json:"enabled"`
	AllowedMountpoints []string          `json:"allowed_mountpoints,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	CreatedAt          time.Time         `json:"created_at,omitempty"`
	UpdatedAt          time.Time         `json:"updated_at,omitempty"`
}

type ClientUser struct {
	Username           string            `json:"username"`
	PasswordHash       string            `json:"password_hash"`
	DisplayName        string            `json:"display_name,omitempty"`
	Description        string            `json:"description,omitempty"`
	Enabled            bool              `json:"enabled"`
	AllowedMountpoints []string          `json:"allowed_mountpoints,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	CreatedAt          time.Time         `json:"created_at,omitempty"`
	UpdatedAt          time.Time         `json:"updated_at,omitempty"`
}

type Mountpoint struct {
	Name                    string    `json:"name"`
	Description             string    `json:"description,omitempty"`
	Enabled                 bool      `json:"enabled"`
	SourceUsername          string    `json:"source_username,omitempty"`
	AllowedSourceUsers      []string  `json:"allowed_source_users,omitempty"`
	AllowedClientUsers      []string  `json:"allowed_client_users,omitempty"`
	SupportedConstellations []string  `json:"supported_constellations,omitempty"`
	RTCMMessages            []string  `json:"rtcm_messages,omitempty"`
	Position                *GeoPoint `json:"position,omitempty"`
	DataRateBps             int64     `json:"data_rate_bps,omitempty"`
	DecodeCandidate         bool      `json:"decode_candidate"`
	LastSeenAt              time.Time `json:"last_seen_at,omitempty"`
	CreatedAt               time.Time `json:"created_at,omitempty"`
	UpdatedAt               time.Time `json:"updated_at,omitempty"`
}

type Relay struct {
	Name               string         `json:"name"`
	Description        string         `json:"description,omitempty"`
	Enabled            bool           `json:"enabled"`
	LocalMount         string         `json:"local_mount"`
	UpstreamHost       string         `json:"upstream_host"`
	UpstreamPort       int            `json:"upstream_port"`
	UpstreamMount      string         `json:"upstream_mount"`
	Username           string         `json:"username,omitempty"`
	Password           string         `json:"password,omitempty"`
	AccountPool        []RelayAccount `json:"account_pool,omitempty"`
	NTRIPVersion       int            `json:"ntrip_version,omitempty"`
	GGASentence        string         `json:"gga_sentence,omitempty"`
	GGAIntervalSeconds int            `json:"gga_interval_seconds,omitempty"`
	ClusterRadiusKM    float64        `json:"cluster_radius_km,omitempty"`
	ClusterSlots       int            `json:"cluster_slots,omitempty"`
	CreatedAt          time.Time      `json:"created_at,omitempty"`
	UpdatedAt          time.Time      `json:"updated_at,omitempty"`
}

type RelayAccount struct {
	Name      string    `json:"name"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	Enabled   bool      `json:"enabled"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

type GeoPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type BlockedIPRule struct {
	Value     string    `json:"value"`
	Kind      string    `json:"kind"`
	Reason    string    `json:"reason,omitempty"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type RuntimeLimits struct {
	MaxAdmins             int `json:"max_admins"`
	MaxSourceUsers        int `json:"max_source_users"`
	MaxClientUsers        int `json:"max_client_users"`
	MaxClients            int `json:"max_clients"`
	MaxSources            int `json:"max_sources"`
	MaxMountpoints        int `json:"max_mountpoints"`
	MaxBlockedIPRules     int `json:"max_blocked_ip_rules"`
	MaxPendingConnections int `json:"max_pending_connections"`
}

func (r *RuntimeLimits) Normalize() {
	if r == nil {
		return
	}
	if r.MaxAdmins < 0 {
		r.MaxAdmins = 0
	}
	if r.MaxSourceUsers < 0 {
		r.MaxSourceUsers = 0
	}
	if r.MaxClientUsers < 0 {
		r.MaxClientUsers = 0
	}
	if r.MaxClients < 0 {
		r.MaxClients = 0
	}
	if r.MaxSources < 0 {
		r.MaxSources = 0
	}
	if r.MaxMountpoints < 0 {
		r.MaxMountpoints = 0
	}
	if r.MaxBlockedIPRules < 0 {
		r.MaxBlockedIPRules = 0
	}
	if r.MaxPendingConnections < 0 {
		r.MaxPendingConnections = 0
	}
}

func (r RuntimeLimits) Validate() error {
	if r.MaxAdmins < 0 || r.MaxSourceUsers < 0 || r.MaxClientUsers < 0 || r.MaxClients < 0 || r.MaxSources < 0 || r.MaxMountpoints < 0 || r.MaxBlockedIPRules < 0 || r.MaxPendingConnections < 0 {
		return fmt.Errorf("%w: runtime limits cannot be negative", ErrInvalidState)
	}
	return nil
}

func validateAdminUsers(users []AdminUser) error {
	return validateUnique(users, func(u AdminUser) string { return normalizeKey(u.Username) })
}

func validateSourceUsers(users []SourceUser) error {
	return validateUnique(users, func(u SourceUser) string { return normalizeKey(u.Username) })
}

func validateClientUsers(users []ClientUser) error {
	return validateUnique(users, func(u ClientUser) string { return normalizeKey(u.Username) })
}

func validateMountpoints(items []Mountpoint) error {
	return validateUnique(items, func(m Mountpoint) string { return normalizeKey(m.Name) })
}

func validateRelays(items []Relay) error {
	if err := validateUnique(items, func(r Relay) string { return normalizeKey(r.Name) }); err != nil {
		return err
	}
	for _, item := range items {
		if err := validateUnique(item.AccountPool, func(account RelayAccount) string { return normalizeKey(account.Name) }); err != nil {
			return err
		}
	}
	return nil
}

func validateBlockedIPRules(items []BlockedIPRule) error {
	return validateUnique(items, func(r BlockedIPRule) string { return normalizeKey(r.Value + "|" + r.Kind) })
}

func validateUnique[T any](items []T, keyFn func(T) string) error {
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := keyFn(item)
		if key == "" {
			return fmt.Errorf("%w: empty key", ErrInvalidState)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%w: duplicate key %q", ErrDuplicateEntry, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func normalizeKey(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func normalizeAdminUsers(users []AdminUser) []AdminUser {
	out := make([]AdminUser, 0, len(users))
	seen := make(map[string]struct{}, len(users))
	for _, user := range users {
		user.Username = strings.TrimSpace(user.Username)
		user.DisplayName = strings.TrimSpace(user.DisplayName)
		key := normalizeKey(user.Username)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, user)
	}
	return out
}

func normalizeSourceUsers(users []SourceUser) []SourceUser {
	out := make([]SourceUser, 0, len(users))
	seen := make(map[string]struct{}, len(users))
	for _, user := range users {
		user.Username = strings.TrimSpace(user.Username)
		user.DisplayName = strings.TrimSpace(user.DisplayName)
		user.Description = strings.TrimSpace(user.Description)
		user.AllowedMountpoints = normalizeStringList(user.AllowedMountpoints)
		key := normalizeKey(user.Username)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, user)
	}
	return out
}

func normalizeClientUsers(users []ClientUser) []ClientUser {
	out := make([]ClientUser, 0, len(users))
	seen := make(map[string]struct{}, len(users))
	for _, user := range users {
		user.Username = strings.TrimSpace(user.Username)
		user.DisplayName = strings.TrimSpace(user.DisplayName)
		user.Description = strings.TrimSpace(user.Description)
		user.AllowedMountpoints = normalizeStringList(user.AllowedMountpoints)
		key := normalizeKey(user.Username)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, user)
	}
	return out
}

func normalizeMountpoints(items []Mountpoint) []Mountpoint {
	out := make([]Mountpoint, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item.Name = strings.TrimSpace(item.Name)
		item.Description = strings.TrimSpace(item.Description)
		item.SourceUsername = strings.TrimSpace(item.SourceUsername)
		item.AllowedSourceUsers = normalizeStringList(item.AllowedSourceUsers)
		item.AllowedClientUsers = normalizeStringList(item.AllowedClientUsers)
		item.SupportedConstellations = normalizeStringList(item.SupportedConstellations)
		item.RTCMMessages = normalizeStringList(item.RTCMMessages)
		key := normalizeKey(item.Name)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func normalizeRelays(items []Relay) []Relay {
	out := make([]Relay, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item.Name = strings.TrimSpace(item.Name)
		item.Description = strings.TrimSpace(item.Description)
		item.LocalMount = strings.TrimSpace(item.LocalMount)
		item.UpstreamHost = strings.TrimSpace(item.UpstreamHost)
		item.UpstreamMount = strings.TrimSpace(item.UpstreamMount)
		item.Username = strings.TrimSpace(item.Username)
		item.Password = strings.TrimSpace(item.Password)
		item.AccountPool = normalizeRelayAccounts(item.AccountPool)
		item.GGASentence = strings.TrimSpace(item.GGASentence)
		if item.UpstreamPort < 0 {
			item.UpstreamPort = 0
		}
		if item.NTRIPVersion != 2 {
			item.NTRIPVersion = 1
		}
		if item.GGAIntervalSeconds < 0 {
			item.GGAIntervalSeconds = 0
		}
		if item.ClusterRadiusKM <= 0 {
			item.ClusterRadiusKM = 30
		}
		if item.ClusterSlots <= 0 {
			item.ClusterSlots = 2
		}
		key := normalizeKey(item.Name)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func normalizeRelayAccounts(items []RelayAccount) []RelayAccount {
	out := make([]RelayAccount, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item.Name = strings.TrimSpace(item.Name)
		item.Username = strings.TrimSpace(item.Username)
		item.Password = strings.TrimSpace(item.Password)
		key := normalizeKey(item.Name)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func normalizeBlockedIPRules(items []BlockedIPRule) []BlockedIPRule {
	out := make([]BlockedIPRule, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item.Value = strings.TrimSpace(item.Value)
		item.Kind = strings.TrimSpace(item.Kind)
		item.Reason = strings.TrimSpace(item.Reason)
		key := normalizeKey(item.Value + "|" + item.Kind)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func normalizeStringList(values []string) []string {
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

func cloneAdminUsers(users []AdminUser) []AdminUser {
	out := make([]AdminUser, len(users))
	copy(out, users)
	return out
}

func cloneSourceUsers(users []SourceUser) []SourceUser {
	out := make([]SourceUser, len(users))
	copy(out, users)
	for i := range out {
		out[i].AllowedMountpoints = append([]string(nil), out[i].AllowedMountpoints...)
		if out[i].Metadata != nil {
			meta := make(map[string]string, len(out[i].Metadata))
			for k, v := range out[i].Metadata {
				meta[k] = v
			}
			out[i].Metadata = meta
		}
	}
	return out
}

func cloneClientUsers(users []ClientUser) []ClientUser {
	out := make([]ClientUser, len(users))
	copy(out, users)
	for i := range out {
		out[i].AllowedMountpoints = append([]string(nil), out[i].AllowedMountpoints...)
		if out[i].Metadata != nil {
			meta := make(map[string]string, len(out[i].Metadata))
			for k, v := range out[i].Metadata {
				meta[k] = v
			}
			out[i].Metadata = meta
		}
	}
	return out
}

func cloneMountpoints(items []Mountpoint) []Mountpoint {
	out := make([]Mountpoint, len(items))
	copy(out, items)
	for i := range out {
		out[i].AllowedSourceUsers = append([]string(nil), out[i].AllowedSourceUsers...)
		out[i].AllowedClientUsers = append([]string(nil), out[i].AllowedClientUsers...)
		out[i].SupportedConstellations = append([]string(nil), out[i].SupportedConstellations...)
		out[i].RTCMMessages = append([]string(nil), out[i].RTCMMessages...)
		if out[i].Position != nil {
			pos := *out[i].Position
			out[i].Position = &pos
		}
	}
	return out
}

func cloneRelays(items []Relay) []Relay {
	out := make([]Relay, len(items))
	copy(out, items)
	for i := range out {
		out[i].AccountPool = append([]RelayAccount(nil), out[i].AccountPool...)
	}
	return out
}

func cloneBlockedIPRules(items []BlockedIPRule) []BlockedIPRule {
	out := make([]BlockedIPRule, len(items))
	copy(out, items)
	return out
}
