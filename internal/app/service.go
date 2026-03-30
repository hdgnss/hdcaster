package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hdcaster/internal/model"
	"hdcaster/internal/relay"
	"hdcaster/internal/rtcm"
	"hdcaster/internal/runtime"
	"hdcaster/internal/security"
	"hdcaster/internal/storage"
)

var (
	ErrUnauthorized = errors.New("unauthorized")
	ErrForbiddenIP  = errors.New("blocked ip")
	ErrNotFound     = errors.New("not found")
	ErrRateLimited  = errors.New("rate limited")
)

const (
	adminSessionIdleTTL   = 12 * time.Hour
	loginRateWindow       = 10 * time.Minute
	loginRateLockDuration = 15 * time.Minute
	loginRateMaxFailures  = 5
)

type Service struct {
	store         storage.Store
	hub           *runtime.Hub
	relayManager  *relay.Manager
	mu            sync.RWMutex
	sessions      map[string]session
	loginAttempts map[string]loginAttempt
	oidcFlow      map[string]oidcFlowState
	bootstrapAuth AuthConfig
	startedAt     time.Time
	ntripReady    atomic.Bool
}

type session struct {
	Username              string
	Display               string
	Email                 string
	AuthMethod            string
	RequirePasswordChange bool
	Expiry                time.Time
}

type loginAttempt struct {
	Failures    int
	WindowStart time.Time
	LockedUntil time.Time
}

type PageResult struct {
	Items    []map[string]any `json:"items"`
	Total    int              `json:"total"`
	Page     int              `json:"page"`
	PageSize int              `json:"page_size"`
}

func Open(statePath string, authCfg AuthConfig) (*Service, error) {
	store := storage.OpenStore(statePath)
	svc := &Service{
		store:         store,
		sessions:      make(map[string]session),
		loginAttempts: make(map[string]loginAttempt),
		oidcFlow:      make(map[string]oidcFlowState),
		bootstrapAuth: authCfg,
		startedAt:     time.Now().UTC(),
	}
	svc.hub = runtime.NewHub(svc)
	if err := svc.ensureBootstrapState(); err != nil {
		return nil, err
	}
	return svc, nil
}

func DefaultStatePath() string {
	if root, err := os.UserConfigDir(); err == nil {
		return filepath.Join(root, "hdcaster", "state.db")
	}
	return "hdcaster-state.db"
}

func (s *Service) StartRelayManager(logger *log.Logger) {
	s.relayManager = relay.NewManager(s.hub, logger).OnAudit(func(event storage.AuditEvent) {
		s.AppendAuditEvent(event)
	})
	s.syncRelayManager()
}

func (s *Service) StartedAt() time.Time {
	return s.startedAt
}

func (s *Service) SetNTRIPReady(ready bool) {
	s.ntripReady.Store(ready)
}

func (s *Service) IsNTRIPReady() bool {
	return s.ntripReady.Load()
}

func (s *Service) CheckStore(ctx context.Context) error {
	_, err := s.store.Load(ctx)
	return err
}

func (s *Service) SessionUsername(token string) string {
	if token == "" {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[token]
	if !ok || time.Now().After(sess.Expiry) {
		return ""
	}
	return sess.Username
}

func (s *Service) AppendAuditEvent(event storage.AuditEvent) {
	if auditStore, ok := s.store.(storage.AuditCapable); ok {
		event.At = maxTime(event.At, time.Now().UTC())
		if event.Status == "" {
			event.Status = "ok"
		}
		_ = auditStore.AppendAuditEvents(context.Background(), []storage.AuditEvent{event})
	}
}

func (s *Service) AuditEvents(limit int) []map[string]any {
	auditStore, ok := s.store.(storage.AuditCapable)
	if !ok {
		return nil
	}
	items, err := auditStore.ListAuditEvents(context.Background(), limit)
	if err != nil {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"id":         item.ID,
			"at":         item.At,
			"actor":      item.Actor,
			"action":     item.Action,
			"resource":   item.Resource,
			"resourceId": item.ResourceID,
			"status":     item.Status,
			"remoteAddr": item.RemoteAddr,
			"message":    item.Message,
			"details":    item.Details,
		})
	}
	return out
}

func (s *Service) MaxSources() int {
	cfg := s.loadConfig()
	return cfg.RuntimeLimits.MaxSources
}

func (s *Service) MaxClients() int {
	cfg := s.loadConfig()
	return cfg.RuntimeLimits.MaxClients
}

func (s *Service) syncRelayManager() {
	if s.relayManager == nil {
		return
	}
	cfg := s.loadConfig()
	s.relayManager.Sync(cfg.Relays, cfg.Mountpoints)
}

func (s *Service) HasRelayMount(mount string) bool {
	if s.relayManager == nil {
		return false
	}
	return s.relayManager.HasRelayMount(mount)
}

func (s *Service) AttachRelayClient(mount, username, remoteAddr string) (*relay.ClientHandle, <-chan []byte, error) {
	if s.relayManager == nil {
		return nil, nil, ErrNotFound
	}
	return s.relayManager.AttachClient(mount, username, remoteAddr)
}

func (s *Service) UpdateRelayClientGGA(clientID uint64, sentence string) {
	if s.relayManager == nil {
		return
	}
	s.relayManager.UpdateClientGGA(clientID, sentence)
}

func (s *Service) Overview() map[string]any {
	snap := s.hub.Snapshot()
	relaySources := s.relaySourceSnapshots()
	decodeCandidates := 0
	var throughput uint64
	for _, src := range snap.OnlineSources {
		if len(src.RTCM.MessageTypes) > 0 {
			decodeCandidates++
		}
		throughput += src.BytesIn
	}
	for _, src := range relaySources {
		if len(src.RTCM.MessageTypes) > 0 {
			decodeCandidates++
		}
		throughput += src.BytesIn
	}
	relayStatuses := s.Relays()
	activeRelays := 0
	errorRelays := 0
	for _, relayStatus := range relayStatuses {
		state := toString(relayStatus["state"])
		if state == "online" || state == "connecting" {
			activeRelays++
		}
		if state == "error" || state == "mount_disabled" {
			errorRelays++
		}
	}
	return map[string]any{
		"activeSources":    snap.TotalSources + len(relaySources),
		"activeMounts":     len(uniqueMountNames(snap.OnlineSources, relaySources)),
		"connectedClients": snap.TotalClients + relayClientCount(relaySources),
		"blockedIPs":       len(s.loadConfig().BlockedIPRules),
		"throughputKbps":   throughput / 1024,
		"decodeCandidates": decodeCandidates,
		"activeRelays":     activeRelays,
		"relayErrors":      errorRelays,
	}
}

func (s *Service) OnlineSources() []map[string]any {
	sources := s.hub.Snapshot().OnlineSources
	relaySources := s.relaySourceSnapshots()
	out := make([]map[string]any, 0, len(sources)+len(relaySources))
	for _, src := range sources {
		positionText := ""
		if src.RTCM.StationGeo != nil {
			positionText = formatRuntimeGeoPoint(src.RTCM.StationGeo)
		}
		out = append(out, map[string]any{
			"id":              src.ID,
			"mountpoint":      src.Mount,
			"host":            src.RemoteAddr,
			"onlineSince":     src.ConnectedAt,
			"encoding":        "RTCM 3",
			"galaxies":        src.RTCM.Constellations,
			"position":        positionText,
			"decodedPosition": src.RTCM.StationGeo,
			"messages":        intsToStrings(src.RTCM.MessageTypes),
			"bitrateKbps":     src.BytesIn / 1024,
			"status":          "online",
			"clients":         src.ClientCount,
			"username":        src.Username,
			"bytesIn":         src.BytesIn,
			"bytesOut":        src.BytesOut,
		})
	}
	for _, src := range relaySources {
		positionText := ""
		if src.RTCM.StationGeo != nil {
			positionText = formatRuntimeGeoPoint(src.RTCM.StationGeo)
		}
		out = append(out, map[string]any{
			"id":              src.ID,
			"mountpoint":      src.LocalMount,
			"host":            src.RemoteAddr,
			"onlineSince":     src.ConnectedAt,
			"encoding":        "RTCM 3",
			"galaxies":        src.RTCM.Constellations,
			"position":        positionText,
			"decodedPosition": src.RTCM.StationGeo,
			"messages":        intsToStrings(src.RTCM.MessageTypes),
			"bitrateKbps":     src.BytesIn / 1024,
			"status":          "online",
			"clients":         src.ClientCount,
			"username":        src.Username,
			"bytesIn":         src.BytesIn,
			"bytesOut":        src.BytesOut,
		})
	}
	return out
}

func (s *Service) CheckBlocked(remoteAddr string) bool {
	host := remoteAddr
	if addr, err := netip.ParseAddrPort(remoteAddr); err == nil {
		host = addr.Addr().String()
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	for _, rule := range s.loadConfig().BlockedIPRules {
		if !rule.Enabled {
			continue
		}
		switch strings.ToLower(rule.Kind) {
		case "cidr":
			if prefix, err := netip.ParsePrefix(rule.Value); err == nil && prefix.Contains(ip) {
				return true
			}
		default:
			if addr, err := netip.ParseAddr(rule.Value); err == nil && addr == ip {
				return true
			}
		}
	}
	return false
}

func (s *Service) AuthenticateSource(remoteAddr, username, password, secret, mount string) error {
	if s.CheckBlocked(remoteAddr) {
		return ErrForbiddenIP
	}
	
	if username != "" {
		if user, err := s.store.GetSourceUser(context.Background(), username); err == nil && user.Enabled {
			if security.CheckPassword(user.PasswordHash, password) && mountAllowed(user.AllowedMountpoints, mount) {
				return nil
			}
		}
	}
	
	if secret != "" {
		users, err := s.store.ListSourceUsers(context.Background())
		if err != nil {
			return err
		}
		for _, user := range users {
			if user.Enabled && security.CheckPassword(user.PasswordHash, secret) && mountAllowed(user.AllowedMountpoints, mount) {
				return nil
			}
		}
	}

	return ErrUnauthorized
}

func (s *Service) ResolveSourceUsername(username, password, secret, mount string) string {
	users, err := s.store.ListSourceUsers(context.Background())
	if err != nil {
		return firstNonEmpty(username, "source")
	}
	for _, user := range users {
		if !user.Enabled || !mountAllowed(user.AllowedMountpoints, mount) {
			continue
		}
		if username != "" && strings.EqualFold(user.Username, username) && security.CheckPassword(user.PasswordHash, password) {
			return user.Username
		}
		if secret != "" && security.CheckPassword(user.PasswordHash, secret) {
			return user.Username
		}
	}
	return firstNonEmpty(username, "source")
}

func (s *Service) AuthenticateClient(remoteAddr, username, password, mount string) error {
	if s.CheckBlocked(remoteAddr) {
		return ErrForbiddenIP
	}
	
	user, err := s.store.GetClientUser(context.Background(), username)
	if err != nil {
		return ErrUnauthorized
	}
	
	if user.Enabled && security.CheckPassword(user.PasswordHash, password) && mountAllowed(user.AllowedMountpoints, mount) {
		return nil
	}
	
	return ErrUnauthorized
}

func mountAllowed(allowed []string, mount string) bool {
	for _, candidate := range allowed {
		if candidate == "*" || strings.EqualFold(candidate, mount) {
			return true
		}
	}
	return false
}

func (s *Service) Hub() *runtime.Hub {
	return s.hub
}

func (s *Service) Sourcetable() string {
	cfg := s.loadConfig()
	online := map[string]bool{}
	for _, src := range s.hub.Snapshot().OnlineSources {
		online[src.Mount] = true
	}
	for _, src := range s.relaySourceSnapshots() {
		online[src.LocalMount] = true
	}
	
	var sb strings.Builder
	for _, mp := range cfg.Mountpoints {
		if !mp.Enabled {
			continue
		}
		positionLat := "0.0000"
		positionLon := "0.0000"
		if mp.Position != nil {
			positionLat = formatFloat(mp.Position.Latitude)
			positionLon = formatFloat(mp.Position.Longitude)
		}
		onlineFlag := "0"
		if online[mp.Name] {
			onlineFlag = "1"
		}
		
		sb.WriteString(strings.Join([]string{
			"STR",
			mp.Name,
			mp.Name,
			"RTCM 3",
			"",
			strings.Join(mp.SupportedConstellations, "+"),
			"2",
			"0",
			"B",
			"N",
			positionLat,
			positionLon,
			"0",
			"",
			"",
			"none",
			"hdcaster",
			"none",
			onlineFlag,
			formatInt(int(mp.DataRateBps / 8)),
		}, ";"))
		sb.WriteString("\r\n")
	}
	sb.WriteString("ENDSOURCETABLE\r\n")
	return sb.String()
}

func (s *Service) Mounts() []map[string]any {
	cfg := s.loadConfig()
	online := map[string]runtime.SourceSnapshot{}
	for _, src := range s.hub.Snapshot().OnlineSources {
		online[src.Mount] = src
	}
	relayRuntimeByMount := s.relayRuntimeByMount()
	out := make([]map[string]any, 0, len(cfg.Mountpoints))
	for _, mp := range cfg.Mountpoints {
		entry := map[string]any{
			"id":            mp.Name,
			"name":          mp.Name,
			"description":   mp.Description,
			"region":        mountRegion(mp),
			"status":        "idle",
			"enabled":       mp.Enabled,
			"clients":       0,
			"sourceId":      nil,
			"currentSource": "",
		}
		if src, ok := online[mp.Name]; ok {
			entry["status"] = "online"
			entry["clients"] = src.ClientCount
			entry["sourceId"] = src.ID
			entry["currentSource"] = src.Username
			entry["region"] = runtimeMountRegion(mp, src)
		} else if relayRuntime, ok := relayRuntimeByMount[normalizeLookupKey(mp.Name)]; ok {
			entry["status"] = "online"
			entry["clients"] = toInt(relayRuntime["clientCount"])
			entry["sourceId"] = relayRuntime["id"]
			entry["currentSource"] = toString(relayRuntime["username"])
			if decodedPosition, ok := relayRuntime["decodedPosition"].(*rtcm.GeoPoint); ok && decodedPosition != nil {
				entry["region"] = formatRuntimeGeoPoint(decodedPosition)
			}
		}
		out = append(out, entry)
	}
	return out
}

func (s *Service) MountsPage(query string, page, pageSize int) PageResult {
	items := s.Mounts()
	filtered := filterMaps(items, query, func(item map[string]any) string {
		return strings.Join([]string{toString(item["id"]), toString(item["name"])}, " ")
	})
	return paginateMaps(filtered, page, pageSize)
}

func (s *Service) Relays() []map[string]any {
	cfg := s.loadConfig()
	statusByName := make(map[string]relay.Status)
	if s.relayManager != nil {
		for _, status := range s.relayManager.Snapshot() {
			statusByName[strings.ToLower(strings.TrimSpace(status.Name))] = status
		}
	}
	out := make([]map[string]any, 0, len(cfg.Relays))
	for _, item := range cfg.Relays {
		status, ok := statusByName[strings.ToLower(strings.TrimSpace(item.Name))]
		if !ok {
			status = relay.Status{
				Name:             item.Name,
				LocalMount:       item.LocalMount,
				Upstream:         relayUpstream(item),
				Enabled:          item.Enabled,
				State:            relayStateForConfig(item, cfg.Mountpoints),
				StaticGGAEnabled: strings.TrimSpace(item.GGASentence) != "",
				UpstreamVersion:  relayVersion(item),
			}
		}
		out = append(out, map[string]any{
			"id":                 item.Name,
			"name":               item.Name,
			"description":        item.Description,
			"enabled":            item.Enabled,
			"localMount":         item.LocalMount,
			"upstreamHost":       item.UpstreamHost,
			"upstreamPort":       item.UpstreamPort,
			"upstreamMount":      item.UpstreamMount,
			"username":           item.Username,
			"accountPool":        item.AccountPool,
			"ntripVersion":       relayVersion(item),
			"ggaSentence":        item.GGASentence,
			"ggaIntervalSeconds": item.GGAIntervalSeconds,
			"clusterRadiusKm":    item.ClusterRadiusKM,
			"clusterSlots":       item.ClusterSlots,
			"state":              status.State,
			"lastError":          status.LastError,
			"lastFailureReason":  status.LastFailureReason,
			"lastRejectReason":   status.LastRejectReason,
			"lastRejectAt":       nilIfZero(status.LastRejectAt),
			"lastConnectAt":      nilIfZero(status.LastConnectAt),
			"lastDisconnectAt":   nilIfZero(status.LastDisconnectAt),
			"lastSuccessfulAt":   nilIfZero(status.LastSuccessfulAt),
			"lastGgaAt":          nilIfZero(status.LastGGAAt),
			"lastRetryAt":        nilIfZero(status.LastRetryAt),
			"nextRetryAt":        nilIfZero(status.NextRetryAt),
			"retryCount":         status.RetryCount,
			"retryDelaySeconds":  status.RetryDelaySeconds,
			"upstream":           status.Upstream,
			"staticGgaEnabled":   status.StaticGGAEnabled,
			"activeSessions":     status.ActiveSessions,
			"activeClients":      status.ActiveClients,
			"poolSize":           status.PoolSize,
			"leasedAccounts":     status.LeasedAccounts,
			"healthyAccounts":    status.HealthyAccounts,
			"unhealthyAccounts":  status.UnhealthyAccounts,
			"poolHealthy":        status.PoolHealthy,
			"accountHealth":      status.AccountHealth,
			"rejectedClients":    status.RejectedClients,
			"recentRejects":      status.RecentRejects,
		})
	}
	return out
}

func (s *Service) RelaysPage(query string, page, pageSize int) PageResult {
	items := s.Relays()
	filtered := filterMaps(items, query, func(item map[string]any) string {
		return strings.Join([]string{
			toString(item["name"]),
			toString(item["localMount"]),
			toString(item["upstreamHost"]),
			toString(item["upstreamMount"]),
			toString(item["state"]),
		}, " ")
	})
	return paginateMaps(filtered, page, pageSize)
}

func (s *Service) MountDetail(name string) (map[string]any, error) {
	cfg := s.loadConfig()
	relayRuntimeByMount := s.relayRuntimeByMount()
	for _, mp := range cfg.Mountpoints {
		if !strings.EqualFold(mp.Name, name) {
			continue
		}
		detail := map[string]any{
			"id":              mp.Name,
			"name":            mp.Name,
			"description":     mp.Description,
			"position":        sanitizeStoredPosition(mp.Position),
			"constellations":  mp.SupportedConstellations,
			"advertisedRtcm":  mp.RTCMMessages,
			"decodeCandidate": mp.DecodeCandidate,
			"sourceUsername":  mp.SourceUsername,
			"allowedSources":  mp.AllowedSourceUsers,
			"allowedClients":  mp.AllowedClientUsers,
			"dataRateBps":     mp.DataRateBps,
			"enabled":         mp.Enabled,
			"relay":           s.relayForMount(mp.Name),
			"runtime":         nil,
		}
		for _, src := range s.hub.Snapshot().OnlineSources {
			if src.Mount == mp.Name {
				detail["runtime"] = map[string]any{
					"id":              src.ID,
					"remoteAddr":      src.RemoteAddr,
					"username":        src.Username,
					"connectedAt":     src.ConnectedAt,
					"lastActive":      src.LastActive,
					"bytesIn":         src.BytesIn,
					"bytesOut":        src.BytesOut,
					"clientCount":     src.ClientCount,
					"messageTypes":    src.RTCM.MessageTypes,
					"messageCounts":   src.RTCM.MessageCounts,
					"constellations":  src.RTCM.Constellations,
					"msmClasses":      src.RTCM.MSMClasses,
					"msmFamilies":     src.RTCM.MSMFamilies,
					"framesObserved":  src.RTCM.FramesObserved,
					"decodedPosition": src.RTCM.StationGeo,
					"decodedECEF":     src.RTCM.StationECEF,
					"reference":       src.RTCM.Reference,
				}
				break
			}
		}
		if detail["runtime"] == nil {
			if relayRuntime, ok := relayRuntimeByMount[normalizeLookupKey(mp.Name)]; ok {
				detail["runtime"] = relayRuntime
			}
		}
		return detail, nil
	}
	return nil, ErrNotFound
}

func (s *Service) Users() []map[string]any {
	cfg := s.loadConfig()
	var out []map[string]any
	for _, user := range cfg.ClientUsers {
		out = append(out, map[string]any{"type": "client", "username": user.Username, "permissions": user.AllowedMountpoints, "status": enabledStatus(user.Enabled)})
	}
	for _, user := range cfg.SourceUsers {
		out = append(out, map[string]any{"type": "source", "username": user.Username, "permissions": user.AllowedMountpoints, "status": enabledStatus(user.Enabled)})
	}
	slices.SortFunc(out, func(a, b map[string]any) int {
		return strings.Compare(a["username"].(string), b["username"].(string))
	})
	return out
}

func (s *Service) UsersPage(query string, page, pageSize int) PageResult {
	items := s.Users()
	filtered := filterMaps(items, query, func(item map[string]any) string {
		return strings.Join([]string{
			toString(item["type"]),
			toString(item["username"]),
			toString(item["status"]),
			strings.Join(toStringSlice(item["permissions"]), ","),
		}, " ")
	})
	return paginateMaps(filtered, page, pageSize)
}

func (s *Service) DeleteUser(kind, username string) error {
	switch kind {
	case "client":
		if err := s.store.DeleteClientUser(context.Background(), username); err != nil {
			return ErrNotFound
		}
		s.hub.DisconnectClientUser(username)
		if s.relayManager != nil {
			s.relayManager.DisconnectClientUser(username)
		}
		return nil
	case "source":
		if err := s.store.DeleteSourceUser(context.Background(), username); err != nil {
			return ErrNotFound
		}
		s.hub.DisconnectSourceUser(username)
		return nil
	default:
		return ErrNotFound
	}
}

func (s *Service) SetUserEnabled(kind, username string, enabled bool) error {
	now := time.Now().UTC()
	switch kind {
	case "client":
		user, err := s.store.GetClientUser(context.Background(), username)
		if err != nil {
			return ErrNotFound
		}
		user.Enabled = enabled
		user.UpdatedAt = now
		if err := s.store.UpsertClientUser(context.Background(), user); err != nil {
			return err
		}
		if !enabled {
			s.hub.DisconnectClientUser(username)
			if s.relayManager != nil {
				s.relayManager.DisconnectClientUser(username)
			}
		}
		return nil
	case "source":
		user, err := s.store.GetSourceUser(context.Background(), username)
		if err != nil {
			return ErrNotFound
		}
		user.Enabled = enabled
		user.UpdatedAt = now
		if err := s.store.UpsertSourceUser(context.Background(), user); err != nil {
			return err
		}
		if !enabled {
			s.hub.DisconnectSourceUser(username)
		}
		return nil
	default:
		return ErrNotFound
	}
}

func enabledStatus(v bool) string {
	if v {
		return "active"
	}
	return "disabled"
}

func (s *Service) UpsertUser(kind, username, password string, permissions []string) error {
	now := time.Now().UTC()
	switch kind {
	case "client":
		user, err := s.store.GetClientUser(context.Background(), username)
		exists := err == nil
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if !exists {
			user = model.ClientUser{
				Username:  username,
				Enabled:   true,
				CreatedAt: now,
			}
		}
		if password != "" {
			hash, err := security.HashPassword(password)
			if err != nil {
				return err
			}
			user.PasswordHash = hash
		} else if !exists {
			return errors.New("password is required for new user")
		}
		user.Username = username
		user.AllowedMountpoints = permissions
		user.UpdatedAt = now
		return s.store.UpsertClientUser(context.Background(), user)
	case "source":
		user, err := s.store.GetSourceUser(context.Background(), username)
		exists := err == nil
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if !exists {
			user = model.SourceUser{
				Username:  username,
				Enabled:   true,
				CreatedAt: now,
			}
		}
		if password != "" {
			hash, err := security.HashPassword(password)
			if err != nil {
				return err
			}
			user.PasswordHash = hash
		} else if !exists {
			return errors.New("password is required for new user")
		}
		user.Username = username
		user.AllowedMountpoints = permissions
		user.UpdatedAt = now
		return s.store.UpsertSourceUser(context.Background(), user)
	default:
		return ErrNotFound
	}
}

func (s *Service) Blocks() []map[string]any {
	cfg := s.loadConfig()
	out := make([]map[string]any, 0, len(cfg.BlockedIPRules))
	for _, rule := range cfg.BlockedIPRules {
		out = append(out, map[string]any{
			"id":        rule.Value + "|" + rule.Kind,
			"ip":        rule.Value,
			"reason":    rule.Reason,
			"expiresAt": "永久",
		})
	}
	return out
}

func (s *Service) AddBlock(value, reason string) error {
	kind := "ip"
	if strings.Contains(value, "/") {
		kind = "cidr"
	}
	return s.store.UpsertBlockedIPRule(context.Background(), model.BlockedIPRule{
		Value:     value,
		Kind:      kind,
		Reason:    reason,
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	})
}

func (s *Service) DeleteBlock(value string) error {
	kind := "ip"
	if strings.Contains(value, "/") || strings.Contains(value, "%2F") {
		kind = "cidr"
	}
	if err := s.store.DeleteBlockedIPRule(context.Background(), value, kind); err != nil {
		return ErrNotFound
	}
	return nil
}

func (s *Service) Limits() map[string]any {
	limits, _ := s.store.GetRuntimeLimits(context.Background())
	return map[string]any{
		"maxClients":     limits.MaxClients,
		"maxSources":     limits.MaxSources,
		"maxPending":     limits.MaxPendingConnections,
		"maxConnections": limits.MaxClients + limits.MaxSources + limits.MaxPendingConnections,
	}
}

func (s *Service) SetLimits(maxClients, maxSources, maxPending int) error {
	limits, _ := s.store.GetRuntimeLimits(context.Background())
	limits.MaxClients = maxClients
	limits.MaxSources = maxSources
	limits.MaxPendingConnections = maxPending
	return s.store.SetRuntimeLimits(context.Background(), limits)
}

func (s *Service) UpsertMountpoint(mp model.Mountpoint) error {
	mp.Enabled = true
	mp.UpdatedAt = time.Now().UTC()
	if mp.CreatedAt.IsZero() {
		mp.CreatedAt = mp.UpdatedAt
	}
	mp.Position = sanitizeStoredPosition(mp.Position)
	if err := s.store.UpsertMountpoint(context.Background(), mp); err != nil {
		return err
	}
	s.syncRelayManager()
	return nil
}

func (s *Service) DeleteMountpoint(name string) error {
	if err := s.store.DeleteMountpoint(context.Background(), name); err != nil {
		return ErrNotFound
	}
	s.syncRelayManager()
	return nil
}

func (s *Service) SetMountpointEnabled(name string, enabled bool) error {
	mp, err := s.store.GetMountpoint(context.Background(), name)
	if err != nil {
		return ErrNotFound
	}
	mp.Enabled = enabled
	mp.UpdatedAt = time.Now().UTC()
	if err := s.store.UpsertMountpoint(context.Background(), mp); err != nil {
		return err
	}
	s.syncRelayManager()
	return nil
}

func (s *Service) UpsertRelay(item model.Relay) error {
	cfg := s.loadConfig()
	now := time.Now().UTC()

	item.Name = strings.TrimSpace(item.Name)
	item.Description = strings.TrimSpace(item.Description)
	item.LocalMount = strings.TrimSpace(item.LocalMount)
	item.UpstreamHost = strings.TrimSpace(item.UpstreamHost)
	item.UpstreamMount = strings.TrimSpace(item.UpstreamMount)
	item.Username = strings.TrimSpace(item.Username)
	item.Password = strings.TrimSpace(item.Password)
	item.GGASentence = strings.TrimSpace(item.GGASentence)
	item.AccountPool = normalizeRelayAccounts(item.AccountPool)
	if item.Name == "" || item.LocalMount == "" || item.UpstreamHost == "" || item.UpstreamMount == "" {
		return errors.New("relay name, local mount, upstream host and upstream mount are required")
	}
	if item.UpstreamPort <= 0 {
		item.UpstreamPort = 2101
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
	item.UpdatedAt = now

	existing := -1
	for i := range cfg.Relays {
		if strings.EqualFold(cfg.Relays[i].Name, item.Name) {
			existing = i
			item.CreatedAt = cfg.Relays[i].CreatedAt
			if item.Password == "" {
				item.Password = cfg.Relays[i].Password
			}
			if len(item.AccountPool) == 0 {
				item.AccountPool = cfg.Relays[i].AccountPool
			}
			break
		}
		if strings.EqualFold(cfg.Relays[i].LocalMount, item.LocalMount) {
			return errors.New("local mount is already assigned to another relay")
		}
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}

	mountIndex := -1
	for i := range cfg.Mountpoints {
		if strings.EqualFold(cfg.Mountpoints[i].Name, item.LocalMount) {
			mountIndex = i
			break
		}
	}
	if mountIndex == -1 {
		cfg.Mountpoints = append(cfg.Mountpoints, model.Mountpoint{
			Name:        item.LocalMount,
			Description: "Managed by relay " + item.Name,
			Enabled:     true,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	} else if !cfg.Mountpoints[mountIndex].Enabled {
		cfg.Mountpoints[mountIndex].Enabled = true
		cfg.Mountpoints[mountIndex].UpdatedAt = now
	}

	if existing >= 0 {
		cfg.Relays[existing] = item
	} else {
		cfg.Relays = append(cfg.Relays, item)
	}
	if err := s.store.Save(context.Background(), &cfg); err != nil {
		return err
	}
	s.syncRelayManager()
	return nil
}

func (s *Service) DeleteRelay(name string) error {
	cfg := s.loadConfig()
	removed := false
	cfg.Relays, removed = deleteBy(cfg.Relays, func(item model.Relay) bool {
		return strings.EqualFold(item.Name, name)
	})
	if !removed {
		return ErrNotFound
	}
	if err := s.store.Save(context.Background(), &cfg); err != nil {
		return err
	}
	s.syncRelayManager()
	return nil
}

func (s *Service) SetRelayEnabled(name string, enabled bool) error {
	cfg := s.loadConfig()
	for i := range cfg.Relays {
		if !strings.EqualFold(cfg.Relays[i].Name, name) {
			continue
		}
		cfg.Relays[i].Enabled = enabled
		cfg.Relays[i].UpdatedAt = time.Now().UTC()
		if err := s.store.Save(context.Background(), &cfg); err != nil {
			return err
		}
		s.syncRelayManager()
		return nil
	}
	return ErrNotFound
}

func (s *Service) Login(username, password, remoteAddr string) (string, error) {
	if !s.localLoginAvailable() {
		return "", ErrUnauthorized
	}
	now := time.Now().UTC()
	keys := loginThrottleKeys(username, remoteAddr)
	if retryAfter, blocked := s.loginRetryAfter(keys, now); blocked {
		return "", fmt.Errorf("%w: retry after %s", ErrRateLimited, retryAfter.Round(time.Second))
	}
	admin, err := s.store.GetAdminUser(context.Background(), username)
	if err != nil {
		s.recordLoginFailure(keys, now)
		return "", ErrUnauthorized
	}
	if !admin.Enabled || !security.CheckPassword(admin.PasswordHash, password) {
		s.recordLoginFailure(keys, now)
		return "", ErrUnauthorized
	}
	token := newID()
	s.mu.Lock()
	s.pruneSecurityStateLocked(now)
	s.clearLoginFailuresLocked(keys)
	s.sessions[token] = session{
		Username:              admin.Username,
		Display:               admin.DisplayName,
		AuthMethod:            "password",
		RequirePasswordChange: admin.RequirePasswordChange,
		Expiry:                now.Add(adminSessionIdleTTL),
	}
	s.mu.Unlock()
	return token, nil
}

func (s *Service) CheckSession(token string) bool {
	_, ok := s.touchSession(token)
	return ok
}

func (s *Service) SessionInfo(token string) map[string]any {
	sess, ok := s.touchSession(token)
	if !ok {
		return map[string]any{"authenticated": false}
	}
	remaining := time.Until(sess.Expiry)
	if remaining < 0 {
		remaining = 0
	}
	return map[string]any{
		"authenticated":      true,
		"expiresAt":          sess.Expiry,
		"idleTimeoutSeconds": int(adminSessionIdleTTL.Seconds()),
		"remainingSeconds":   int(remaining.Seconds()),
		"user": map[string]any{
			"username":              sess.Username,
			"display":               sess.Display,
			"email":                 sess.Email,
			"authMethod":            sess.AuthMethod,
			"requirePasswordChange": sess.RequirePasswordChange,
		},
	}
}

func (s *Service) Logout(token string) {
	if token == "" {
		return
	}
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
}

func (s *Service) PublicAuthConfig() map[string]any {
	authCfg := s.currentAuthConfig()
	oidc := NewOIDCManager(authCfg.OIDC)
	localAvailable := s.localLoginAvailable()
	cfg := map[string]any{
		"local": map[string]any{
			"enabled": localAvailable,
			"label":   "用户名密码登录",
		},
		"oidc": map[string]any{
			"enabled": false,
		},
	}
	if oidc != nil {
		cfg["oidc"] = oidc.PublicConfig()
	}
	return cfg
}

func (s *Service) localLoginAvailable() bool {
	admins, err := s.store.ListAdminUsers(context.Background())
	if err != nil {
		return false
	}
	for _, admin := range admins {
		if admin.Enabled {
			return true
		}
	}
	return false
}

func (s *Service) StartOIDCLogin(ctx context.Context) (string, error) {
	oidc := NewOIDCManager(s.currentAuthConfig().OIDC)
	if oidc == nil || !oidc.Enabled() {
		return "", ErrUnauthorized
	}
	state, verifier, authURL, err := oidc.StartAuth(ctx)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.pruneSecurityStateLocked(time.Now().UTC())
	s.oidcFlow[state] = oidcFlowState{Verifier: verifier, Expiry: time.Now().Add(10 * time.Minute)}
	s.mu.Unlock()
	return authURL, nil
}

func (s *Service) FinishOIDCLogin(ctx context.Context, state, code string) (string, error) {
	authCfg := s.currentAuthConfig()
	oidc := NewOIDCManager(authCfg.OIDC)
	if oidc == nil || !oidc.Enabled() {
		return "", ErrUnauthorized
	}
	s.mu.Lock()
	flow, ok := s.oidcFlow[state]
	if ok {
		delete(s.oidcFlow, state)
	}
	s.mu.Unlock()
	if !ok || time.Now().After(flow.Expiry) {
		return "", ErrUnauthorized
	}
	user, err := oidc.ExchangeCode(ctx, code, flow.Verifier)
	if err != nil {
		return "", err
	}
	if !allowOIDCUser(user, authCfg.OIDC) {
		return "", ErrUnauthorized
	}
	name := firstNonEmpty(user.PreferredUsername, user.Username, user.Email, user.Subject)
	display := firstNonEmpty(user.Name, name)
	token := newID()
	s.mu.Lock()
	s.pruneSecurityStateLocked(time.Now().UTC())
	s.sessions[token] = session{
		Username:   name,
		Display:    display,
		Email:      user.Email,
		AuthMethod: "oidc:pocketid",
		Expiry:     time.Now().Add(adminSessionIdleTTL),
	}
	s.mu.Unlock()
	return token, nil
}

func (s *Service) touchSession(token string) (session, bool) {
	if token == "" {
		return session{}, false
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneSecurityStateLocked(now)
	sess, ok := s.sessions[token]
	if !ok || now.After(sess.Expiry) {
		delete(s.sessions, token)
		return session{}, false
	}
	sess.Expiry = now.Add(adminSessionIdleTTL)
	s.sessions[token] = sess
	return sess, true
}

func (s *Service) loginRetryAfter(keys []string, now time.Time) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneSecurityStateLocked(now)
	var retryAfter time.Duration
	for _, key := range keys {
		attempt, ok := s.loginAttempts[key]
		if !ok || !now.Before(attempt.LockedUntil) {
			continue
		}
		wait := attempt.LockedUntil.Sub(now)
		if wait > retryAfter {
			retryAfter = wait
		}
	}
	return retryAfter, retryAfter > 0
}

func (s *Service) recordLoginFailure(keys []string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneSecurityStateLocked(now)
	for _, key := range keys {
		attempt := s.loginAttempts[key]
		if attempt.WindowStart.IsZero() || now.Sub(attempt.WindowStart) > loginRateWindow {
			attempt = loginAttempt{WindowStart: now}
		}
		attempt.Failures++
		if attempt.Failures >= loginRateMaxFailures {
			attempt.LockedUntil = now.Add(loginRateLockDuration)
			attempt.Failures = 0
			attempt.WindowStart = now
		}
		s.loginAttempts[key] = attempt
	}
}

func (s *Service) pruneSecurityStateLocked(now time.Time) {
	for token, sess := range s.sessions {
		if now.After(sess.Expiry) {
			delete(s.sessions, token)
		}
	}
	for state, flow := range s.oidcFlow {
		if now.After(flow.Expiry) {
			delete(s.oidcFlow, state)
		}
	}
	for key, attempt := range s.loginAttempts {
		if !attempt.LockedUntil.IsZero() && now.After(attempt.LockedUntil) && attempt.Failures == 0 {
			delete(s.loginAttempts, key)
			continue
		}
		if attempt.LockedUntil.IsZero() && !attempt.WindowStart.IsZero() && now.Sub(attempt.WindowStart) > loginRateWindow {
			delete(s.loginAttempts, key)
		}
	}
}

func (s *Service) clearLoginFailuresLocked(keys []string) {
	for _, key := range keys {
		delete(s.loginAttempts, key)
	}
}

func loginThrottleKeys(username, remoteAddr string) []string {
	ip := normalizeRemoteHost(remoteAddr)
	userKey := "user:" + strings.ToLower(strings.TrimSpace(username)) + "|" + ip
	if ip == "" {
		return []string{userKey}
	}
	return []string{"ip:" + ip, userKey}
}

func normalizeRemoteHost(remoteAddr string) string {
	value := strings.TrimSpace(remoteAddr)
	if value == "" {
		return ""
	}
	if addr, err := netip.ParseAddrPort(value); err == nil {
		return addr.Addr().String()
	}
	host, _, err := net.SplitHostPort(value)
	if err == nil {
		return strings.TrimSpace(host)
	}
	return value
}

func allowOIDCUser(user *OIDCUser, cfg OIDCConfig) bool {
	if user == nil {
		return false
	}
	if len(cfg.AllowedEmails) == 0 && len(cfg.AllowedDomains) == 0 {
		return true
	}
	for _, email := range cfg.AllowedEmails {
		if strings.EqualFold(strings.TrimSpace(email), strings.TrimSpace(user.Email)) {
			return true
		}
	}
	if at := strings.LastIndex(user.Email, "@"); at > 0 {
		domain := user.Email[at+1:]
		for _, allowed := range cfg.AllowedDomains {
			if strings.EqualFold(strings.TrimSpace(allowed), strings.TrimSpace(domain)) {
				return true
			}
		}
	}
	return false
}

func (s *Service) currentAuthConfig() AuthConfig {
	cfg := s.loadConfig()
	if cfg.Auth.Initialized {
		return authConfigFromModel(cfg.Auth)
	}
	return defaultAuthConfig(s.bootstrapAuth)
}

func defaultAuthConfig(cfg AuthConfig) AuthConfig {
	out := cfg
	if !out.LocalEnabled && !out.OIDC.Enabled {
		out.LocalEnabled = true
	}
	if strings.TrimSpace(out.OIDC.Provider) == "" {
		out.OIDC.Provider = "pocketid"
	}
	if len(out.OIDC.Scopes) == 0 {
		out.OIDC.Scopes = []string{"openid", "profile", "email"}
	}
	return out
}

func authConfigFromModel(settings model.AuthSettings) AuthConfig {
	out := AuthConfig{
		LocalEnabled: settings.LocalEnabled,
		OIDC: OIDCConfig{
			Enabled:        settings.OIDC.Enabled,
			Provider:       settings.OIDC.Provider,
			IssuerURL:      settings.OIDC.IssuerURL,
			ClientID:       settings.OIDC.ClientID,
			ClientSecret:   settings.OIDC.ClientSecret,
			RedirectURL:    settings.OIDC.RedirectURL,
			Scopes:         append([]string(nil), settings.OIDC.Scopes...),
			AllowedEmails:  append([]string(nil), settings.OIDC.AllowedEmails...),
			AllowedDomains: append([]string(nil), settings.OIDC.AllowedDomains...),
		},
	}
	return defaultAuthConfig(out)
}

func modelAuthFromConfig(cfg AuthConfig) model.AuthSettings {
	cfg = defaultAuthConfig(cfg)
	return model.AuthSettings{
		Initialized:  true,
		LocalEnabled: cfg.LocalEnabled,
		OIDC: model.OIDCAuthSettings{
			Enabled:        cfg.OIDC.Enabled,
			Provider:       cfg.OIDC.Provider,
			IssuerURL:      cfg.OIDC.IssuerURL,
			ClientID:       cfg.OIDC.ClientID,
			ClientSecret:   cfg.OIDC.ClientSecret,
			RedirectURL:    cfg.OIDC.RedirectURL,
			Scopes:         append([]string(nil), cfg.OIDC.Scopes...),
			AllowedEmails:  append([]string(nil), cfg.OIDC.AllowedEmails...),
			AllowedDomains: append([]string(nil), cfg.OIDC.AllowedDomains...),
		},
	}
}

func (s *Service) CurrentAdminSettings(token string) (map[string]any, error) {
	admin, err := s.currentAdminUser(token)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"username":              admin.Username,
		"displayName":           admin.DisplayName,
		"enabled":               admin.Enabled,
		"requirePasswordChange": admin.RequirePasswordChange,
	}, nil
}

func (s *Service) UpdateCurrentAdmin(token, username, password string, enabled bool) error {
	current, err := s.currentAdminUser(token)
	if err != nil {
		return err
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return errors.New("username is required")
	}

	authCfg := s.currentAuthConfig()

	cfg := s.loadConfig()
	if !enabled && !authCfg.OIDC.Enabled {
		activeAdmins := 0
		for _, admin := range cfg.AdminUsers {
			if admin.Enabled {
				activeAdmins++
			}
		}
		if current.Enabled && activeAdmins <= 1 {
			return errors.New("cannot disable the last enabled admin while local login is the only login method")
		}
	}
	for i := range cfg.AdminUsers {
		if !strings.EqualFold(cfg.AdminUsers[i].Username, current.Username) {
			continue
		}
		if !strings.EqualFold(current.Username, username) {
			for _, other := range cfg.AdminUsers {
				if strings.EqualFold(other.Username, username) {
					return errors.New("admin username already exists")
				}
			}
		}
		cfg.AdminUsers[i].Username = username
		cfg.AdminUsers[i].Enabled = enabled
		cfg.AdminUsers[i].UpdatedAt = time.Now().UTC()
		if password != "" {
			hash, err := security.HashPassword(password)
			if err != nil {
				return err
			}
			cfg.AdminUsers[i].PasswordHash = hash
		}
		if password != "" || !strings.EqualFold(current.Username, username) {
			cfg.AdminUsers[i].RequirePasswordChange = false
		}
		if err := s.store.Save(context.Background(), &cfg); err != nil {
			return err
		}
		s.refreshAdminSessions(current.Username, cfg.AdminUsers[i])
		return nil
	}
	return ErrNotFound
}

func (s *Service) AuthSettings() map[string]any {
	authCfg := s.currentAuthConfig()
	return map[string]any{
		"oidc": map[string]any{
			"enabled":      authCfg.OIDC.Enabled,
			"provider":     firstNonEmpty(authCfg.OIDC.Provider, "pocketid"),
			"issuerURL":    authCfg.OIDC.IssuerURL,
			"clientID":     authCfg.OIDC.ClientID,
			"clientSecret": authCfg.OIDC.ClientSecret,
			"redirectURL":  authCfg.OIDC.RedirectURL,
		},
	}
}

func (s *Service) UpdateAuthSettings(oidc OIDCConfig) error {
	oidc.Provider = firstNonEmpty(strings.TrimSpace(oidc.Provider), "pocketid")
	oidc.IssuerURL = strings.TrimSpace(oidc.IssuerURL)
	oidc.ClientID = strings.TrimSpace(oidc.ClientID)
	oidc.ClientSecret = strings.TrimSpace(oidc.ClientSecret)
	oidc.RedirectURL = strings.TrimSpace(oidc.RedirectURL)
	oidc.Scopes = []string{"openid", "profile", "email"}

	if oidc.Enabled {
		if !strings.EqualFold(oidc.Provider, "pocketid") {
			return errors.New("only Pocket ID is supported")
		}
		if oidc.IssuerURL == "" || oidc.ClientID == "" || oidc.RedirectURL == "" {
			return errors.New("OIDC issuer URL, client ID and redirect URL are required")
		}
	}
	cfg := s.loadConfig()
	if !s.hasEnabledAdmin(cfg) && !oidc.Enabled {
		return errors.New("at least one login method must be enabled")
	}
	cfg.Auth = modelAuthFromConfig(AuthConfig{
		LocalEnabled: s.hasEnabledAdmin(cfg),
		OIDC:         oidc,
	})
	if err := s.store.Save(context.Background(), &cfg); err != nil {
		return err
	}
	return nil
}

func (s *Service) BackupSQLite() ([]byte, string, error) {
	backupStore, ok := s.store.(storage.BackupCapable)
	if !ok {
		return nil, "", ErrNotFound
	}
	tmpDir, err := os.MkdirTemp("", "hdcaster-backup-*")
	if err != nil {
		return nil, "", err
	}
	defer os.RemoveAll(tmpDir)

	filename := "hdcaster-backup-" + time.Now().UTC().Format("20060102-150405") + ".sqlite3"
	path := filepath.Join(tmpDir, filename)
	if err := backupStore.Backup(context.Background(), path); err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	return data, filename, nil
}

func (s *Service) RecordRuntimeHistory() error {
	historyStore, ok := s.store.(storage.RuntimeHistoryCapable)
	if !ok {
		return nil
	}
	snap := s.hub.Snapshot()
	now := time.Now().UTC()
	points := make([]storage.RuntimeHistoryPoint, 0, len(snap.OnlineSources))
	for _, src := range snap.OnlineSources {
		points = append(points, storage.RuntimeHistoryPoint{
			Mount:          src.Mount,
			Username:       src.Username,
			RemoteAddr:     src.RemoteAddr,
			SampleTime:     now,
			ConnectedAt:    src.ConnectedAt,
			LastActive:     src.LastActive,
			BytesIn:        src.BytesIn,
			BytesOut:       src.BytesOut,
			ClientCount:    src.ClientCount,
			MessageTypes:   append([]int(nil), src.RTCM.MessageTypes...),
			Constellations: append([]string(nil), src.RTCM.Constellations...),
			FramesObserved: src.RTCM.FramesObserved,
		})
	}
	return historyStore.AppendRuntimeHistory(context.Background(), points)
}

func (s *Service) MountHistory(name string, limit int) ([]storage.RuntimeHistoryPoint, error) {
	historyStore, ok := s.store.(storage.RuntimeHistoryCapable)
	if !ok {
		return nil, ErrNotFound
	}
	points, err := historyStore.ListRuntimeHistory(context.Background(), name, limit)
	if err != nil {
		return nil, err
	}
	return points, nil
}

func (s *Service) relayForMount(mount string) map[string]any {
	for _, item := range s.Relays() {
		if strings.EqualFold(toString(item["localMount"]), mount) {
			return item
		}
	}
	return nil
}

func (s *Service) ensureBootstrapState() error {
	cfg := s.loadConfig()
	changed := false
	if !cfg.Auth.Initialized {
		cfg.Auth = modelAuthFromConfig(s.bootstrapAuth)
		changed = true
	}
	if len(cfg.AdminUsers) == 0 {
		hash, err := security.HashPassword("admin123456")
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		cfg.AdminUsers = append(cfg.AdminUsers, model.AdminUser{
			Username:              "admin",
			PasswordHash:          hash,
			DisplayName:           "Bootstrap Admin",
			Enabled:               true,
			RequirePasswordChange: !cfg.Auth.OIDC.Enabled,
			CreatedAt:             now,
			UpdatedAt:             now,
		})
		changed = true
	}
	if !changed {
		return nil
	}
	return s.store.Save(context.Background(), &cfg)
}

func (s *Service) loadConfig() model.AppConfig {
	cfg, err := s.store.Load(context.Background())
	if err != nil || cfg == nil {
		out := model.NewAppConfig()
		return out
	}
	return cfg.Clone()
}

func (s *Service) currentAdminUser(token string) (model.AdminUser, error) {
	s.mu.RLock()
	sess, ok := s.sessions[token]
	s.mu.RUnlock()
	if ok && strings.EqualFold(sess.AuthMethod, "password") {
		admin, err := s.store.GetAdminUser(context.Background(), sess.Username)
		if err == nil {
			return admin, nil
		}
	}
	admins, err := s.store.ListAdminUsers(context.Background())
	if err != nil {
		return model.AdminUser{}, err
	}
	if len(admins) == 0 {
		return model.AdminUser{}, ErrNotFound
	}
	slices.SortFunc(admins, func(a, b model.AdminUser) int {
		return strings.Compare(strings.ToLower(a.Username), strings.ToLower(b.Username))
	})
	return admins[0], nil
}

func (s *Service) refreshAdminSessions(oldUsername string, admin model.AdminUser) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for token, sess := range s.sessions {
		if !strings.EqualFold(sess.AuthMethod, "password") || !strings.EqualFold(sess.Username, oldUsername) {
			continue
		}
		sess.Username = admin.Username
		sess.Display = admin.DisplayName
		sess.RequirePasswordChange = admin.RequirePasswordChange
		if !admin.Enabled {
			delete(s.sessions, token)
			continue
		}
		s.sessions[token] = sess
	}
}

func mountRegion(mp model.Mountpoint) string {
	position := sanitizeStoredPosition(mp.Position)
	if position == nil {
		return "Unknown"
	}
	return formatFloat(position.Latitude) + ", " + formatFloat(position.Longitude)
}

func relayStateForConfig(item model.Relay, mounts []model.Mountpoint) string {
	if !item.Enabled {
		return "disabled"
	}
	for _, mount := range mounts {
		if strings.EqualFold(mount.Name, item.LocalMount) {
			if mount.Enabled {
				return "idle"
			}
			return "mount_disabled"
		}
	}
	return "mount_missing"
}

func relayUpstream(item model.Relay) string {
	mount := strings.TrimSpace(item.UpstreamMount)
	if mount != "" && !strings.HasPrefix(mount, "/") {
		mount = "/" + mount
	}
	port := item.UpstreamPort
	if port <= 0 {
		port = 2101
	}
	return item.UpstreamHost + ":" + strconv.Itoa(port) + mount
}

func relayVersion(item model.Relay) int {
	if item.NTRIPVersion == 2 {
		return 2
	}
	return 1
}

func relayClientCount(items []relay.SessionSnapshot) int {
	total := 0
	for _, item := range items {
		total += item.ClientCount
	}
	return total
}

func uniqueMountNames(runtimeSources []runtime.SourceSnapshot, relaySources []relay.SessionSnapshot) map[string]struct{} {
	out := make(map[string]struct{}, len(runtimeSources)+len(relaySources))
	for _, item := range runtimeSources {
		out[normalizeLookupKey(item.Mount)] = struct{}{}
	}
	for _, item := range relaySources {
		out[normalizeLookupKey(item.LocalMount)] = struct{}{}
	}
	return out
}

func (s *Service) relaySourceSnapshots() []relay.SessionSnapshot {
	if s.relayManager == nil {
		return nil
	}
	return s.relayManager.SessionSnapshots()
}

func (s *Service) relayRuntimeByMount() map[string]map[string]any {
	grouped := make(map[string][]relay.SessionSnapshot)
	for _, item := range s.relaySourceSnapshots() {
		grouped[normalizeLookupKey(item.LocalMount)] = append(grouped[normalizeLookupKey(item.LocalMount)], item)
	}
	out := make(map[string]map[string]any, len(grouped))
	for mount, items := range grouped {
		if len(items) == 0 {
			continue
		}
		mergedMessages := make(map[int]struct{})
		mergedMessageCounts := make(map[int]int)
		mergedConstellations := make(map[string]struct{})
		mergedMSMClasses := make(map[string]struct{})
		var framesObserved uint64
		var bytesIn uint64
		var bytesOut uint64
		var clientCount int
		var connectedAt time.Time
		var lastActive time.Time
		var decodedPosition *rtcm.GeoPoint
		var decodedECEF *rtcm.ECEF
		var reference *rtcm.ReferenceStation
		msmFamiliesBySystem := make(map[string]map[string]any)
		for _, item := range items {
			bytesIn += item.BytesIn
			bytesOut += item.BytesOut
			clientCount += item.ClientCount
			connectedAt = maxTime(connectedAt, item.ConnectedAt)
			lastActive = maxTime(lastActive, item.LastActive)
			framesObserved += item.RTCM.FramesObserved
			if decodedPosition == nil && item.RTCM.StationGeo != nil {
				decodedPosition = item.RTCM.StationGeo
			}
			if decodedECEF == nil && item.RTCM.StationECEF != nil {
				decodedECEF = item.RTCM.StationECEF
			}
			if reference == nil && item.RTCM.Reference != nil {
				reference = item.RTCM.Reference
			}
			for _, messageType := range item.RTCM.MessageTypes {
				mergedMessages[messageType] = struct{}{}
			}
			for messageType, count := range item.RTCM.MessageCounts {
				mergedMessageCounts[messageType] += count
			}
			for _, constellation := range item.RTCM.Constellations {
				mergedConstellations[constellation] = struct{}{}
			}
			for _, msmClass := range item.RTCM.MSMClasses {
				mergedMSMClasses[msmClass] = struct{}{}
			}
			for _, family := range item.RTCM.MSMFamilies {
				entry, ok := msmFamiliesBySystem[family.System]
				if !ok {
					entry = map[string]any{
						"system":       family.System,
						"messageTypes": append([]int(nil), family.MessageTypes...),
						"msmClasses":   append([]string(nil), family.MSMClasses...),
					}
					msmFamiliesBySystem[family.System] = entry
					continue
				}
				existingMessages := make(map[int]struct{})
				for _, messageType := range toIntSlice(entry["messageTypes"]) {
					existingMessages[messageType] = struct{}{}
				}
				for _, messageType := range family.MessageTypes {
					existingMessages[messageType] = struct{}{}
				}
				messageTypes := make([]int, 0, len(existingMessages))
				for messageType := range existingMessages {
					messageTypes = append(messageTypes, messageType)
				}
				slices.Sort(messageTypes)
				entry["messageTypes"] = messageTypes
				existingClasses := make(map[string]struct{})
				for _, msmClass := range toStringSlice(entry["msmClasses"]) {
					existingClasses[msmClass] = struct{}{}
				}
				for _, msmClass := range family.MSMClasses {
					existingClasses[msmClass] = struct{}{}
				}
				msmClasses := make([]string, 0, len(existingClasses))
				for msmClass := range existingClasses {
					msmClasses = append(msmClasses, msmClass)
				}
				slices.Sort(msmClasses)
				entry["msmClasses"] = msmClasses
			}
		}
		messageTypes := make([]int, 0, len(mergedMessages))
		for messageType := range mergedMessages {
			messageTypes = append(messageTypes, messageType)
		}
		slices.Sort(messageTypes)
		constellations := make([]string, 0, len(mergedConstellations))
		for constellation := range mergedConstellations {
			constellations = append(constellations, constellation)
		}
		slices.Sort(constellations)
		msmClasses := make([]string, 0, len(mergedMSMClasses))
		for msmClass := range mergedMSMClasses {
			msmClasses = append(msmClasses, msmClass)
		}
		slices.Sort(msmClasses)
		msmFamilies := make([]map[string]any, 0, len(msmFamiliesBySystem))
		for _, family := range msmFamiliesBySystem {
			msmFamilies = append(msmFamilies, family)
		}
		slices.SortFunc(msmFamilies, func(a, b map[string]any) int {
			return strings.Compare(toString(a["system"]), toString(b["system"]))
		})
		out[mount] = map[string]any{
			"id":              items[0].ID,
			"remoteAddr":      items[0].RemoteAddr,
			"username":        items[0].Username,
			"connectedAt":     connectedAt,
			"lastActive":      lastActive,
			"bytesIn":         bytesIn,
			"bytesOut":        bytesOut,
			"clientCount":     clientCount,
			"messageTypes":    messageTypes,
			"messageCounts":   mergedMessageCounts,
			"constellations":  constellations,
			"msmClasses":      msmClasses,
			"msmFamilies":     msmFamilies,
			"framesObserved":  framesObserved,
			"decodedPosition": decodedPosition,
			"decodedECEF":     decodedECEF,
			"reference":       reference,
			"sessionCount":    len(items),
			"sessions":        items,
		}
	}
	return out
}

func normalizeLookupKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeRelayAccounts(items []model.RelayAccount) []model.RelayAccount {
	out := make([]model.RelayAccount, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item.Name = strings.TrimSpace(item.Name)
		item.Username = strings.TrimSpace(item.Username)
		item.Password = strings.TrimSpace(item.Password)
		key := normalizeLookupKey(firstNonEmpty(item.Name, item.Username))
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

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func nilIfZero(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func runtimeMountRegion(mp model.Mountpoint, src runtime.SourceSnapshot) string {
	if src.RTCM.StationGeo != nil {
		return formatFloat(src.RTCM.StationGeo.Latitude) + ", " + formatFloat(src.RTCM.StationGeo.Longitude) + ", " + formatFloat(src.RTCM.StationGeo.Altitude) + "m"
	}
	return mountRegion(mp)
}

func formatInt(v int) string {
	return strconv.Itoa(v)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 4, 64)
}

func formatRuntimeGeoPoint(point *rtcm.GeoPoint) string {
	if point == nil {
		return ""
	}
	return formatFloat(point.Latitude) + ", " + formatFloat(point.Longitude) + ", " + formatFloat(point.Altitude) + "m"
}

func sanitizeStoredPosition(point *model.GeoPoint) *model.GeoPoint {
	if point == nil {
		return nil
	}
	if point.Latitude == 0 && point.Longitude == 0 {
		return nil
	}
	out := *point
	return &out
}

func newID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(buf)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func paginateMaps(items []map[string]any, page, pageSize int) PageResult {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 25
	}
	total := len(items)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	result := make([]map[string]any, 0, end-start)
	result = append(result, items[start:end]...)
	return PageResult{
		Items:    result,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}
}

func filterMaps(items []map[string]any, query string, textFn func(map[string]any) string) []map[string]any {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return items
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if strings.Contains(strings.ToLower(textFn(item)), query) {
			out = append(out, item)
		}
	}
	return out
}

func toString(v any) string {
	s, _ := v.(string)
	return s
}

func toStringSlice(v any) []string {
	items, _ := v.([]string)
	return items
}

func toInt(v any) int {
	switch value := v.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case uint64:
		return int(value)
	case float64:
		return int(value)
	case string:
		out, _ := strconv.Atoi(strings.TrimSpace(value))
		return out
	default:
		return 0
	}
}

func toIntSlice(v any) []int {
	switch items := v.(type) {
	case []int:
		return append([]int(nil), items...)
	case []any:
		out := make([]int, 0, len(items))
		for _, item := range items {
			out = append(out, toInt(item))
		}
		return out
	default:
		return nil
	}
}

func intsToStrings(values []int) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, strconv.Itoa(v))
	}
	return out
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

func (s *Service) hasEnabledAdmin(cfg model.AppConfig) bool {
	for _, admin := range cfg.AdminUsers {
		if admin.Enabled {
			return true
		}
	}
	return false
}
