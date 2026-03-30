package relay

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"hdcaster/internal/model"
	"hdcaster/internal/rtcm"
	"hdcaster/internal/runtime"
	"hdcaster/internal/storage"
)

var (
	ErrRelayNotFound              = errors.New("relay not found")
	ErrPoolExhausted              = errors.New("relay account pool exhausted")
	ErrClientLimit                = errors.New("relay client limit reached")
	ErrSourceLimit                = errors.New("relay source limit reached")
	ErrClientNotFound             = errors.New("relay client not found")
	errWaitingForGGA              = errors.New("waiting for valid gga")
	errUpstreamUnauthorized       = errors.New("upstream unauthorized")
	errUpstreamMountUnavailable   = errors.New("upstream mount unavailable")
	errUnexpectedUpstreamResponse = errors.New("unexpected upstream response")
	errUpstreamConnectionClosed   = errors.New("upstream connection closed")
	defaultClusterKM              = 30.0
	defaultClusterSlot            = 2
)

type Manager struct {
	hub   *runtime.Hub
	log   *log.Logger
	audit func(storage.AuditEvent)

	mu          sync.RWMutex
	configs     map[string]model.Relay
	byMount     map[string]string
	statuses    map[string]Status
	relays      map[string]*relayRuntime
	clientRelay map[uint64]string
	clientSeq   uint64
	sessionSeq  uint64
}

type Status struct {
	Name              string          `json:"name"`
	LocalMount        string          `json:"localMount"`
	Upstream          string          `json:"upstream"`
	Enabled           bool            `json:"enabled"`
	State             string          `json:"state"`
	LastError         string          `json:"lastError,omitempty"`
	LastFailureReason string          `json:"lastFailureReason,omitempty"`
	LastRejectReason  string          `json:"lastRejectReason,omitempty"`
	LastRejectAt      time.Time       `json:"lastRejectAt,omitempty"`
	LastConnectAt     time.Time       `json:"lastConnectAt,omitempty"`
	LastDisconnectAt  time.Time       `json:"lastDisconnectAt,omitempty"`
	LastSuccessfulAt  time.Time       `json:"lastSuccessfulAt,omitempty"`
	LastGGAAt         time.Time       `json:"lastGgaAt,omitempty"`
	LastRetryAt       time.Time       `json:"lastRetryAt,omitempty"`
	NextRetryAt       time.Time       `json:"nextRetryAt,omitempty"`
	RetryCount        int             `json:"retryCount"`
	RetryDelaySeconds int             `json:"retryDelaySeconds"`
	StaticGGAEnabled  bool            `json:"staticGgaEnabled"`
	UpstreamVersion   int             `json:"upstreamVersion"`
	LastConnectReason string          `json:"lastConnectReason,omitempty"`
	ActiveSessions    int             `json:"activeSessions"`
	ActiveClients     int             `json:"activeClients"`
	PoolSize          int             `json:"poolSize"`
	LeasedAccounts    int             `json:"leasedAccounts"`
	HealthyAccounts   int             `json:"healthyAccounts"`
	UnhealthyAccounts int             `json:"unhealthyAccounts"`
	PoolHealthy       bool            `json:"poolHealthy"`
	ClusterRadiusKM   float64         `json:"clusterRadiusKm"`
	ClusterSlots      int             `json:"clusterSlots"`
	RejectedClients   int             `json:"rejectedClients"`
	AccountHealth     []AccountHealth `json:"accountHealth,omitempty"`
	RecentRejects     []RejectEvent   `json:"recentRejects,omitempty"`
}

type AccountHealth struct {
	Name              string    `json:"name"`
	Username          string    `json:"username"`
	Enabled           bool      `json:"enabled"`
	Leased            bool      `json:"leased"`
	Healthy           bool      `json:"healthy"`
	State             string    `json:"state"`
	FailureCount      int       `json:"failureCount"`
	BackoffUntil      time.Time `json:"backoffUntil,omitempty"`
	LastError         string    `json:"lastError,omitempty"`
	LastFailureReason string    `json:"lastFailureReason,omitempty"`
	LastAttemptAt     time.Time `json:"lastAttemptAt,omitempty"`
	LastSuccessfulAt  time.Time `json:"lastSuccessfulAt,omitempty"`
}

type RejectEvent struct {
	At         time.Time `json:"at"`
	Reason     string    `json:"reason"`
	Username   string    `json:"username,omitempty"`
	RemoteAddr string    `json:"remoteAddr,omitempty"`
	Mount      string    `json:"mount,omitempty"`
}

type SessionSnapshot struct {
	ID                string        `json:"id"`
	RelayName         string        `json:"relayName"`
	LocalMount        string        `json:"localMount"`
	AccountName       string        `json:"accountName"`
	Username          string        `json:"username"`
	RemoteAddr        string        `json:"remoteAddr"`
	Upstream          string        `json:"upstream"`
	State             string        `json:"state"`
	LastError         string        `json:"lastError,omitempty"`
	LastFailureReason string        `json:"lastFailureReason,omitempty"`
	ConnectedAt       time.Time     `json:"connectedAt,omitempty"`
	LastActive        time.Time     `json:"lastActive,omitempty"`
	LastGGAAt         time.Time     `json:"lastGgaAt,omitempty"`
	LastSuccessfulAt  time.Time     `json:"lastSuccessfulAt,omitempty"`
	LastDisconnectAt  time.Time     `json:"lastDisconnectAt,omitempty"`
	LastRetryAt       time.Time     `json:"lastRetryAt,omitempty"`
	NextRetryAt       time.Time     `json:"nextRetryAt,omitempty"`
	RetryCount        int           `json:"retryCount"`
	RetryDelaySeconds int           `json:"retryDelaySeconds"`
	BytesIn           uint64        `json:"bytesIn"`
	BytesOut          uint64        `json:"bytesOut"`
	ClientCount       int           `json:"clientCount"`
	ClusterCount      int           `json:"clusterCount"`
	RTCM              rtcm.Snapshot `json:"rtcm"`
}

type ClientHandle struct {
	manager  *Manager
	clientID uint64
}

func (h *ClientHandle) Close() {
	if h == nil || h.manager == nil {
		return
	}
	h.manager.detachClient(h.clientID, true)
}

func (h *ClientHandle) ID() uint64 {
	if h == nil {
		return 0
	}
	return h.clientID
}

type relayRuntime struct {
	cfg           model.Relay
	mountEnabled  bool
	clients       map[uint64]*client
	sessions      map[string]*sharedSession
	accountLease  map[string]string
	accountHealth map[string]*accountHealthState
}

type client struct {
	ID          uint64
	Username    string
	RemoteAddr  string
	ConnectedAt time.Time
	LastActive  time.Time
	BytesOut    uint64
	ch          chan []byte

	LatestGGA string
	Position  *rtcm.GeoPoint
	ECEF      *rtcm.ECEF
	SessionID string
	SlotIndex int
}

type sharedSession struct {
	id       string
	manager  *Manager
	relayKey string
	cfg      model.Relay
	account  model.RelayAccount
	ctx      context.Context
	cancel   context.CancelFunc

	mu                sync.Mutex
	state             string
	lastError         string
	lastFailureReason string
	connectedAt       time.Time
	lastActive        time.Time
	lastGGAAt         time.Time
	lastSuccessfulAt  time.Time
	lastDisconnectAt  time.Time
	lastRetryAt       time.Time
	bytesIn           uint64
	bytesOut          uint64
	rtcmStats         *rtcm.Stats
	nextSlot          int
	slots             []*clusterSlot
	retryCount        int
	retryDelay        time.Duration
	nextRetryAt       time.Time
}

type accountHealthState struct {
	failureCount      int
	lastError         string
	lastFailureReason string
	lastAttemptAt     time.Time
	lastSuccessfulAt  time.Time
	backoffUntil      time.Time
	healthy           bool
}

type clusterSlot struct {
	index                  int
	clientIDs              map[uint64]struct{}
	representativeClientID uint64
	representativeECEF     *rtcm.ECEF
	lastGGA                string
	lastGGAAt              time.Time
}

func NewManager(hub *runtime.Hub, logger *log.Logger) *Manager {
	if logger == nil {
		logger = log.Default()
	}
	return &Manager{
		hub:         hub,
		log:         logger,
		configs:     make(map[string]model.Relay),
		byMount:     make(map[string]string),
		statuses:    make(map[string]Status),
		relays:      make(map[string]*relayRuntime),
		clientRelay: make(map[uint64]string),
	}
}

func (m *Manager) OnAudit(fn func(storage.AuditEvent)) *Manager {
	m.audit = fn
	return m
}

func (m *Manager) emitAudit(event storage.AuditEvent) {
	if m.audit == nil {
		return
	}
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	if event.Status == "" {
		event.Status = "ok"
	}
	m.audit(event)
}

func (m *Manager) Sync(relays []model.Relay, mounts []model.Mountpoint) {
	mountStates := make(map[string]bool, len(mounts))
	for _, mount := range mounts {
		mountStates[normalizeKey(mount.Name)] = mount.Enabled
	}

	nextConfigs := make(map[string]model.Relay, len(relays))
	nextByMount := make(map[string]string, len(relays))
	nextStatuses := make(map[string]Status, len(relays))
	for _, relayCfg := range relays {
		key := normalizeKey(relayCfg.Name)
		if key == "" {
			continue
		}
		cfg := normalizeRelayConfig(relayCfg)
		nextConfigs[key] = cfg
		nextByMount[normalizeKey(cfg.LocalMount)] = key
		nextStatuses[key] = Status{
			Name:             cfg.Name,
			LocalMount:       cfg.LocalMount,
			Upstream:         formatUpstream(cfg),
			Enabled:          cfg.Enabled,
			State:            relayStateForConfig(cfg, mountStates),
			StaticGGAEnabled: normalizeGGASentence(cfg.GGASentence) != "",
			UpstreamVersion:  upstreamVersion(cfg),
			PoolSize:         len(configuredAccounts(cfg)),
			ClusterRadiusKM:  clusterRadiusKM(cfg),
			ClusterSlots:     clusterSlots(cfg),
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for relayKey, runtimeState := range m.relays {
		nextCfg, ok := nextConfigs[relayKey]
		if !ok || !nextCfg.Enabled || !mountStates[normalizeKey(nextCfg.LocalMount)] || !sameRelayConfig(runtimeState.cfg, nextCfg) {
			m.closeRelayLocked(relayKey, true)
			delete(m.relays, relayKey)
			continue
		}
		runtimeState.cfg = nextCfg
		runtimeState.mountEnabled = mountStates[normalizeKey(nextCfg.LocalMount)]
	}

	for relayKey, cfg := range nextConfigs {
		rs, ok := m.relays[relayKey]
		if !ok {
			rs = &relayRuntime{
				cfg:           cfg,
				mountEnabled:  mountStates[normalizeKey(cfg.LocalMount)],
				clients:       make(map[uint64]*client),
				sessions:      make(map[string]*sharedSession),
				accountLease:  make(map[string]string),
				accountHealth: make(map[string]*accountHealthState),
			}
			m.relays[relayKey] = rs
		}
		rs.cfg = cfg
		rs.mountEnabled = mountStates[normalizeKey(cfg.LocalMount)]
		rs.accountHealth = m.syncAccountHealthLocked(rs, nextConfigs[relayKey].AccountPool)
		status := nextStatuses[relayKey]
		if prev, ok := m.statuses[relayKey]; ok {
			status.LastConnectAt = prev.LastConnectAt
			status.LastDisconnectAt = prev.LastDisconnectAt
			status.LastSuccessfulAt = prev.LastSuccessfulAt
			status.LastError = prev.LastError
			status.LastFailureReason = prev.LastFailureReason
			status.LastRejectReason = prev.LastRejectReason
			status.LastRejectAt = prev.LastRejectAt
			status.LastGGAAt = prev.LastGGAAt
			status.LastConnectReason = prev.LastConnectReason
			status.RejectedClients = prev.RejectedClients
			status.RecentRejects = append([]RejectEvent(nil), prev.RecentRejects...)
			status.RetryCount = prev.RetryCount
			status.RetryDelaySeconds = prev.RetryDelaySeconds
			status.LastRetryAt = prev.LastRetryAt
			status.NextRetryAt = prev.NextRetryAt
		}
		m.statuses[relayKey] = status
		m.refreshStatusLocked(relayKey)
	}

	m.configs = nextConfigs
	m.byMount = nextByMount
	for relayKey := range m.statuses {
		if _, ok := nextConfigs[relayKey]; !ok {
			delete(m.statuses, relayKey)
		}
	}
}

func (m *Manager) HasRelayMount(mount string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.byMount[normalizeKey(mount)]
	return ok
}

func (m *Manager) AttachClient(mount, username, remoteAddr string) (*ClientHandle, <-chan []byte, error) {
	relayKey := normalizeKey(mount)
	m.mu.Lock()
	cfgKey, ok := m.byMount[relayKey]
	if !ok {
		m.mu.Unlock()
		return nil, nil, ErrRelayNotFound
	}
	rs, ok := m.relays[cfgKey]
	if !ok || !rs.cfg.Enabled || !rs.mountEnabled {
		m.mu.Unlock()
		return nil, nil, ErrRelayNotFound
	}
	if !m.canAcceptClientLocked() {
		m.mu.Unlock()
		return nil, nil, ErrClientLimit
	}
	m.clientSeq++
	now := time.Now().UTC()
	c := &client{
		ID:          m.clientSeq,
		Username:    username,
		RemoteAddr:  remoteAddr,
		ConnectedAt: now,
		LastActive:  now,
		ch:          make(chan []byte, 64),
		SlotIndex:   -1,
	}
	rs.clients[c.ID] = c
	m.clientRelay[c.ID] = cfgKey

	var startSession *sharedSession
	if staticGGA := normalizeGGASentence(rs.cfg.GGASentence); staticGGA != "" {
		if geo, ecef, ok := parseGGASentence(staticGGA); ok {
			c.LatestGGA = staticGGA
			c.Position = geo
			c.ECEF = ecef
			startSession, _, _ = m.bindClientLocked(cfgKey, rs, c, "static_fallback")
		}
	}
	m.refreshStatusLocked(cfgKey)
	m.mu.Unlock()

	if startSession != nil {
		go startSession.run()
	}
	return &ClientHandle{manager: m, clientID: c.ID}, c.ch, nil
}

func (m *Manager) UpdateClientGGA(clientID uint64, sentence string) {
	sentence = normalizeGGASentence(sentence)
	if sentence == "" {
		return
	}
	geo, ecef, ok := parseGGASentence(sentence)
	m.mu.Lock()
	if !ok {
		if relayKey, exists := m.clientRelay[clientID]; exists {
			if status, ok := m.statuses[relayKey]; ok {
				now := time.Now().UTC()
				status.LastGGAAt = now
				status.LastRejectReason = "invalid_gga"
				status.LastRejectAt = now
				status.RecentRejects = appendRejectEvent(status.RecentRejects, RejectEvent{
					At:     now,
					Reason: "invalid_gga",
				})
				m.statuses[relayKey] = status
				m.emitAudit(storage.AuditEvent{
					At:         now,
					Actor:      "",
					Action:     "relay.client_reject",
					Resource:   "relay",
					ResourceID: status.Name,
					Status:     "error",
					Message:    "client rejected due to invalid gga",
					Details: map[string]string{
						"reason": "invalid_gga",
						"mount":  status.LocalMount,
					},
				})
			}
		}
		m.mu.Unlock()
		return
	}

	relayKey, ok := m.clientRelay[clientID]
	if !ok {
		m.mu.Unlock()
		return
	}
	rs, ok := m.relays[relayKey]
	if !ok {
		m.mu.Unlock()
		return
	}
	c, ok := rs.clients[clientID]
	if !ok {
		m.mu.Unlock()
		return
	}
	c.LatestGGA = sentence
	c.Position = geo
	c.ECEF = ecef
	c.LastActive = time.Now().UTC()

	startSession, rebound, err := m.bindClientLocked(relayKey, rs, c, "client_gga")
	if err == nil || errors.Is(err, errWaitingForGGA) {
		if status, exists := m.statuses[relayKey]; exists {
			status.LastGGAAt = time.Now().UTC()
			if rebound {
				status.LastConnectReason = "client_gga"
			}
			m.statuses[relayKey] = status
		}
	} else if errors.Is(err, ErrPoolExhausted) {
		if status, exists := m.statuses[relayKey]; exists {
			rejectAt := time.Now().UTC()
			status.LastGGAAt = time.Now().UTC()
			status.LastError = err.Error()
			status.LastRejectReason = "pool_exhausted"
			status.LastRejectAt = rejectAt
			status.LastDisconnectAt = rejectAt
			status.RejectedClients++
			status.RecentRejects = appendRejectEvent(status.RecentRejects, RejectEvent{
				At:         rejectAt,
				Reason:     "pool_exhausted",
				Username:   c.Username,
				RemoteAddr: c.RemoteAddr,
				Mount:      rs.cfg.LocalMount,
			})
			m.statuses[relayKey] = status
			m.emitAudit(storage.AuditEvent{
				At:         rejectAt,
				Actor:      c.Username,
				Action:     "relay.client_reject",
				Resource:   "relay",
				ResourceID: status.Name,
				Status:     "error",
				RemoteAddr: c.RemoteAddr,
				Message:    "client rejected because relay account pool is exhausted",
				Details: map[string]string{
					"reason": "pool_exhausted",
					"mount":  rs.cfg.LocalMount,
				},
			})
		}
		m.detachClientLocked(relayKey, rs, c, true)
	} else if errors.Is(err, ErrSourceLimit) {
		if status, exists := m.statuses[relayKey]; exists {
			rejectAt := time.Now().UTC()
			status.LastGGAAt = rejectAt
			status.LastError = err.Error()
			status.LastRejectReason = "source_limit"
			status.LastRejectAt = rejectAt
			status.LastDisconnectAt = rejectAt
			status.RejectedClients++
			status.RecentRejects = appendRejectEvent(status.RecentRejects, RejectEvent{
				At:         rejectAt,
				Reason:     "source_limit",
				Username:   c.Username,
				RemoteAddr: c.RemoteAddr,
				Mount:      rs.cfg.LocalMount,
			})
			m.statuses[relayKey] = status
			m.emitAudit(storage.AuditEvent{
				At:         rejectAt,
				Actor:      c.Username,
				Action:     "relay.client_reject",
				Resource:   "relay",
				ResourceID: status.Name,
				Status:     "error",
				RemoteAddr: c.RemoteAddr,
				Message:    "client rejected because relay source limit is reached",
				Details: map[string]string{
					"reason": "source_limit",
					"mount":  rs.cfg.LocalMount,
				},
			})
		}
		m.detachClientLocked(relayKey, rs, c, true)
	}
	m.refreshStatusLocked(relayKey)
	m.mu.Unlock()

	if startSession != nil {
		go startSession.run()
	}
}

func (m *Manager) Snapshot() []Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Status, 0, len(m.statuses))
	for _, status := range m.statuses {
		out = append(out, status)
	}
	return out
}

func (m *Manager) SessionSnapshots() []SessionSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []SessionSnapshot
	for relayKey, rs := range m.relays {
		for _, sess := range rs.sessions {
			sess.mu.Lock()
			snap := SessionSnapshot{
				ID:                sess.id,
				RelayName:         rs.cfg.Name,
				LocalMount:        rs.cfg.LocalMount,
				AccountName:       firstNonEmpty(sess.account.Name, sess.account.Username, rs.cfg.Name),
				Username:          firstNonEmpty(sess.account.Username, rs.cfg.Username),
				RemoteAddr:        net.JoinHostPort(rs.cfg.UpstreamHost, fmt.Sprintf("%d", relayPort(rs.cfg))),
				Upstream:          formatUpstream(rs.cfg),
				State:             sess.state,
				LastError:         firstNonEmpty(sess.lastError, m.statuses[relayKey].LastError),
				LastFailureReason: firstNonEmpty(sess.lastFailureReason, m.statuses[relayKey].LastFailureReason),
				ConnectedAt:       sess.connectedAt,
				LastActive:        sess.lastActive,
				LastGGAAt:         sess.lastGGAAt,
				LastSuccessfulAt:  sess.lastSuccessfulAt,
				LastDisconnectAt:  sess.lastDisconnectAt,
				LastRetryAt:       sess.lastRetryAt,
				NextRetryAt:       sess.nextRetryAt,
				RetryCount:        sess.retryCount,
				RetryDelaySeconds: int(sess.retryDelay.Seconds()),
				BytesIn:           sess.bytesIn,
				BytesOut:          sess.bytesOut,
				ClientCount:       sessionClientCount(sess),
				ClusterCount:      sessionClusterCount(sess),
				RTCM:              sess.rtcmStats.Snapshot(),
			}
			sess.mu.Unlock()
			out = append(out, snap)
		}
	}
	return out
}

func (m *Manager) detachClient(clientID uint64, closeCh bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	relayKey, ok := m.clientRelay[clientID]
	if !ok {
		return
	}
	rs, ok := m.relays[relayKey]
	if !ok {
		delete(m.clientRelay, clientID)
		return
	}
	c, ok := rs.clients[clientID]
	if !ok {
		delete(m.clientRelay, clientID)
		return
	}
	m.detachClientLocked(relayKey, rs, c, closeCh)
	m.refreshStatusLocked(relayKey)
}

func (m *Manager) detachClientLocked(relayKey string, rs *relayRuntime, c *client, closeCh bool) {
	if c == nil {
		return
	}
	if closeCh {
		close(c.ch)
	}
	m.removeClientFromSessionLocked(rs, c)
	delete(rs.clients, c.ID)
	delete(m.clientRelay, c.ID)
	if relayKey != "" {
		m.refreshStatusLocked(relayKey)
	}
}

func (m *Manager) DisconnectClientUser(username string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for relayKey, rs := range m.relays {
		for _, c := range rs.clients {
			if c.Username != username {
				continue
			}
			m.detachClientLocked(relayKey, rs, c, true)
		}
		m.refreshStatusLocked(relayKey)
	}
}

func (m *Manager) bindClientLocked(relayKey string, rs *relayRuntime, c *client, reason string) (*sharedSession, bool, error) {
	if c.ECEF == nil || c.LatestGGA == "" {
		return nil, false, errWaitingForGGA
	}
	if c.SessionID != "" {
		if sess, ok := rs.sessions[c.SessionID]; ok {
			slot := sessionSlot(sess, c.SlotIndex)
			if slot != nil {
				slot.lastGGA = c.LatestGGA
				slot.lastGGAAt = time.Now().UTC()
				if slot.representativeClientID == 0 {
					slot.representativeClientID = c.ID
					slot.representativeECEF = cloneECEF(c.ECEF)
				}
				return nil, false, nil
			}
		}
		m.removeClientFromSessionLocked(rs, c)
	}

	bestSessionID, bestSlot, _, foundWithin := m.findReusableSessionLocked(rs, c)
	if foundWithin {
		m.attachClientToSessionLocked(rs, rs.sessions[bestSessionID], c, bestSlot)
		return nil, true, nil
	}
	if sessionID, slotIndex, ok := m.findEmptySlotLocked(rs); ok {
		m.attachClientToSessionLocked(rs, rs.sessions[sessionID], c, slotIndex)
		return nil, true, nil
	}

	if !m.canCreateSessionLocked() {
		return nil, false, ErrSourceLimit
	}
	account, ok := m.leaseAccountLocked(rs)
	if ok {
		m.sessionSeq++
		sessionID := fmt.Sprintf("%s-%d", normalizeKey(rs.cfg.Name), m.sessionSeq)
		ctx, cancel := context.WithCancel(context.Background())
		sess := &sharedSession{
			id:        sessionID,
			manager:   m,
			relayKey:  relayKey,
			cfg:       rs.cfg,
			account:   account,
			ctx:       ctx,
			cancel:    cancel,
			state:     "connecting",
			rtcmStats: rtcm.NewStats(),
			slots:     make([]*clusterSlot, clusterSlots(rs.cfg)),
		}
		for i := range sess.slots {
			sess.slots[i] = &clusterSlot{index: i, clientIDs: make(map[uint64]struct{})}
		}
		rs.sessions[sessionID] = sess
		rs.accountLease[accountKey(account)] = sessionID
		m.markAccountLeasedLocked(rs, account)
		m.attachClientToSessionLocked(rs, sess, c, 0)
		status := m.statuses[relayKey]
		status.LastConnectAt = time.Now().UTC()
		status.LastConnectReason = reason
		status.LastError = ""
		m.statuses[relayKey] = status
		return sess, true, nil
	}

	if sessionID, slotIndex, ok := m.findEmptySlotLocked(rs); ok {
		m.attachClientToSessionLocked(rs, rs.sessions[sessionID], c, slotIndex)
		return nil, true, nil
	}
	if bestSessionID != "" {
		m.attachClientToSessionLocked(rs, rs.sessions[bestSessionID], c, bestSlot)
		return nil, true, nil
	}
	return nil, false, ErrPoolExhausted
}

func (m *Manager) findReusableSessionLocked(rs *relayRuntime, c *client) (string, int, float64, bool) {
	threshold := clusterRadiusKM(rs.cfg) * 1000
	bestDistance := math.MaxFloat64
	bestSessionID := ""
	bestSlot := -1
	for sessionID, sess := range rs.sessions {
		for slotIndex, slot := range sess.slots {
			if slot == nil || slot.representativeECEF == nil {
				continue
			}
			distance := ecefDistanceMeters(slot.representativeECEF, c.ECEF)
			if distance <= threshold && distance < bestDistance {
				bestDistance = distance
				bestSessionID = sessionID
				bestSlot = slotIndex
			}
		}
	}
	return bestSessionID, bestSlot, bestDistance, bestSessionID != ""
}

func (m *Manager) findEmptySlotLocked(rs *relayRuntime) (string, int, bool) {
	for sessionID, sess := range rs.sessions {
		for slotIndex, slot := range sess.slots {
			if slot == nil || len(slot.clientIDs) == 0 {
				return sessionID, slotIndex, true
			}
		}
	}
	return "", -1, false
}

func (m *Manager) attachClientToSessionLocked(rs *relayRuntime, sess *sharedSession, c *client, slotIndex int) {
	if sess == nil {
		return
	}
	slot := sessionSlot(sess, slotIndex)
	if slot == nil {
		return
	}
	slot.clientIDs[c.ID] = struct{}{}
	slot.lastGGA = c.LatestGGA
	slot.lastGGAAt = time.Now().UTC()
	if slot.representativeClientID == 0 {
		slot.representativeClientID = c.ID
		slot.representativeECEF = cloneECEF(c.ECEF)
	}
	c.SessionID = sess.id
	c.SlotIndex = slotIndex
}

func (m *Manager) removeClientFromSessionLocked(rs *relayRuntime, c *client) {
	if c == nil || c.SessionID == "" {
		return
	}
	sess, ok := rs.sessions[c.SessionID]
	if !ok {
		c.SessionID = ""
		c.SlotIndex = -1
		return
	}
	slot := sessionSlot(sess, c.SlotIndex)
	if slot != nil {
		delete(slot.clientIDs, c.ID)
		if slot.representativeClientID == c.ID {
			slot.representativeClientID = 0
			slot.representativeECEF = nil
			for otherID := range slot.clientIDs {
				if other, ok := rs.clients[otherID]; ok {
					slot.representativeClientID = other.ID
					slot.representativeECEF = cloneECEF(other.ECEF)
					slot.lastGGA = other.LatestGGA
					slot.lastGGAAt = time.Now().UTC()
					break
				}
			}
		}
	}
	c.SessionID = ""
	c.SlotIndex = -1
	if sessionClientCount(sess) == 0 {
		delete(rs.sessions, sess.id)
		delete(rs.accountLease, accountKey(sess.account))
		sess.cancel()
		status := m.statuses[normalizeKey(rs.cfg.Name)]
		status.LastDisconnectAt = time.Now().UTC()
		m.statuses[normalizeKey(rs.cfg.Name)] = status
	}
}

func (m *Manager) leaseAccountLocked(rs *relayRuntime) (model.RelayAccount, bool) {
	now := time.Now().UTC()
	var best model.RelayAccount
	bestScore := math.MaxInt
	for _, account := range configuredAccounts(rs.cfg) {
		key := accountKey(account)
		if _, inUse := rs.accountLease[key]; inUse {
			continue
		}
		health := rs.accountHealth[key]
		if health != nil && !health.backoffUntil.IsZero() && now.Before(health.backoffUntil) {
			continue
		}
		score := 0
		if health != nil {
			score = health.failureCount
		}
		if best.Name == "" || score < bestScore {
			best = account
			bestScore = score
		}
	}
	if best.Name == "" {
		return model.RelayAccount{}, false
	}
	return best, true
}

func (m *Manager) refreshStatusLocked(relayKey string) {
	status, ok := m.statuses[relayKey]
	if !ok {
		return
	}
	rs, ok := m.relays[relayKey]
	if !ok {
		m.statuses[relayKey] = status
		return
	}
	status.ActiveClients = len(rs.clients)
	status.ActiveSessions = len(rs.sessions)
	status.LeasedAccounts = len(rs.accountLease)
	status.PoolSize = len(configuredAccounts(rs.cfg))
	status.ClusterRadiusKM = clusterRadiusKM(rs.cfg)
	status.ClusterSlots = clusterSlots(rs.cfg)
	if !rs.cfg.Enabled {
		status.State = "disabled"
	} else if !rs.mountEnabled {
		status.State = "mount_disabled"
	} else if len(rs.sessions) == 0 && len(rs.clients) > 0 {
		status.State = "waiting_gga"
	} else if len(rs.sessions) == 0 {
		status.State = "idle"
	} else {
		state := "online"
		var retryCount int
		var nextRetryAt time.Time
		var retryDelay time.Duration
		for _, sess := range rs.sessions {
			sess.mu.Lock()
			sessionState := sess.state
			if sessionState == "connecting" {
				state = "connecting"
			}
			if sessionState == "reconnecting" {
				state = "reconnecting"
			}
			if sessionState == "error" {
				state = "error"
				status.LastError = firstNonEmpty(sess.lastError, status.LastError)
				status.LastFailureReason = firstNonEmpty(sess.lastFailureReason, status.LastFailureReason)
			}
			status.LastSuccessfulAt = maxTime(status.LastSuccessfulAt, sess.lastSuccessfulAt)
			status.LastDisconnectAt = maxTime(status.LastDisconnectAt, sess.lastDisconnectAt)
			status.LastGGAAt = maxTime(status.LastGGAAt, sess.lastGGAAt)
			status.LastRetryAt = maxTime(status.LastRetryAt, sess.lastRetryAt)
			if sess.retryCount > retryCount {
				retryCount = sess.retryCount
			}
			if nextRetryAt.IsZero() || (!sess.nextRetryAt.IsZero() && sess.nextRetryAt.Before(nextRetryAt)) {
				nextRetryAt = sess.nextRetryAt
				retryDelay = sess.retryDelay
			}
			sess.mu.Unlock()
			if state == "connecting" {
				break
			}
		}
		status.RetryCount = retryCount
		status.NextRetryAt = nextRetryAt
		status.RetryDelaySeconds = int(retryDelay.Seconds())
		status.State = state
	}
	status.HealthyAccounts, status.UnhealthyAccounts, status.PoolHealthy, status.AccountHealth = accountHealthSummary(rs)
	m.statuses[relayKey] = status
}

func (m *Manager) refreshAccountHealthLocked(status *Status, rs *relayRuntime) {
	if status == nil || rs == nil {
		return
	}
	status.HealthyAccounts, status.UnhealthyAccounts, status.PoolHealthy, status.AccountHealth = accountHealthSummary(rs)
}

func (m *Manager) closeRelayLocked(relayKey string, closeClients bool) {
	rs, ok := m.relays[relayKey]
	if !ok {
		return
	}
	for _, sess := range rs.sessions {
		sess.cancel()
	}
	rs.sessions = make(map[string]*sharedSession)
	rs.accountLease = make(map[string]string)
	if closeClients {
		for clientID, c := range rs.clients {
			close(c.ch)
			delete(m.clientRelay, clientID)
		}
		rs.clients = make(map[uint64]*client)
	}
	m.refreshStatusLocked(relayKey)
}

func (m *Manager) canAcceptClientLocked() bool {
	if m.hub == nil {
		return true
	}
	limit := m.hub.MaxClients()
	if limit <= 0 {
		return true
	}
	return m.hub.Snapshot().TotalClients+m.totalRelayClientsLocked() < limit
}

func (m *Manager) canCreateSessionLocked() bool {
	if m.hub == nil {
		return true
	}
	limit := m.hub.MaxSources()
	if limit <= 0 {
		return true
	}
	return m.hub.Snapshot().TotalSources+m.totalRelaySessionsLocked() < limit
}

func (m *Manager) totalRelayClientsLocked() int {
	total := 0
	for _, rs := range m.relays {
		total += len(rs.clients)
	}
	return total
}

func (m *Manager) totalRelaySessionsLocked() int {
	total := 0
	for _, rs := range m.relays {
		total += len(rs.sessions)
	}
	return total
}

func (s *sharedSession) run() {
	for {
		if delay, ok := s.acquireAccountForAttempt(); !ok {
			if !s.waitForRetry(delay) {
				s.finish("", false)
				return
			}
			continue
		}
		err := s.runOnce()
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) {
			s.finish("", false)
			return
		}
		retry, delay := s.handleRunError(err)
		if !retry {
			s.finish(firstNonEmpty(statusErrString(err), "relay session closed"), true)
			return
		}
		if !s.waitForRetry(delay) {
			s.finish("", false)
			return
		}
	}
}

func (s *sharedSession) acquireAccountForAttempt() (time.Duration, bool) {
	now := time.Now().UTC()
	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()

	rs, ok := s.manager.relays[s.relayKey]
	if !ok {
		return 0, false
	}

	currentKey := accountKey(s.account)
	if currentKey != "" {
		if leaseID, leased := rs.accountLease[currentKey]; leased && leaseID == s.id {
			s.manager.markAccountLeasedLocked(rs, s.account)
			return 0, true
		}
		if health := rs.accountHealth[currentKey]; health != nil && !health.backoffUntil.IsZero() && now.Before(health.backoffUntil) {
			// fall through to another account or backoff wait below
		} else if _, leased := rs.accountLease[currentKey]; !leased {
			rs.accountLease[currentKey] = s.id
			s.manager.markAccountLeasedLocked(rs, s.account)
			return 0, true
		}
	}

	if nextAccount, ok := s.manager.selectRetryAccountLocked(rs, now); ok {
		s.account = nextAccount
		rs.accountLease[accountKey(nextAccount)] = s.id
		s.manager.markAccountLeasedLocked(rs, nextAccount)
		return 0, true
	}

	nextReady := s.manager.nextAccountRetryAtLocked(rs)
	if nextReady.IsZero() {
		return 5 * time.Second, false
	}
	delay := time.Until(nextReady)
	if delay < 1*time.Second {
		delay = 1 * time.Second
	}
	return delay, false
}

func (s *sharedSession) runOnce() error {
	s.mu.Lock()
	if s.state != "online" {
		s.state = "connecting"
	}
	s.mu.Unlock()

	address := net.JoinHostPort(s.cfg.UpstreamHost, fmt.Sprintf("%d", relayPort(s.cfg)))
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(s.ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("dial upstream %s: %w", address, err)
	}
	defer conn.Close()

	if _, err := io.WriteString(conn, buildRequestForAccount(s.cfg, s.account)); err != nil {
		return fmt.Errorf("write upstream request: %w", err)
	}

	reader := bufio.NewReader(conn)
	if err := readResponse(reader); err != nil {
		return fmt.Errorf("read upstream response: %w", err)
	}

	s.mu.Lock()
	s.state = "online"
	s.connectedAt = time.Now().UTC()
	s.lastSuccessfulAt = s.connectedAt
	s.lastActive = s.connectedAt
	s.lastError = ""
	s.lastFailureReason = ""
	s.retryCount = 0
	s.retryDelay = 0
	s.nextRetryAt = time.Time{}
	s.mu.Unlock()

	s.manager.mu.Lock()
	status := s.manager.statuses[s.relayKey]
	status.LastSuccessfulAt = time.Now().UTC()
	status.LastError = ""
	status.LastFailureReason = ""
	status.LastRejectReason = ""
	s.manager.statuses[s.relayKey] = status
	if rs, ok := s.manager.relays[s.relayKey]; ok {
		s.manager.markAccountHealthyLocked(rs, s.account, s.connectedAt)
	}
	s.manager.refreshStatusLocked(s.relayKey)
	s.manager.mu.Unlock()
	s.manager.emitAudit(storage.AuditEvent{
		At:         s.connectedAt,
		Actor:      s.account.Username,
		Action:     "relay.session_connect",
		Resource:   "relay",
		ResourceID: s.cfg.Name,
		Status:     "ok",
		Message:    "relay upstream session connected",
		Details: map[string]string{
			"local_mount":    s.cfg.LocalMount,
			"upstream_host":  s.cfg.UpstreamHost,
			"upstream_mount": s.cfg.UpstreamMount,
			"account_name":   s.account.Name,
		},
	})

	ggaTicker := time.NewTicker(ggaInterval(s.cfg))
	defer ggaTicker.Stop()

	buf := make([]byte, 4096)
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-ggaTicker.C:
			if gga := s.nextGGA(); gga != "" {
				if _, err := io.WriteString(conn, gga+"\r\n"); err != nil {
					return fmt.Errorf("write gga sentence: %w", err)
				}
			}
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err := reader.Read(buf)
		if n > 0 {
			s.publish(buf[:n])
		}
		if err == nil {
			continue
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			continue
		}
		return fmt.Errorf("read upstream stream: %w", err)
	}
}

func (s *sharedSession) finish(lastError string, closeClients bool) {
	var clientsToClose []*client

	s.manager.mu.Lock()
	rs, ok := s.manager.relays[s.relayKey]
	if ok {
		if current, exists := rs.sessions[s.id]; exists && current == s {
			delete(rs.sessions, s.id)
			delete(rs.accountLease, accountKey(s.account))
			if closeClients {
				for clientID, c := range rs.clients {
					if c.SessionID != s.id {
						continue
					}
					c.SessionID = ""
					c.SlotIndex = -1
					delete(rs.clients, clientID)
					delete(s.manager.clientRelay, clientID)
					clientsToClose = append(clientsToClose, c)
				}
			} else {
				for _, c := range rs.clients {
					if c.SessionID == s.id {
						c.SessionID = ""
						c.SlotIndex = -1
					}
				}
			}
		}
	}
	s.mu.Lock()
	s.state = "error"
	s.lastError = lastError
	s.lastDisconnectAt = time.Now().UTC()
	s.mu.Unlock()

	status := s.manager.statuses[s.relayKey]
	status.LastDisconnectAt = time.Now().UTC()
	if closeClients {
		status.LastError = lastError
		status.LastFailureReason = failureReasonFromError(lastError)
	}
	s.manager.statuses[s.relayKey] = status
	s.manager.refreshStatusLocked(s.relayKey)
	s.manager.mu.Unlock()
	action := "relay.session_disconnect"
	statusValue := "ok"
	message := "relay upstream session disconnected"
	if closeClients && lastError != "" {
		action = "relay.session_error"
		statusValue = "error"
		message = "relay upstream session closed with error"
	}
	s.manager.emitAudit(storage.AuditEvent{
		At:         s.lastDisconnectAt,
		Actor:      s.account.Username,
		Action:     action,
		Resource:   "relay",
		ResourceID: s.cfg.Name,
		Status:     statusValue,
		Message:    message,
		Details: map[string]string{
			"local_mount":    s.cfg.LocalMount,
			"upstream_host":  s.cfg.UpstreamHost,
			"upstream_mount": s.cfg.UpstreamMount,
			"account_name":   s.account.Name,
			"failure_reason": failureReasonFromError(lastError),
			"last_error":     lastError,
		},
	})

	for _, c := range clientsToClose {
		close(c.ch)
	}
}

func (s *sharedSession) handleRunError(err error) (bool, time.Duration) {
	now := time.Now().UTC()
	reason := failureReasonFromError(statusErrString(err))
	delay := retryBackoff(reason, s.retryCount+1)

	s.manager.mu.Lock()
	rs, ok := s.manager.relays[s.relayKey]
	if !ok {
		s.manager.mu.Unlock()
		return false, 0
	}
	if sessionClientCount(s) == 0 {
		s.manager.mu.Unlock()
		return false, 0
	}

	s.mu.Lock()
	s.retryCount++
	s.retryDelay = delay
	s.lastRetryAt = now
	s.nextRetryAt = now.Add(delay)
	s.lastError = statusErrString(err)
	s.lastFailureReason = reason
	s.state = "reconnecting"
	s.mu.Unlock()

	s.manager.markAccountFailureLocked(rs, s.account, reason, s.lastError, now)
	s.manager.releaseAccountLeaseLocked(rs, s.account)
	if nextAccount, ok := s.manager.selectRetryAccountLocked(rs, now); ok {
		s.account = nextAccount
		s.account.Name = firstNonEmpty(s.account.Name, s.account.Username)
		s.manager.markAccountLeasedLocked(rs, s.account)
	} else if nextReady := s.manager.nextAccountRetryAtLocked(rs); !nextReady.IsZero() && nextReady.After(now) {
		if wait := nextReady.Sub(now); wait > delay {
			delay = wait
		}
	}
	s.mu.Lock()
	s.retryDelay = delay
	s.nextRetryAt = now.Add(delay)
	s.mu.Unlock()

	status := s.manager.statuses[s.relayKey]
	status.State = "reconnecting"
	status.LastError = s.lastError
	status.LastFailureReason = reason
	status.LastDisconnectAt = now
	status.LastRetryAt = now
	status.NextRetryAt = s.nextRetryAt
	status.RetryCount = s.retryCount
	status.RetryDelaySeconds = int(delay.Seconds())
	s.manager.refreshAccountHealthLocked(&status, rs)
	s.manager.statuses[s.relayKey] = status
	s.manager.refreshStatusLocked(s.relayKey)
	s.manager.mu.Unlock()

	s.manager.emitAudit(storage.AuditEvent{
		At:         now,
		Actor:      s.account.Username,
		Action:     "relay.session_retry",
		Resource:   "relay",
		ResourceID: s.cfg.Name,
		Status:     "error",
		Message:    "relay upstream session scheduled for retry",
		Details: map[string]string{
			"local_mount":    s.cfg.LocalMount,
			"upstream_host":  s.cfg.UpstreamHost,
			"upstream_mount": s.cfg.UpstreamMount,
			"account_name":   s.account.Name,
			"failure_reason": reason,
			"last_error":     s.lastError,
			"retry_delay":    delay.String(),
		},
	})

	return true, delay
}

func (s *sharedSession) waitForRetry(delay time.Duration) bool {
	if delay <= 0 {
		delay = 2 * time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *sharedSession) nextGGA() string {
	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()

	rs, ok := s.manager.relays[s.relayKey]
	if !ok {
		return ""
	}
	if len(s.slots) == 0 {
		return normalizeGGASentence(s.cfg.GGASentence)
	}
	for i := 0; i < len(s.slots); i++ {
		index := (s.nextSlot + i) % len(s.slots)
		slot := s.slots[index]
		if slot == nil || len(slot.clientIDs) == 0 || slot.lastGGA == "" {
			continue
		}
		s.nextSlot = (index + 1) % len(s.slots)
		s.mu.Lock()
		s.lastGGAAt = time.Now().UTC()
		s.mu.Unlock()
		if status, exists := s.manager.statuses[s.relayKey]; exists {
			status.LastGGAAt = time.Now().UTC()
			s.manager.statuses[s.relayKey] = status
		}
		_ = rs
		return slot.lastGGA
	}
	return normalizeGGASentence(s.cfg.GGASentence)
}

func (s *sharedSession) publish(data []byte) {
	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()

	rs, ok := s.manager.relays[s.relayKey]
	if !ok {
		return
	}
	if _, exists := rs.sessions[s.id]; !exists {
		return
	}
	s.mu.Lock()
	s.bytesIn += uint64(len(data))
	s.lastActive = time.Now().UTC()
	s.rtcmStats.Consume(data)
	s.mu.Unlock()

	for _, slot := range s.slots {
		if slot == nil {
			continue
		}
		for clientID := range slot.clientIDs {
			c, ok := rs.clients[clientID]
			if !ok {
				continue
			}
			payload := append([]byte(nil), data...)
			select {
			case c.ch <- payload:
				c.LastActive = time.Now().UTC()
				c.BytesOut += uint64(len(payload))
				s.mu.Lock()
				s.bytesOut += uint64(len(payload))
				s.mu.Unlock()
			default:
			}
		}
	}
}

func relayStateForConfig(cfg model.Relay, mountStates map[string]bool) string {
	if !cfg.Enabled {
		return "disabled"
	}
	if !mountStates[normalizeKey(cfg.LocalMount)] {
		return "mount_disabled"
	}
	return "idle"
}

func normalizeRelayConfig(cfg model.Relay) model.Relay {
	cfg.Name = strings.TrimSpace(cfg.Name)
	cfg.Description = strings.TrimSpace(cfg.Description)
	cfg.LocalMount = strings.TrimSpace(cfg.LocalMount)
	cfg.UpstreamHost = strings.TrimSpace(cfg.UpstreamHost)
	cfg.UpstreamMount = strings.TrimSpace(cfg.UpstreamMount)
	cfg.Username = strings.TrimSpace(cfg.Username)
	cfg.Password = strings.TrimSpace(cfg.Password)
	cfg.GGASentence = strings.TrimSpace(cfg.GGASentence)
	cfg.AccountPool = normalizeRelayAccounts(cfg.AccountPool)
	if cfg.UpstreamPort <= 0 {
		cfg.UpstreamPort = 2101
	}
	if cfg.NTRIPVersion != 2 {
		cfg.NTRIPVersion = 1
	}
	if cfg.GGAIntervalSeconds < 0 {
		cfg.GGAIntervalSeconds = 0
	}
	if cfg.ClusterRadiusKM <= 0 {
		cfg.ClusterRadiusKM = defaultClusterKM
	}
	if cfg.ClusterSlots <= 0 {
		cfg.ClusterSlots = defaultClusterSlot
	}
	return cfg
}

func normalizeRelayAccounts(items []model.RelayAccount) []model.RelayAccount {
	out := make([]model.RelayAccount, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item.Name = strings.TrimSpace(item.Name)
		item.Username = strings.TrimSpace(item.Username)
		item.Password = strings.TrimSpace(item.Password)
		key := normalizeKey(firstNonEmpty(item.Name, item.Username))
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

func configuredAccounts(cfg model.Relay) []model.RelayAccount {
	if len(cfg.AccountPool) > 0 {
		out := make([]model.RelayAccount, 0, len(cfg.AccountPool))
		now := time.Now().UTC()
		for _, item := range cfg.AccountPool {
			if !item.Enabled {
				continue
			}
			if !item.ExpiresAt.IsZero() && item.ExpiresAt.Before(now) {
				continue
			}
			item.Name = firstNonEmpty(item.Name, item.Username)
			out = append(out, item)
		}
		return out
	}
	return []model.RelayAccount{{
		Name:     firstNonEmpty(cfg.Username, cfg.Name, "default"),
		Username: cfg.Username,
		Password: cfg.Password,
		Enabled:  true,
	}}
}

func accountKey(account model.RelayAccount) string {
	return normalizeKey(firstNonEmpty(account.Name, account.Username))
}

func (m *Manager) syncAccountHealthLocked(rs *relayRuntime, accounts []model.RelayAccount) map[string]*accountHealthState {
	next := make(map[string]*accountHealthState, len(accounts))
	for _, account := range accounts {
		key := accountKey(account)
		if key == "" {
			continue
		}
		if prev, ok := rs.accountHealth[key]; ok && prev != nil {
			copyState := *prev
			next[key] = &copyState
			continue
		}
		next[key] = &accountHealthState{healthy: true}
	}
	return next
}

func (m *Manager) markAccountLeasedLocked(rs *relayRuntime, account model.RelayAccount) {
	key := accountKey(account)
	if key == "" {
		return
	}
	health := rs.accountHealth[key]
	if health == nil {
		health = &accountHealthState{}
		rs.accountHealth[key] = health
	}
	health.healthy = true
}

func (m *Manager) releaseAccountLeaseLocked(rs *relayRuntime, account model.RelayAccount) {
	key := accountKey(account)
	if key == "" {
		return
	}
	delete(rs.accountLease, key)
	if health := rs.accountHealth[key]; health != nil {
		health.healthy = health.failureCount == 0 || time.Now().UTC().After(health.backoffUntil)
	}
}

func (m *Manager) markAccountHealthyLocked(rs *relayRuntime, account model.RelayAccount, now time.Time) {
	key := accountKey(account)
	if key == "" {
		return
	}
	health := rs.accountHealth[key]
	if health == nil {
		health = &accountHealthState{}
		rs.accountHealth[key] = health
	}
	health.failureCount = 0
	health.lastError = ""
	health.lastFailureReason = ""
	health.lastSuccessfulAt = now
	health.lastAttemptAt = now
	health.backoffUntil = time.Time{}
	health.healthy = true
}

func (m *Manager) markAccountFailureLocked(rs *relayRuntime, account model.RelayAccount, reason, lastError string, now time.Time) {
	key := accountKey(account)
	if key == "" {
		return
	}
	health := rs.accountHealth[key]
	if health == nil {
		health = &accountHealthState{}
		rs.accountHealth[key] = health
	}
	health.failureCount++
	health.lastError = lastError
	health.lastFailureReason = reason
	health.lastAttemptAt = now
	health.backoffUntil = now.Add(retryBackoff(reason, health.failureCount))
	health.healthy = false
}

func (m *Manager) selectRetryAccountLocked(rs *relayRuntime, now time.Time) (model.RelayAccount, bool) {
	accounts := configuredAccounts(rs.cfg)
	if len(accounts) == 0 {
		return model.RelayAccount{}, false
	}

	var best model.RelayAccount
	bestScore := math.MaxInt
	for _, account := range accounts {
		key := accountKey(account)
		if key == "" {
			continue
		}
		if _, inUse := rs.accountLease[key]; inUse {
			continue
		}
		health := rs.accountHealth[key]
		if health == nil {
			health = &accountHealthState{healthy: true}
			rs.accountHealth[key] = health
		}
		if !health.backoffUntil.IsZero() && now.Before(health.backoffUntil) {
			continue
		}
		score := health.failureCount
		if best.Name == "" || score < bestScore {
			best = account
			bestScore = score
		}
	}
	if best.Name == "" {
		return model.RelayAccount{}, false
	}
	return best, true
}

func (m *Manager) nextAccountRetryAtLocked(rs *relayRuntime) time.Time {
	var earliest time.Time
	for _, health := range rs.accountHealth {
		if health == nil || health.backoffUntil.IsZero() {
			continue
		}
		if earliest.IsZero() || health.backoffUntil.Before(earliest) {
			earliest = health.backoffUntil
		}
	}
	return earliest
}

func sameRelayConfig(a, b model.Relay) bool {
	if a.Name != b.Name ||
		a.Description != b.Description ||
		a.Enabled != b.Enabled ||
		a.LocalMount != b.LocalMount ||
		a.UpstreamHost != b.UpstreamHost ||
		a.UpstreamPort != b.UpstreamPort ||
		a.UpstreamMount != b.UpstreamMount ||
		a.Username != b.Username ||
		a.Password != b.Password ||
		a.NTRIPVersion != b.NTRIPVersion ||
		a.GGASentence != b.GGASentence ||
		a.GGAIntervalSeconds != b.GGAIntervalSeconds ||
		clusterRadiusKM(a) != clusterRadiusKM(b) ||
		clusterSlots(a) != clusterSlots(b) ||
		len(a.AccountPool) != len(b.AccountPool) {
		return false
	}
	for i := range a.AccountPool {
		if a.AccountPool[i] != b.AccountPool[i] {
			return false
		}
	}
	return true
}

func buildRequestForAccount(cfg model.Relay, account model.RelayAccount) string {
	mount := cfg.UpstreamMount
	if !strings.HasPrefix(mount, "/") {
		mount = "/" + mount
	}
	var lines []string
	if upstreamVersion(cfg) == 2 {
		lines = []string{
			fmt.Sprintf("GET %s HTTP/1.1", mount),
			fmt.Sprintf("Host: %s:%d", cfg.UpstreamHost, relayPort(cfg)),
			"User-Agent: NTRIP hdcaster-relay/1.0",
			"Ntrip-Version: Ntrip/2.0",
			"Connection: close",
		}
	} else {
		lines = []string{
			fmt.Sprintf("GET %s HTTP/1.0", mount),
			"User-Agent: NTRIP hdcaster-relay/1.0",
			"Connection: close",
		}
	}
	if account.Username != "" || account.Password != "" {
		token := base64.StdEncoding.EncodeToString([]byte(account.Username + ":" + account.Password))
		lines = append(lines, "Authorization: Basic "+token)
	}
	return strings.Join(lines, "\r\n") + "\r\n\r\n"
}

func readResponse(reader *bufio.Reader) error {
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	switch {
	case strings.HasPrefix(line, "ICY 200"), strings.HasPrefix(line, "HTTP/1.0 200"), strings.HasPrefix(line, "HTTP/1.1 200"):
	case strings.Contains(line, "401"):
		return errUpstreamUnauthorized
	case strings.HasPrefix(line, "SOURCETABLE 200"):
		return errUpstreamMountUnavailable
	default:
		return fmt.Errorf("%w: %s", errUnexpectedUpstreamResponse, line)
	}
	for i := 0; i < 64; i++ {
		headerLine, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if strings.TrimSpace(headerLine) == "" {
			return nil
		}
	}
	return nil
}

func relayPort(cfg model.Relay) int {
	if cfg.UpstreamPort > 0 {
		return cfg.UpstreamPort
	}
	return 2101
}

func upstreamVersion(cfg model.Relay) int {
	if cfg.NTRIPVersion == 2 {
		return 2
	}
	return 1
}

func ggaInterval(cfg model.Relay) time.Duration {
	if cfg.GGAIntervalSeconds > 0 {
		return time.Duration(cfg.GGAIntervalSeconds) * time.Second
	}
	return 5 * time.Second
}

func clusterRadiusKM(cfg model.Relay) float64 {
	if cfg.ClusterRadiusKM > 0 {
		return cfg.ClusterRadiusKM
	}
	return defaultClusterKM
}

func clusterSlots(cfg model.Relay) int {
	if cfg.ClusterSlots > 0 {
		return cfg.ClusterSlots
	}
	return defaultClusterSlot
}

func normalizeGGASentence(sentence string) string {
	sentence = strings.TrimSpace(sentence)
	if sentence == "" {
		return ""
	}
	if !strings.HasPrefix(sentence, "$") {
		return ""
	}
	if !strings.Contains(strings.ToUpper(sentence), "GGA") {
		return ""
	}
	return strings.TrimRight(sentence, "\r\n")
}

func parseGGASentence(sentence string) (*rtcm.GeoPoint, *rtcm.ECEF, bool) {
	sentence = normalizeGGASentence(sentence)
	if sentence == "" {
		return nil, nil, false
	}
	if star := strings.IndexByte(sentence, '*'); star >= 0 {
		sentence = sentence[:star]
	}
	fields := strings.Split(sentence, ",")
	if len(fields) < 10 {
		return nil, nil, false
	}
	lat, ok := parseNMEACoordinate(fields[2], fields[3], 2)
	if !ok || math.Abs(lat) < 1e-9 {
		return nil, nil, false
	}
	lon, ok := parseNMEACoordinate(fields[4], fields[5], 3)
	if !ok {
		return nil, nil, false
	}
	quality := strings.TrimSpace(fields[6])
	if quality == "" || quality == "0" {
		return nil, nil, false
	}
	altitude := 0.0
	fmt.Sscanf(strings.TrimSpace(fields[9]), "%f", &altitude)
	geo := &rtcm.GeoPoint{Latitude: lat, Longitude: lon, Altitude: altitude}
	return geo, geodeticToECEF(geo), true
}

func parseNMEACoordinate(value, hemi string, degreeDigits int) (float64, bool) {
	value = strings.TrimSpace(value)
	hemi = strings.ToUpper(strings.TrimSpace(hemi))
	if value == "" || len(value) <= degreeDigits {
		return 0, false
	}
	degreePart := value[:degreeDigits]
	minutePart := value[degreeDigits:]
	var degrees, minutes float64
	fmt.Sscanf(degreePart, "%f", &degrees)
	fmt.Sscanf(minutePart, "%f", &minutes)
	out := degrees + minutes/60.0
	switch hemi {
	case "S", "W":
		out = -out
	case "N", "E":
	default:
		return 0, false
	}
	return out, true
}

func geodeticToECEF(point *rtcm.GeoPoint) *rtcm.ECEF {
	if point == nil {
		return nil
	}
	const (
		a = 6378137.0
		f = 1 / 298.257223563
	)
	lat := point.Latitude * math.Pi / 180
	lon := point.Longitude * math.Pi / 180
	e2 := f * (2 - f)
	sinLat := math.Sin(lat)
	cosLat := math.Cos(lat)
	sinLon := math.Sin(lon)
	cosLon := math.Cos(lon)
	n := a / math.Sqrt(1-e2*sinLat*sinLat)
	return &rtcm.ECEF{
		X: (n + point.Altitude) * cosLat * cosLon,
		Y: (n + point.Altitude) * cosLat * sinLon,
		Z: (n*(1-e2) + point.Altitude) * sinLat,
	}
}

func ecefDistanceMeters(a, b *rtcm.ECEF) float64 {
	if a == nil || b == nil {
		return math.MaxFloat64
	}
	dx := a.X - b.X
	dy := a.Y - b.Y
	dz := a.Z - b.Z
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}

func cloneECEF(in *rtcm.ECEF) *rtcm.ECEF {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func sessionSlot(sess *sharedSession, slotIndex int) *clusterSlot {
	if sess == nil || slotIndex < 0 || slotIndex >= len(sess.slots) {
		return nil
	}
	return sess.slots[slotIndex]
}

func sessionClientCount(sess *sharedSession) int {
	total := 0
	for _, slot := range sess.slots {
		if slot != nil {
			total += len(slot.clientIDs)
		}
	}
	return total
}

func sessionClusterCount(sess *sharedSession) int {
	count := 0
	for _, slot := range sess.slots {
		if slot != nil && len(slot.clientIDs) > 0 {
			count++
		}
	}
	return count
}

func formatUpstream(cfg model.Relay) string {
	mount := strings.TrimSpace(cfg.UpstreamMount)
	if mount != "" && !strings.HasPrefix(mount, "/") {
		mount = "/" + mount
	}
	return fmt.Sprintf("%s:%d%s", cfg.UpstreamHost, relayPort(cfg), mount)
}

func normalizeKey(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func statusErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func appendRejectEvent(items []RejectEvent, event RejectEvent) []RejectEvent {
	out := append([]RejectEvent(nil), items...)
	out = append(out, event)
	if len(out) > 10 {
		out = out[len(out)-10:]
	}
	return out
}

func accountHealthSummary(rs *relayRuntime) (int, int, bool, []AccountHealth) {
	now := time.Now().UTC()
	items := make([]AccountHealth, 0, len(rs.accountHealth))
	healthy := 0
	unhealthy := 0
	for _, account := range configuredAccounts(rs.cfg) {
		key := accountKey(account)
		health := rs.accountHealth[key]
		item := AccountHealth{
			Name:     firstNonEmpty(account.Name, account.Username),
			Username: account.Username,
			Enabled:  account.Enabled,
			Leased:   rs.accountLease[key] != "",
		}
		if health != nil {
			item.FailureCount = health.failureCount
			item.BackoffUntil = health.backoffUntil
			item.LastError = health.lastError
			item.LastFailureReason = health.lastFailureReason
			item.LastAttemptAt = health.lastAttemptAt
			item.LastSuccessfulAt = health.lastSuccessfulAt
		}
		item.Healthy = health == nil || health.backoffUntil.IsZero() || !now.Before(health.backoffUntil)
		if item.Healthy {
			item.State = "healthy"
			healthy++
		} else {
			item.State = "backoff"
			unhealthy++
		}
		if item.Leased && item.Healthy {
			item.State = "leased"
		}
		if item.Leased && !item.Healthy {
			item.State = "leased_backoff"
		}
		items = append(items, item)
	}
	slices.SortFunc(items, func(a, b AccountHealth) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return healthy, unhealthy, unhealthy == 0, items
}

func retryBackoff(reason string, attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	base := 5 * time.Second
	capDelay := 2 * time.Minute
	switch reason {
	case "auth_failed_upstream":
		base = 30 * time.Second
		capDelay = 10 * time.Minute
	case "upstream_mount_unavailable":
		base = 15 * time.Second
		capDelay = 5 * time.Minute
	case "upstream_dns_lookup_failed", "upstream_connection_refused", "upstream_network_unreachable":
		base = 10 * time.Second
		capDelay = 2 * time.Minute
	}
	multiplier := 1
	for i := 1; i < attempt && multiplier < 64; i++ {
		multiplier *= 2
	}
	delay := base * time.Duration(multiplier)
	if delay > capDelay {
		delay = capDelay
	}
	return delay
}

func failureReasonFromError(message string) string {
	switch {
	case errors.Is(errorFromMessage(message), errUpstreamUnauthorized):
		return "auth_failed_upstream"
	case errors.Is(errorFromMessage(message), errUpstreamMountUnavailable):
		return "upstream_mount_unavailable"
	case errors.Is(errorFromMessage(message), errUnexpectedUpstreamResponse):
		return "unexpected_upstream_response"
	case strings.Contains(strings.ToLower(message), "dns") || strings.Contains(strings.ToLower(message), "lookup") || strings.Contains(strings.ToLower(message), "no such host"):
		return "upstream_dns_lookup_failed"
	case strings.Contains(strings.ToLower(message), "refused"):
		return "upstream_connection_refused"
	case strings.Contains(strings.ToLower(message), "no route") || strings.Contains(strings.ToLower(message), "network is unreachable"):
		return "upstream_network_unreachable"
	case strings.Contains(strings.ToLower(message), "timeout"):
		return "upstream_timeout"
	case strings.Contains(strings.ToLower(message), "eof"):
		return "upstream_eof"
	case strings.Contains(strings.ToLower(message), "closed"):
		return "upstream_closed"
	case strings.TrimSpace(message) == "":
		return ""
	default:
		return "upstream_io_error"
	}
}

func errorFromMessage(message string) error {
	switch {
	case strings.Contains(message, errUpstreamUnauthorized.Error()):
		return errUpstreamUnauthorized
	case strings.Contains(message, errUpstreamMountUnavailable.Error()):
		return errUpstreamMountUnavailable
	case strings.Contains(message, errUnexpectedUpstreamResponse.Error()):
		return errUnexpectedUpstreamResponse
	default:
		return nil
	}
}
