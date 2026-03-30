package runtime

import (
	"errors"
	"io"
	"sync"
	"time"

	"hdcaster/internal/rtcm"
)

var (
	ErrMountBusy       = errors.New("mountpoint already has an online source")
	ErrMountNotOnline  = errors.New("mountpoint is offline")
	ErrClientQueueFull = errors.New("client queue full")
)

type LimitsProvider interface {
	MaxSources() int
	MaxClients() int
}

type Hub struct {
	mu           sync.RWMutex
	sources      map[string]*SourceSession
	totalClients int
	limits       LimitsProvider
	clientSeq    uint64
	sourceSeq    uint64
}

func NewHub(limits LimitsProvider) *Hub {
	return &Hub{
		sources: make(map[string]*SourceSession),
		limits:  limits,
	}
}

type SourceSession struct {
	ID          uint64
	Mount       string
	Username    string
	RemoteAddr  string
	ConnectedAt time.Time
	LastActive  time.Time
	BytesIn     uint64
	BytesOut    uint64
	RTCM        *rtcm.Stats

	clients map[uint64]*ClientSession
}

type ClientSession struct {
	ID          uint64
	Username    string
	RemoteAddr  string
	ConnectedAt time.Time
	LastActive  time.Time
	BytesOut    uint64
	ch          chan []byte
}

type SourceHandle struct {
	hub    *Hub
	source *SourceSession
}

type SourceState struct {
	Mount       string
	Username    string
	RemoteAddr  string
	ConnectedAt time.Time
	LastActive  time.Time
	BytesIn     uint64
	BytesOut    uint64
	ClientCount int
}

type ClientHandle struct {
	hub    *Hub
	mount  string
	client *ClientSession
}

func (h *Hub) RegisterSource(mount, username, remoteAddr string) (*SourceHandle, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.sources[mount]; exists {
		return nil, ErrMountBusy
	}
	if limit := h.limits.MaxSources(); limit > 0 && len(h.sources) >= limit {
		return nil, ErrMountBusy
	}

	h.sourceSeq++
	now := time.Now()
	src := &SourceSession{
		ID:          h.sourceSeq,
		Mount:       mount,
		Username:    username,
		RemoteAddr:  remoteAddr,
		ConnectedAt: now,
		LastActive:  now,
		RTCM:        rtcm.NewStats(),
		clients:     make(map[uint64]*ClientSession),
	}
	h.sources[mount] = src
	return &SourceHandle{hub: h, source: src}, nil
}

func (h *Hub) RegisterClient(mount, username, remoteAddr string) (*ClientHandle, <-chan []byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	src, ok := h.sources[mount]
	if !ok {
		return nil, nil, ErrMountNotOnline
	}
	if limit := h.limits.MaxClients(); limit > 0 && h.totalClients >= limit {
		return nil, nil, ErrClientQueueFull
	}

	h.clientSeq++
	now := time.Now()
	client := &ClientSession{
		ID:          h.clientSeq,
		Username:    username,
		RemoteAddr:  remoteAddr,
		ConnectedAt: now,
		LastActive:  now,
		ch:          make(chan []byte, 64),
	}
	src.clients[client.ID] = client
	h.totalClients++
	return &ClientHandle{hub: h, mount: mount, client: client}, client.ch, nil
}

func (s *SourceHandle) Publish(data []byte) {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()

	src, ok := s.hub.sources[s.source.Mount]
	if !ok {
		return
	}
	src.LastActive = time.Now()
	src.BytesIn += uint64(len(data))
	src.RTCM.Consume(data)

	for _, client := range src.clients {
		select {
		case client.ch <- append([]byte(nil), data...):
			client.LastActive = time.Now()
			client.BytesOut += uint64(len(data))
			src.BytesOut += uint64(len(data))
		default:
		}
	}
}

func (s *SourceHandle) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()

	src, ok := s.hub.sources[s.source.Mount]
	if !ok {
		return
	}
	for _, client := range src.clients {
		close(client.ch)
		hDeleteClient(src, client.ID)
		s.hub.totalClients--
	}
	delete(s.hub.sources, s.source.Mount)
}

func (s *SourceHandle) Snapshot() SourceState {
	if s == nil || s.hub == nil || s.source == nil {
		return SourceState{}
	}
	s.hub.mu.RLock()
	defer s.hub.mu.RUnlock()
	src, ok := s.hub.sources[s.source.Mount]
	if !ok {
		return SourceState{
			Mount:       s.source.Mount,
			Username:    s.source.Username,
			RemoteAddr:  s.source.RemoteAddr,
			ConnectedAt: s.source.ConnectedAt,
		}
	}
	return SourceState{
		Mount:       src.Mount,
		Username:    src.Username,
		RemoteAddr:  src.RemoteAddr,
		ConnectedAt: src.ConnectedAt,
		LastActive:  src.LastActive,
		BytesIn:     src.BytesIn,
		BytesOut:    src.BytesOut,
		ClientCount: len(src.clients),
	}
}

func (c *ClientHandle) Close() {
	c.hub.mu.Lock()
	defer c.hub.mu.Unlock()

	src, ok := c.hub.sources[c.mount]
	if !ok {
		return
	}
	if _, ok := src.clients[c.client.ID]; !ok {
		return
	}
	close(c.client.ch)
	hDeleteClient(src, c.client.ID)
	c.hub.totalClients--
}

func hDeleteClient(src *SourceSession, clientID uint64) {
	delete(src.clients, clientID)
}

func (h *Hub) DisconnectSourceUser(username string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for mount, src := range h.sources {
		if src.Username != username {
			continue
		}
		for _, client := range src.clients {
			close(client.ch)
			hDeleteClient(src, client.ID)
			h.totalClients--
		}
		delete(h.sources, mount)
	}
}

func (h *Hub) DisconnectClientUser(username string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, src := range h.sources {
		for clientID, client := range src.clients {
			if client.Username != username {
				continue
			}
			close(client.ch)
			hDeleteClient(src, clientID)
			h.totalClients--
		}
	}
}

func (h *Hub) MountClientCount(mount string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	src, ok := h.sources[mount]
	if !ok {
		return 0
	}
	return len(src.clients)
}

func (h *Hub) MaxClients() int {
	if h == nil || h.limits == nil {
		return 0
	}
	return h.limits.MaxClients()
}

func (h *Hub) MaxSources() int {
	if h == nil || h.limits == nil {
		return 0
	}
	return h.limits.MaxSources()
}

type HubSnapshot struct {
	OnlineSources []SourceSnapshot `json:"online_sources"`
	TotalSources  int              `json:"total_sources"`
	TotalClients  int              `json:"total_clients"`
}

type SourceSnapshot struct {
	ID            uint64           `json:"id"`
	Mount         string           `json:"mount"`
	Username      string           `json:"username"`
	RemoteAddr    string           `json:"remote_addr"`
	ConnectedAt   time.Time        `json:"connected_at"`
	LastActive    time.Time        `json:"last_active"`
	BytesIn       uint64           `json:"bytes_in"`
	BytesOut      uint64           `json:"bytes_out"`
	ClientCount   int              `json:"client_count"`
	RTCM          rtcm.Snapshot    `json:"rtcm"`
	OnlineClients []ClientSnapshot `json:"online_clients"`
}

type ClientSnapshot struct {
	ID          uint64    `json:"id"`
	Username    string    `json:"username"`
	RemoteAddr  string    `json:"remote_addr"`
	ConnectedAt time.Time `json:"connected_at"`
	LastActive  time.Time `json:"last_active"`
	BytesOut    uint64    `json:"bytes_out"`
}

func (h *Hub) Snapshot() HubSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := HubSnapshot{
		OnlineSources: make([]SourceSnapshot, 0, len(h.sources)),
		TotalSources:  len(h.sources),
		TotalClients:  h.totalClients,
	}
	for _, src := range h.sources {
		ss := SourceSnapshot{
			ID:            src.ID,
			Mount:         src.Mount,
			Username:      src.Username,
			RemoteAddr:    src.RemoteAddr,
			ConnectedAt:   src.ConnectedAt,
			LastActive:    src.LastActive,
			BytesIn:       src.BytesIn,
			BytesOut:      src.BytesOut,
			ClientCount:   len(src.clients),
			RTCM:          src.RTCM.Snapshot(),
			OnlineClients: make([]ClientSnapshot, 0, len(src.clients)),
		}
		for _, client := range src.clients {
			ss.OnlineClients = append(ss.OnlineClients, ClientSnapshot{
				ID:          client.ID,
				Username:    client.Username,
				RemoteAddr:  client.RemoteAddr,
				ConnectedAt: client.ConnectedAt,
				LastActive:  client.LastActive,
				BytesOut:    client.BytesOut,
			})
		}
		out.OnlineSources = append(out.OnlineSources, ss)
	}
	return out
}

func StreamToWriter(ch <-chan []byte, w io.Writer) error {
	for chunk := range ch {
		if _, err := w.Write(chunk); err != nil {
			return err
		}
	}
	return io.EOF
}
