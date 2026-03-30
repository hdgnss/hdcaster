package relay

import (
	"bufio"
	"strings"
	"testing"
	"time"

	"hdcaster/internal/model"
	"hdcaster/internal/runtime"
)

type stubLimits struct {
	maxClients int
	maxSources int
}

func (s stubLimits) MaxClients() int { return s.maxClients }
func (s stubLimits) MaxSources() int { return s.maxSources }

func TestBuildRequestRev1(t *testing.T) {
	req := buildRequestForAccount(model.Relay{
		UpstreamHost:  "caster.example.com",
		UpstreamPort:  2101,
		UpstreamMount: "MOUNT1",
		NTRIPVersion:  1,
	}, model.RelayAccount{
		Username: "user",
		Password: "pass",
	})
	if !strings.Contains(req, "GET /MOUNT1 HTTP/1.0") {
		t.Fatalf("unexpected request line: %q", req)
	}
	if !strings.Contains(req, "Authorization: Basic") {
		t.Fatalf("expected auth header in request: %q", req)
	}
}

func TestReadResponseICYOK(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("ICY 200 OK\r\n\r\n"))
	if err := readResponse(reader); err != nil {
		t.Fatalf("readResponse() error = %v", err)
	}
}

func TestNormalizeGGASentence(t *testing.T) {
	got := normalizeGGASentence(" $GPGGA,1,2,3*00\r\n")
	if got != "$GPGGA,1,2,3*00" {
		t.Fatalf("unexpected GGA sentence: %q", got)
	}
	if normalizeGGASentence("$GPRMC,1,2,3") != "" {
		t.Fatal("expected non-GGA sentence to be ignored")
	}
}

func TestParseGGASentence(t *testing.T) {
	geo, ecef, ok := parseGGASentence("$GPGGA,123519,3113.1400,N,12128.4220,E,1,08,0.9,18.0,M,0,M,,*47")
	if !ok {
		t.Fatal("expected valid GGA to parse")
	}
	if geo == nil || ecef == nil {
		t.Fatal("expected parsed geo and ecef")
	}
	if geo.Latitude < 31.21 || geo.Latitude > 31.23 {
		t.Fatalf("unexpected latitude: %+v", geo)
	}
	if geo.Longitude < 121.47 || geo.Longitude > 121.48 {
		t.Fatalf("unexpected longitude: %+v", geo)
	}
}

func TestAttachClientReusesClusterWithinRadius(t *testing.T) {
	manager := NewManager(nil, nil)
	cfg := model.Relay{
		Name:            "relay1",
		Enabled:         true,
		LocalMount:      "mount1",
		UpstreamHost:    "caster.example.com",
		UpstreamMount:   "SRC1",
		ClusterRadiusKM: 30,
		AccountPool: []model.RelayAccount{
			{Name: "a1", Username: "u1", Password: "p1", Enabled: true},
			{Name: "a2", Username: "u2", Password: "p2", Enabled: true},
		},
	}
	manager.Sync([]model.Relay{cfg}, []model.Mountpoint{{Name: "mount1", Enabled: true}})

	h1, _, err := manager.AttachClient("mount1", "c1", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("AttachClient() error = %v", err)
	}
	defer h1.Close()
	manager.mu.Lock()
	rs := manager.relays["relay1"]
	c1 := rs.clients[h1.ID()]
	c1.LatestGGA = "$GPGGA,123519,3113.1400,N,12128.4220,E,1,08,0.9,18.0,M,0,M,,*47"
	c1.Position, c1.ECEF, _ = parseGGASentence(c1.LatestGGA)
	_, _, err = manager.bindClientLocked("relay1", rs, c1, "test")
	manager.mu.Unlock()
	if err != nil {
		t.Fatalf("bindClientLocked() error = %v", err)
	}

	h2, _, err := manager.AttachClient("mount1", "c2", "127.0.0.1:2")
	if err != nil {
		t.Fatalf("AttachClient() error = %v", err)
	}
	defer h2.Close()
	manager.mu.Lock()
	c2 := rs.clients[h2.ID()]
	c2.LatestGGA = "$GPGGA,123520,3113.1410,N,12128.4230,E,1,08,0.9,18.0,M,0,M,,*47"
	c2.Position, c2.ECEF, _ = parseGGASentence(c2.LatestGGA)
	_, _, err = manager.bindClientLocked("relay1", rs, c2, "test")
	manager.mu.Unlock()
	if err != nil {
		t.Fatalf("bindClientLocked() error = %v", err)
	}

	sessions := manager.SessionSnapshots()
	if len(sessions) != 1 {
		t.Fatalf("expected one shared session, got %d", len(sessions))
	}
	if sessions[0].ClientCount != 2 {
		t.Fatalf("expected both clients on one session, got %+v", sessions[0])
	}
}

func TestAttachClientCreatesSecondSessionWhenFarAway(t *testing.T) {
	manager := NewManager(nil, nil)
	cfg := model.Relay{
		Name:            "relay2",
		Enabled:         true,
		LocalMount:      "mount2",
		UpstreamHost:    "caster.example.com",
		UpstreamMount:   "SRC2",
		ClusterRadiusKM: 30,
		ClusterSlots:    1,
		AccountPool: []model.RelayAccount{
			{Name: "a1", Username: "u1", Password: "p1", Enabled: true},
			{Name: "a2", Username: "u2", Password: "p2", Enabled: true},
		},
	}
	manager.Sync([]model.Relay{cfg}, []model.Mountpoint{{Name: "mount2", Enabled: true}})

	h1, _, _ := manager.AttachClient("mount2", "c1", "127.0.0.1:1")
	defer h1.Close()
	manager.mu.Lock()
	rs := manager.relays["relay2"]
	c1 := rs.clients[h1.ID()]
	c1.LatestGGA = "$GPGGA,123519,3113.1400,N,12128.4220,E,1,08,0.9,18.0,M,0,M,,*47"
	c1.Position, c1.ECEF, _ = parseGGASentence(c1.LatestGGA)
	_, _, _ = manager.bindClientLocked("relay2", rs, c1, "test")
	manager.mu.Unlock()

	h2, _, _ := manager.AttachClient("mount2", "c2", "127.0.0.1:2")
	defer h2.Close()
	manager.mu.Lock()
	c2 := rs.clients[h2.ID()]
	c2.LatestGGA = "$GPGGA,123520,2232.5860,N,11403.4740,E,1,08,0.9,18.0,M,0,M,,*47"
	c2.Position, c2.ECEF, _ = parseGGASentence(c2.LatestGGA)
	_, _, _ = manager.bindClientLocked("relay2", rs, c2, "test")
	manager.mu.Unlock()

	sessions := manager.SessionSnapshots()
	if len(sessions) != 2 {
		t.Fatalf("expected two sessions for distant clients, got %d", len(sessions))
	}
}

func TestPoolExhaustedClientIsDisconnectedAfterGGA(t *testing.T) {
	manager := NewManager(nil, nil)
	cfg := model.Relay{
		Name:            "relay3",
		Enabled:         true,
		LocalMount:      "mount3",
		UpstreamHost:    "caster.example.com",
		UpstreamMount:   "SRC3",
		ClusterRadiusKM: 30,
		ClusterSlots:    1,
		AccountPool: []model.RelayAccount{
			{Name: "a1", Username: "u1", Password: "p1", Enabled: true},
		},
	}
	manager.Sync([]model.Relay{cfg}, []model.Mountpoint{{Name: "mount3", Enabled: true}})

	h1, _, err := manager.AttachClient("mount3", "c1", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("AttachClient() error = %v", err)
	}
	defer h1.Close()

	manager.mu.Lock()
	rs := manager.relays["relay3"]
	c1 := rs.clients[h1.ID()]
	c1.LatestGGA = "$GPGGA,123519,3113.1400,N,12128.4220,E,1,08,0.9,18.0,M,0,M,,*47"
	c1.Position, c1.ECEF, _ = parseGGASentence(c1.LatestGGA)
	_, _, err = manager.bindClientLocked("relay3", rs, c1, "test")
	manager.mu.Unlock()
	if err != nil {
		t.Fatalf("bindClientLocked() error = %v", err)
	}

	h2, ch2, err := manager.AttachClient("mount3", "c2", "127.0.0.1:2")
	if err != nil {
		t.Fatalf("AttachClient() error = %v", err)
	}

	manager.UpdateClientGGA(h2.ID(), "$GPGGA,123520,2232.5860,N,11403.4740,E,1,08,0.9,18.0,M,0,M,,*47")

	if _, ok := <-ch2; ok {
		t.Fatal("expected second client channel to be closed on pool exhaustion")
	}
	statuses := manager.Snapshot()
	if len(statuses) != 1 || statuses[0].ActiveClients != 1 || statuses[0].ActiveSessions != 1 {
		t.Fatalf("unexpected status after pool exhaustion: %+v", statuses)
	}
}

func TestAttachClientHonorsGlobalClientLimit(t *testing.T) {
	hub := runtime.NewHub(stubLimits{maxClients: 1})
	manager := NewManager(hub, nil)
	cfg := model.Relay{
		Name:          "relay4",
		Enabled:       true,
		LocalMount:    "mount4",
		UpstreamHost:  "caster.example.com",
		UpstreamMount: "SRC4",
		AccountPool: []model.RelayAccount{
			{Name: "a1", Username: "u1", Password: "p1", Enabled: true},
		},
	}
	manager.Sync([]model.Relay{cfg}, []model.Mountpoint{{Name: "mount4", Enabled: true}})

	if _, _, err := manager.AttachClient("mount4", "c1", "127.0.0.1:1"); err != nil {
		t.Fatalf("first AttachClient() error = %v", err)
	}
	if _, _, err := manager.AttachClient("mount4", "c2", "127.0.0.1:2"); err != ErrClientLimit {
		t.Fatalf("expected ErrClientLimit, got %v", err)
	}
}

func TestBindClientHonorsGlobalSourceLimit(t *testing.T) {
	hub := runtime.NewHub(stubLimits{maxSources: 1})
	manager := NewManager(hub, nil)
	cfg := model.Relay{
		Name:            "relay5",
		Enabled:         true,
		LocalMount:      "mount5",
		UpstreamHost:    "caster.example.com",
		UpstreamMount:   "SRC5",
		ClusterRadiusKM: 30,
		ClusterSlots:    1,
		AccountPool: []model.RelayAccount{
			{Name: "a1", Username: "u1", Password: "p1", Enabled: true},
			{Name: "a2", Username: "u2", Password: "p2", Enabled: true},
		},
	}
	manager.Sync([]model.Relay{cfg}, []model.Mountpoint{{Name: "mount5", Enabled: true}})

	h1, _, _ := manager.AttachClient("mount5", "c1", "127.0.0.1:1")
	defer h1.Close()
	manager.mu.Lock()
	rs := manager.relays["relay5"]
	c1 := rs.clients[h1.ID()]
	c1.LatestGGA = "$GPGGA,123519,3113.1400,N,12128.4220,E,1,08,0.9,18.0,M,0,M,,*47"
	c1.Position, c1.ECEF, _ = parseGGASentence(c1.LatestGGA)
	_, _, err := manager.bindClientLocked("relay5", rs, c1, "test")
	manager.mu.Unlock()
	if err != nil {
		t.Fatalf("bindClientLocked() error = %v", err)
	}

	h2, ch2, _ := manager.AttachClient("mount5", "c2", "127.0.0.1:2")
	manager.UpdateClientGGA(h2.ID(), "$GPGGA,123520,2232.5860,N,11403.4740,E,1,08,0.9,18.0,M,0,M,,*47")
	if _, ok := <-ch2; ok {
		t.Fatal("expected second client channel to be closed on source limit")
	}
	statuses := manager.Snapshot()
	if len(statuses) != 1 || statuses[0].LastRejectReason != "source_limit" {
		t.Fatalf("unexpected statuses after source limit: %+v", statuses)
	}
}

func TestDisconnectClientUserRemovesRelayClient(t *testing.T) {
	manager := NewManager(nil, nil)
	cfg := model.Relay{
		Name:          "relay6",
		Enabled:       true,
		LocalMount:    "mount6",
		UpstreamHost:  "caster.example.com",
		UpstreamMount: "SRC6",
		AccountPool: []model.RelayAccount{
			{Name: "a1", Username: "u1", Password: "p1", Enabled: true},
		},
	}
	manager.Sync([]model.Relay{cfg}, []model.Mountpoint{{Name: "mount6", Enabled: true}})
	h1, ch1, err := manager.AttachClient("mount6", "victim", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("AttachClient() error = %v", err)
	}
	manager.DisconnectClientUser("victim")
	if _, ok := <-ch1; ok {
		t.Fatal("expected relay client channel to be closed on disconnect")
	}
	if _, exists := manager.clientRelay[h1.ID()]; exists {
		t.Fatal("expected relay client to be removed from index")
	}
}

func TestFailureReasonClassification(t *testing.T) {
	cases := map[string]string{
		"read upstream response: upstream unauthorized":                  "auth_failed_upstream",
		"read upstream response: upstream mount unavailable":             "upstream_mount_unavailable",
		"read upstream response: unexpected upstream response: 500":      "unexpected_upstream_response",
		"dial upstream example: lookup caster.example.com: no such host": "upstream_dns_lookup_failed",
		"dial upstream example: connection refused":                      "upstream_connection_refused",
		"read upstream stream: i/o timeout":                              "upstream_timeout",
		"read upstream stream: EOF":                                      "upstream_eof",
	}
	for input, want := range cases {
		if got := failureReasonFromError(input); got != want {
			t.Fatalf("failureReasonFromError(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestAccountHealthBackoffAndLeaseRotation(t *testing.T) {
	manager := NewManager(nil, nil)
	cfg := model.Relay{
		Name:            "relay7",
		Enabled:         true,
		LocalMount:      "mount7",
		UpstreamHost:    "caster.example.com",
		UpstreamMount:   "SRC7",
		ClusterRadiusKM: 30,
		ClusterSlots:    1,
		AccountPool: []model.RelayAccount{
			{Name: "a1", Username: "u1", Password: "p1", Enabled: true},
			{Name: "a2", Username: "u2", Password: "p2", Enabled: true},
		},
	}
	manager.Sync([]model.Relay{cfg}, []model.Mountpoint{{Name: "mount7", Enabled: true}})
	rs := manager.relays["relay7"]
	now := time.Now().UTC()
	manager.markAccountFailureLocked(rs, model.RelayAccount{Name: "a1", Username: "u1"}, "auth_failed_upstream", "upstream unauthorized", now)
	account, ok := manager.leaseAccountLocked(rs)
	if !ok {
		t.Fatal("expected a healthy account to remain leaseable")
	}
	if account.Username != "u2" {
		t.Fatalf("expected lease to rotate to account u2, got %+v", account)
	}
	healthy, unhealthy, poolHealthy, items := accountHealthSummary(rs)
	if healthy != 1 || unhealthy != 1 || poolHealthy {
		t.Fatalf("unexpected pool summary: healthy=%d unhealthy=%d poolHealthy=%v items=%+v", healthy, unhealthy, poolHealthy, items)
	}
	if len(items) != 2 {
		t.Fatalf("expected two account health items, got %+v", items)
	}
	if items[0].Name == "" || items[1].Name == "" {
		t.Fatalf("expected populated account names, got %+v", items)
	}
}

func TestRetryBackoffEscalates(t *testing.T) {
	short := retryBackoff("upstream_timeout", 1)
	longer := retryBackoff("upstream_timeout", 2)
	if longer <= short {
		t.Fatalf("expected backoff to grow, short=%s longer=%s", short, longer)
	}
	if got := retryBackoff("auth_failed_upstream", 10); got > 10*time.Minute {
		t.Fatalf("expected auth backoff cap, got %s", got)
	}
}
