package api

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"hdcaster/internal/app"
	"hdcaster/internal/model"
	"hdcaster/internal/storage"
)

type Server struct {
	svc       *app.Service
	buildInfo app.BuildInfo
	mux       *http.ServeMux
}

func New(svc *app.Service, buildInfo app.BuildInfo) *Server {
	s := &Server{svc: svc, buildInfo: buildInfo, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/readyz", s.handleReadyz)
	s.mux.HandleFunc("/version", s.handleVersion)
	s.mux.HandleFunc("/api/v1/login", s.handleLogin)
	s.mux.HandleFunc("/api/v1/auth/config", s.handleAuthConfig)
	s.mux.HandleFunc("/api/v1/auth/session", s.handleAuthSession)
	s.mux.HandleFunc("/api/v1/auth/login", s.handleLogin)
	s.mux.HandleFunc("/api/v1/auth/logout", s.handleLogout)
	s.mux.HandleFunc("/api/v1/auth/oidc/start", s.handleOIDCStart)
	s.mux.HandleFunc("/api/v1/auth/oidc/callback", s.handleOIDCCallback)
	s.mux.Handle("/api/v1/overview", s.guard(http.HandlerFunc(s.handleOverview)))
	s.mux.Handle("/api/v1/audit", s.guard(http.HandlerFunc(s.handleAudit)))
	s.mux.Handle("/api/v1/sources/online", s.guard(http.HandlerFunc(s.handleSources)))
	s.mux.Handle("/api/v1/mounts", s.guard(http.HandlerFunc(s.handleMounts)))
	s.mux.Handle("/api/v1/mounts/", s.guard(http.HandlerFunc(s.handleMountDetail)))
	s.mux.Handle("/api/v1/users", s.guard(http.HandlerFunc(s.handleUsers)))
	s.mux.Handle("/api/v1/users/", s.guard(http.HandlerFunc(s.handleUserAction)))
	s.mux.Handle("/api/v1/relays", s.guard(http.HandlerFunc(s.handleRelays)))
	s.mux.Handle("/api/v1/relays/", s.guard(http.HandlerFunc(s.handleRelayAction)))
	s.mux.Handle("/api/v1/blocks", s.guard(http.HandlerFunc(s.handleBlocks)))
	s.mux.Handle("/api/v1/blocks/", s.guard(http.HandlerFunc(s.handleBlockDelete)))
	s.mux.Handle("/api/v1/limits", s.guard(http.HandlerFunc(s.handleLimits)))
	s.mux.Handle("/api/v1/settings/admin", s.guard(http.HandlerFunc(s.handleCurrentAdmin)))
	s.mux.Handle("/api/v1/settings/auth", s.guard(http.HandlerFunc(s.handleSettingsAuth)))
	s.mux.Handle("/api/v1/system/backup.sqlite3", s.guard(http.HandlerFunc(s.handleBackupSQLite)))
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	writeJSON(w, s.svc.AuditEvents(limit))
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.svc.HealthReport())
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	report := s.svc.ReadinessReport(ctx)
	status := http.StatusOK
	if report.Status != "ready" {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(report)
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.buildInfo)
}

func (s *Server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/login" || strings.HasPrefix(r.URL.Path, "/api/v1/auth/") {
			next.ServeHTTP(w, r)
			return
		}
		cookie, err := r.Cookie("hdcaster_session")
		if err != nil || !s.svc.CheckSession(cookie.Value) {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleAuthConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.svc.PublicAuthConfig())
}

func (s *Server) handleAuthSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cookie, _ := r.Cookie("hdcaster_session")
	token := ""
	if cookie != nil {
		token = cookie.Value
	}
	writeJSON(w, s.svc.SessionInfo(token))
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	remoteAddr := clientRemoteAddr(r)
	token, err := s.svc.Login(payload.Username, payload.Password, remoteAddr)
	if err != nil {
		status := http.StatusUnauthorized
		message := "admin login failed"
		if errors.Is(err, app.ErrRateLimited) {
			status = http.StatusTooManyRequests
			message = "admin login rate limited"
			w.Header().Set("Retry-After", strconv.Itoa(int((15 * time.Minute).Seconds())))
		}
		s.svc.AppendAuditEvent(storage.AuditEvent{
			Actor:      payload.Username,
			Action:     "auth.login",
			Resource:   "admin_session",
			ResourceID: payload.Username,
			Status:     "error",
			RemoteAddr: remoteAddr,
			Message:    message,
		})
		http.Error(w, `{"error":"login failed"}`, status)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "hdcaster_session",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	s.svc.AppendAuditEvent(storage.AuditEvent{
		Actor:      payload.Username,
		Action:     "auth.login",
		Resource:   "admin_session",
		ResourceID: payload.Username,
		Status:     "ok",
		RemoteAddr: remoteAddr,
		Message:    "admin login succeeded",
	})
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if cookie, err := r.Cookie("hdcaster_session"); err == nil {
		s.svc.AppendAuditEvent(storage.AuditEvent{
			Actor:      s.svc.SessionUsername(cookie.Value),
			Action:     "auth.logout",
			Resource:   "admin_session",
			Status:     "ok",
			RemoteAddr: clientRemoteAddr(r),
			Message:    "admin logout",
		})
		s.svc.Logout(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "hdcaster_session",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
		SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleOIDCStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	authURL, err := s.svc.StartOIDCLogin(r.Context())
	if err != nil {
		http.Redirect(w, r, "/?auth_error=oidc_not_configured", http.StatusFound)
		return
	}
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (s *Server) handleOIDCCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")
	if state == "" || code == "" {
		http.Redirect(w, r, "/?auth_error=oidc_callback_invalid", http.StatusFound)
		return
	}
	token, err := s.svc.FinishOIDCLogin(r.Context(), state, code)
	if err != nil {
		http.Redirect(w, r, "/?auth_error=oidc_login_failed", http.StatusFound)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "hdcaster_session",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/", http.StatusFound)
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.svc.Overview())
}

func (s *Server) currentActor(r *http.Request) string {
	cookie, _ := r.Cookie("hdcaster_session")
	if cookie == nil {
		return ""
	}
	return s.svc.SessionUsername(cookie.Value)
}

func (s *Server) auditRequest(r *http.Request, action, resource, resourceID, status, message string, details map[string]string) {
	s.svc.AppendAuditEvent(storage.AuditEvent{
		Actor:      s.currentActor(r),
		Action:     action,
		Resource:   resource,
		ResourceID: resourceID,
		Status:     status,
		RemoteAddr: clientRemoteAddr(r),
		Message:    message,
		Details:    details,
	})
}

func clientRemoteAddr(r *http.Request) string {
	for _, candidate := range []string{
		forwardedHeaderAddr(r.Header.Get("Forwarded")),
		firstForwardedAddr(r.Header.Get("X-Forwarded-For")),
		strings.TrimSpace(r.Header.Get("X-Real-Ip")),
		strings.TrimSpace(r.RemoteAddr),
	} {
		if addr := normalizeClientAddr(candidate); addr != "" {
			return addr
		}
	}
	return ""
}

func forwardedHeaderAddr(value string) string {
	for _, part := range strings.Split(value, ",") {
		for _, token := range strings.Split(part, ";") {
			key, raw, ok := strings.Cut(strings.TrimSpace(token), "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "for") {
				continue
			}
			return strings.TrimSpace(raw)
		}
	}
	return ""
}

func firstForwardedAddr(value string) string {
	head := strings.TrimSpace(strings.Split(value, ",")[0])
	return head
}

func normalizeClientAddr(value string) string {
	value = strings.TrimSpace(strings.Trim(value, `"`))
	if value == "" || strings.EqualFold(value, "unknown") {
		return ""
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = strings.TrimSpace(host)
	}
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	if addr, err := netip.ParseAddr(value); err == nil {
		return addr.String()
	}
	if addrPort, err := netip.ParseAddrPort(value); err == nil {
		return addrPort.Addr().String()
	}
	return value
}

func (s *Server) handleCurrentAdmin(w http.ResponseWriter, r *http.Request) {
	cookie, _ := r.Cookie("hdcaster_session")
	token := ""
	if cookie != nil {
		token = cookie.Value
	}
	switch r.Method {
	case http.MethodGet:
		admin, err := s.svc.CurrentAdminSettings(token)
		if err != nil {
			http.Error(w, `{"error":"admin not found"}`, http.StatusNotFound)
			return
		}
		writeJSON(w, admin)
	case http.MethodPut:
		var payload struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Enabled  bool   `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.svc.UpdateCurrentAdmin(token, payload.Username, payload.Password, payload.Enabled); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		s.auditRequest(r, "settings.admin.update", "admin_user", payload.Username, "ok", "updated current admin settings", map[string]string{
			"enabled": strconv.FormatBool(payload.Enabled),
		})
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleSettingsAuth(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.svc.AuthSettings())
	case http.MethodPut:
		var payload struct {
			OIDC struct {
				Enabled      bool   `json:"enabled"`
				Provider     string `json:"provider"`
				IssuerURL    string `json:"issuerURL"`
				ClientID     string `json:"clientID"`
				ClientSecret string `json:"clientSecret"`
				RedirectURL  string `json:"redirectURL"`
			} `json:"oidc"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.svc.UpdateAuthSettings(app.OIDCConfig{
			Enabled:      payload.OIDC.Enabled,
			Provider:     payload.OIDC.Provider,
			IssuerURL:    payload.OIDC.IssuerURL,
			ClientID:     payload.OIDC.ClientID,
			ClientSecret: payload.OIDC.ClientSecret,
			RedirectURL:  payload.OIDC.RedirectURL,
		}); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		s.auditRequest(r, "settings.auth.update", "auth_settings", "oidc", "ok", "updated auth settings", map[string]string{
			"oidc_enabled": strconv.FormatBool(payload.OIDC.Enabled),
			"provider":     payload.OIDC.Provider,
			"issuer_url":   payload.OIDC.IssuerURL,
		})
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleSources(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.svc.OnlineSources())
}

func (s *Server) handleMounts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("page") || r.URL.Query().Has("page_size") || r.URL.Query().Has("q") {
			page, pageSize := parsePageParams(r)
			writeJSON(w, s.svc.MountsPage(r.URL.Query().Get("q"), page, pageSize))
			return
		}
		writeJSON(w, s.svc.Mounts())
	case http.MethodPost:
		var mp model.Mountpoint
		if err := json.NewDecoder(r.Body).Decode(&mp); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.svc.UpsertMountpoint(mp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.auditRequest(r, "mount.upsert", "mountpoint", mp.Name, "ok", "upserted mountpoint", map[string]string{
			"enabled": strconv.FormatBool(mp.Enabled),
		})
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleMountDetail(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/mounts/")
	if strings.HasSuffix(name, "/enabled") {
		s.handleMountEnabled(w, r)
		return
	}
	if strings.HasSuffix(name, "/history") {
		s.handleMountHistory(w, r)
		return
	}
	if r.Method == http.MethodDelete {
		if err := s.svc.DeleteMountpoint(name); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s.auditRequest(r, "mount.delete", "mountpoint", name, "ok", "deleted mountpoint", nil)
		writeJSON(w, map[string]any{"ok": true})
		return
	}
	detail, err := s.svc.MountDetail(name)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, detail)
}

func (s *Server) handleMountHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/mounts/"), "/history")
	points, err := s.svc.MountHistory(name, 60)
	if err != nil {
		http.Error(w, "history unavailable", http.StatusBadRequest)
		return
	}
	writeJSON(w, points)
}

func (s *Server) handleMountEnabled(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/mounts/"), "/enabled")
	var payload struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.svc.SetMountpointEnabled(name, payload.Enabled); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.auditRequest(r, "mount.set_enabled", "mountpoint", name, "ok", "updated mountpoint enabled state", map[string]string{
		"enabled": strconv.FormatBool(payload.Enabled),
	})
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("page") || r.URL.Query().Has("page_size") || r.URL.Query().Has("q") {
			page, pageSize := parsePageParams(r)
			writeJSON(w, s.svc.UsersPage(r.URL.Query().Get("q"), page, pageSize))
			return
		}
		writeJSON(w, s.svc.Users())
	case http.MethodPost:
		var payload struct {
			Type        string   `json:"type"`
			Username    string   `json:"username"`
			Password    string   `json:"password"`
			Permissions []string `json:"permissions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.svc.UpsertUser(payload.Type, payload.Username, payload.Password, payload.Permissions); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.auditRequest(r, "user.upsert", payload.Type+"_user", payload.Username, "ok", "upserted user", map[string]string{
			"permissions": strings.Join(payload.Permissions, ","),
		})
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRelays(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("page") || r.URL.Query().Has("page_size") || r.URL.Query().Has("q") {
			page, pageSize := parsePageParams(r)
			writeJSON(w, s.svc.RelaysPage(r.URL.Query().Get("q"), page, pageSize))
			return
		}
		writeJSON(w, s.svc.Relays())
	case http.MethodPost:
		var payload struct {
			Name          string `json:"name"`
			Description   string `json:"description"`
			Enabled       bool   `json:"enabled"`
			LocalMount    string `json:"localMount"`
			UpstreamHost  string `json:"upstreamHost"`
			UpstreamPort  int    `json:"upstreamPort"`
			UpstreamMount string `json:"upstreamMount"`
			Username      string `json:"username"`
			Password      string `json:"password"`
			AccountPool   []struct {
				Name      string    `json:"name"`
				Username  string    `json:"username"`
				Password  string    `json:"password"`
				Enabled   bool      `json:"enabled"`
				ExpiresAt time.Time `json:"expiresAt"`
			} `json:"accountPool"`
			NTRIPVersion       int     `json:"ntripVersion"`
			GGASentence        string  `json:"ggaSentence"`
			GGAIntervalSeconds int     `json:"ggaIntervalSeconds"`
			ClusterRadiusKM    float64 `json:"clusterRadiusKm"`
			ClusterSlots       int     `json:"clusterSlots"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		accountPool := make([]model.RelayAccount, 0, len(payload.AccountPool))
		for _, item := range payload.AccountPool {
			accountPool = append(accountPool, model.RelayAccount{
				Name:      item.Name,
				Username:  item.Username,
				Password:  item.Password,
				Enabled:   item.Enabled,
				ExpiresAt: item.ExpiresAt,
			})
		}
		if err := s.svc.UpsertRelay(model.Relay{
			Name:               payload.Name,
			Description:        payload.Description,
			Enabled:            payload.Enabled,
			LocalMount:         payload.LocalMount,
			UpstreamHost:       payload.UpstreamHost,
			UpstreamPort:       payload.UpstreamPort,
			UpstreamMount:      payload.UpstreamMount,
			Username:           payload.Username,
			Password:           payload.Password,
			AccountPool:        accountPool,
			NTRIPVersion:       payload.NTRIPVersion,
			GGASentence:        payload.GGASentence,
			GGAIntervalSeconds: payload.GGAIntervalSeconds,
			ClusterRadiusKM:    payload.ClusterRadiusKM,
			ClusterSlots:       payload.ClusterSlots,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.auditRequest(r, "relay.upsert", "relay", payload.Name, "ok", "upserted relay", map[string]string{
			"local_mount":     payload.LocalMount,
			"upstream_host":   payload.UpstreamHost,
			"upstream_mount":  payload.UpstreamMount,
			"cluster_radius":  strconv.FormatFloat(payload.ClusterRadiusKM, 'f', -1, 64),
			"cluster_slots":   strconv.Itoa(payload.ClusterSlots),
			"account_pool_sz": strconv.Itoa(len(payload.AccountPool)),
		})
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRelayAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/relays/")
	if strings.HasSuffix(path, "/enabled") {
		name := strings.TrimSuffix(path, "/enabled")
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.svc.SetRelayEnabled(name, payload.Enabled); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s.auditRequest(r, "relay.set_enabled", "relay", name, "ok", "updated relay enabled state", map[string]string{
			"enabled": strconv.FormatBool(payload.Enabled),
		})
		writeJSON(w, map[string]any{"ok": true})
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.svc.DeleteRelay(path); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.auditRequest(r, "relay.delete", "relay", path, "ok", "deleted relay", nil)
	writeJSON(w, map[string]any{"ok": true})
}

func parsePageParams(r *http.Request) (int, int) {
	page, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("page")))
	pageSize, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("page_size")))
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 25
	}
	if pageSize > 200 {
		pageSize = 200
	}
	return page, pageSize
}

func (s *Server) handleUserAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/users/")
	if strings.HasSuffix(path, "/enabled") {
		s.handleUserEnabled(w, r)
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.svc.DeleteUser(parts[0], parts[1]); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.auditRequest(r, "user.delete", parts[0]+"_user", parts[1], "ok", "deleted user", nil)
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleUserEnabled(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/users/"), "/enabled")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var payload struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.svc.SetUserEnabled(parts[0], parts[1], payload.Enabled); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.auditRequest(r, "user.set_enabled", parts[0]+"_user", parts[1], "ok", "updated user enabled state", map[string]string{
		"enabled": strconv.FormatBool(payload.Enabled),
	})
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleBlocks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.svc.Blocks())
	case http.MethodPost:
		var payload struct {
			IP     string `json:"ip"`
			Reason string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.svc.AddBlock(payload.IP, payload.Reason); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.auditRequest(r, "block.add", "blocked_ip_rule", payload.IP, "ok", "added block rule", map[string]string{
			"reason": payload.Reason,
		})
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleBlockDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	value := strings.TrimPrefix(r.URL.Path, "/api/v1/blocks/")
	if err := s.svc.DeleteBlock(value); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.auditRequest(r, "block.delete", "blocked_ip_rule", value, "ok", "deleted block rule", nil)
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleLimits(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.svc.Limits())
	case http.MethodPut:
		var payload struct {
			MaxClients int `json:"maxClients"`
			MaxSources int `json:"maxSources"`
			MaxPending int `json:"maxPending"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.svc.SetLimits(payload.MaxClients, payload.MaxSources, payload.MaxPending); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.auditRequest(r, "limits.update", "runtime_limits", "default", "ok", "updated runtime limits", map[string]string{
			"max_clients": strconv.Itoa(payload.MaxClients),
			"max_sources": strconv.Itoa(payload.MaxSources),
			"max_pending": strconv.Itoa(payload.MaxPending),
		})
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleBackupSQLite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	data, filename, err := s.svc.BackupSQLite()
	if err != nil {
		http.Error(w, "sqlite backup unavailable", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	_, _ = w.Write(data)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
