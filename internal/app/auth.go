package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type AuthConfig struct {
	LocalEnabled bool
	OIDC         OIDCConfig
}

type OIDCConfig struct {
	Enabled        bool
	Provider       string
	IssuerURL      string
	ClientID       string
	ClientSecret   string
	RedirectURL    string
	Scopes         []string
	AllowedEmails  []string
	AllowedDomains []string
}

type oidcProviderMetadata struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

type OIDCManager struct {
	cfg      OIDCConfig
	client   *http.Client
	mu       sync.Mutex
	metadata oidcProviderMetadata
	loaded   bool
}

type oidcFlowState struct {
	Verifier string
	Expiry   time.Time
}

type OIDCUser struct {
	Subject           string `json:"sub"`
	Email             string `json:"email"`
	PreferredUsername string `json:"preferred_username"`
	Username          string `json:"username"`
	Name              string `json:"name"`
}

func NewOIDCManager(cfg OIDCConfig) *OIDCManager {
	if !cfg.Enabled {
		return nil
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"openid", "profile", "email"}
	}
	return &OIDCManager{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (m *OIDCManager) Enabled() bool {
	return m != nil && m.cfg.Enabled && strings.EqualFold(m.cfg.Provider, "pocketid") && m.cfg.IssuerURL != "" && m.cfg.ClientID != "" && m.cfg.RedirectURL != ""
}

func (m *OIDCManager) PublicConfig() map[string]any {
	return map[string]any{
		"enabled":  m.Enabled(),
		"provider": "pocketid",
		"label":    "HDGNSS 登录",
	}
}

func (m *OIDCManager) StartAuth(ctx context.Context) (string, string, string, error) {
	if !m.Enabled() {
		return "", "", "", errors.New("oidc not configured")
	}
	meta, err := m.loadMetadata(ctx)
	if err != nil {
		return "", "", "", err
	}
	state, err := randomURLSafe(24)
	if err != nil {
		return "", "", "", err
	}
	verifier, err := randomURLSafe(48)
	if err != nil {
		return "", "", "", err
	}
	challengeHash := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(challengeHash[:])
	u, err := url.Parse(meta.AuthorizationEndpoint)
	if err != nil {
		return "", "", "", err
	}
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", m.cfg.ClientID)
	q.Set("redirect_uri", m.cfg.RedirectURL)
	q.Set("scope", strings.Join(m.cfg.Scopes, " "))
	q.Set("state", state)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	u.RawQuery = q.Encode()
	return state, verifier, u.String(), nil
}

func (m *OIDCManager) ExchangeCode(ctx context.Context, code, verifier string) (*OIDCUser, error) {
	meta, err := m.loadMetadata(ctx)
	if err != nil {
		return nil, err
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", m.cfg.RedirectURL)
	form.Set("client_id", m.cfg.ClientID)
	form.Set("client_secret", m.cfg.ClientSecret)
	form.Set("code_verifier", verifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, meta.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("oidc token exchange failed: %s", strings.TrimSpace(string(body)))
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, err
	}
	if token.AccessToken == "" {
		return nil, errors.New("oidc token response missing access_token")
	}

	userReq, err := http.NewRequestWithContext(ctx, http.MethodGet, meta.UserinfoEndpoint, nil)
	if err != nil {
		return nil, err
	}
	userReq.Header.Set("Authorization", "Bearer "+token.AccessToken)
	userResp, err := m.client.Do(userReq)
	if err != nil {
		return nil, err
	}
	defer userResp.Body.Close()
	if userResp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(userResp.Body, 4096))
		return nil, fmt.Errorf("oidc userinfo failed: %s", strings.TrimSpace(string(body)))
	}
	var user OIDCUser
	if err := json.NewDecoder(userResp.Body).Decode(&user); err != nil {
		return nil, err
	}
	return &user, nil
}

func (m *OIDCManager) loadMetadata(ctx context.Context) (oidcProviderMetadata, error) {
	m.mu.Lock()
	if m.loaded {
		meta := m.metadata
		m.mu.Unlock()
		return meta, nil
	}
	m.mu.Unlock()

	discoveryURL := strings.TrimRight(m.cfg.IssuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return oidcProviderMetadata{}, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return oidcProviderMetadata{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return oidcProviderMetadata{}, fmt.Errorf("oidc discovery failed: %s", strings.TrimSpace(string(body)))
	}
	var meta oidcProviderMetadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return oidcProviderMetadata{}, err
	}
	if meta.AuthorizationEndpoint == "" || meta.TokenEndpoint == "" || meta.UserinfoEndpoint == "" {
		return oidcProviderMetadata{}, errors.New("oidc metadata incomplete")
	}
	m.mu.Lock()
	m.metadata = meta
	m.loaded = true
	m.mu.Unlock()
	return meta, nil
}

func randomURLSafe(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
