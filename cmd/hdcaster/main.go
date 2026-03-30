package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"hdcaster/internal/api"
	"hdcaster/internal/app"
	"hdcaster/internal/ntrip"
	"hdcaster/internal/web"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildTime = ""
)

func main() {
	statePath := flag.String("state", envString("HDCASTER_STATE_PATH", app.DefaultStatePath()), "path to persistent SQLite state database")
	ntripAddr := flag.String("ntrip-addr", envString("HDCASTER_NTRIP_ADDR", ":2101"), "ntrip listen address")
	adminAddr := flag.String("admin-addr", envString("HDCASTER_ADMIN_ADDR", ":8080"), "admin listen address")
	localAuth := flag.Bool("local-auth", envBool("HDCASTER_LOCAL_AUTH", true), "enable local username/password admin login")
	oidcPocketID := flag.Bool("oidc-pocketid", envBool("HDCASTER_OIDC_POCKETID", false), "enable Pocket ID OIDC login")
	oidcIssuerURL := flag.String("oidc-issuer-url", os.Getenv("HDCASTER_OIDC_ISSUER_URL"), "Pocket ID issuer URL")
	oidcClientID := flag.String("oidc-client-id", os.Getenv("HDCASTER_OIDC_CLIENT_ID"), "Pocket ID client id")
	oidcClientSecret := flag.String("oidc-client-secret", os.Getenv("HDCASTER_OIDC_CLIENT_SECRET"), "Pocket ID client secret")
	oidcRedirectURL := flag.String("oidc-redirect-url", os.Getenv("HDCASTER_OIDC_REDIRECT_URL"), "Pocket ID redirect URL, e.g. http://host:8080/api/v1/auth/oidc/callback")
	oidcAllowedEmails := flag.String("oidc-allowed-emails", os.Getenv("HDCASTER_OIDC_ALLOWED_EMAILS"), "comma-separated allowed OIDC emails")
	oidcAllowedDomains := flag.String("oidc-allowed-domains", os.Getenv("HDCASTER_OIDC_ALLOWED_DOMAINS"), "comma-separated allowed OIDC email domains")
	flag.Parse()

	logger := log.New(os.Stdout, "[hdcaster] ", log.LstdFlags|log.Lmicroseconds)

	authCfg := app.AuthConfig{
		LocalEnabled: *localAuth,
		OIDC: app.OIDCConfig{
			Enabled:        *oidcPocketID,
			Provider:       "pocketid",
			IssuerURL:      strings.TrimSpace(*oidcIssuerURL),
			ClientID:       strings.TrimSpace(*oidcClientID),
			ClientSecret:   *oidcClientSecret,
			RedirectURL:    strings.TrimSpace(*oidcRedirectURL),
			AllowedEmails:  splitCSV(*oidcAllowedEmails),
			AllowedDomains: splitCSV(*oidcAllowedDomains),
		},
	}

	svc, err := app.Open(*statePath, authCfg)
	if err != nil {
		logger.Fatalf("open service: %v", err)
	}
	svc.StartRelayManager(logger)
	buildInfo := app.NewBuildInfo(version, commit, buildTime)

	webHandler, err := web.Handler()
	if err != nil {
		logger.Fatalf("build web handler: %v", err)
	}

	apiServer := api.New(svc, buildInfo)
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := svc.RecordRuntimeHistory(); err != nil {
				logger.Printf("runtime history recorder: %v", err)
			}
		}
	}()
	go func() {
		ntripServer := ntrip.NewServer(*ntripAddr, svc, logger).OnListen(func(_ net.Addr) {
			svc.SetNTRIPReady(true)
		})
		if err := ntripServer.ListenAndServe(); err != nil {
			svc.SetNTRIPReady(false)
			logger.Fatalf("ntrip server: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/api/", apiServer.Handler())
	mux.Handle("/", webHandler)

	logger.Printf("admin listening on %s", *adminAddr)
	logger.Printf("bootstrap admin username=admin password=admin123456")
	if err := http.ListenAndServe(*adminAddr, mux); err != nil {
		logger.Fatalf("admin server: %v", err)
	}
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
