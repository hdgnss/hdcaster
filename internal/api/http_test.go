package api

import (
	"net/http/httptest"
	"testing"
)

func TestClientRemoteAddrPrefersForwardedHeaders(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/audit", nil)
	req.RemoteAddr = "172.18.0.3:54321"
	req.Header.Set("Forwarded", `for=198.51.100.7;proto=https;host=admin.example.com`)
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	req.Header.Set("X-Real-Ip", "203.0.113.10")

	if got := clientRemoteAddr(req); got != "198.51.100.7" {
		t.Fatalf("clientRemoteAddr() = %q, want %q", got, "198.51.100.7")
	}
}

func TestClientRemoteAddrFallsBackThroughProxyHeaders(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/audit", nil)
	req.RemoteAddr = "172.18.0.3:54321"
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")

	if got := clientRemoteAddr(req); got != "203.0.113.9" {
		t.Fatalf("clientRemoteAddr() = %q, want %q", got, "203.0.113.9")
	}

	req = httptest.NewRequest("GET", "/api/v1/audit", nil)
	req.RemoteAddr = "172.18.0.3:54321"
	req.Header.Set("X-Real-Ip", "203.0.113.10")

	if got := clientRemoteAddr(req); got != "203.0.113.10" {
		t.Fatalf("clientRemoteAddr() = %q, want %q", got, "203.0.113.10")
	}
}

func TestClientRemoteAddrFallsBackToRemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/audit", nil)
	req.RemoteAddr = "172.18.0.3:54321"

	if got := clientRemoteAddr(req); got != "172.18.0.3" {
		t.Fatalf("clientRemoteAddr() = %q, want %q", got, "172.18.0.3")
	}
}
