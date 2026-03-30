package ntrip

import (
	"encoding/base64"
	"net"
	"testing"
)

func TestReadRequestRev1Source(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		client.Write([]byte("SOURCE secret /MOUNT\r\nSource-Agent: NTRIP test\r\n\r\n"))
	}()

	req, _, err := ReadRequest(server)
	if err != nil {
		t.Fatalf("ReadRequest() error = %v", err)
	}
	if req.Method != "SOURCE" || req.Secret != "secret" || req.Path != "/MOUNT" || req.Version != Version1 {
		t.Fatalf("unexpected request: %+v", req)
	}
}

func TestReadRequestRev2Get(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	auth := base64.StdEncoding.EncodeToString([]byte("alice:pw"))
	go func() {
		client.Write([]byte("GET /MOUNT HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nAuthorization: Basic " + auth + "\r\n\r\n"))
	}()

	req, _, err := ReadRequest(server)
	if err != nil {
		t.Fatalf("ReadRequest() error = %v", err)
	}
	if req.Version != Version2 || req.Username != "alice" || req.Password != "pw" {
		t.Fatalf("unexpected request: %+v", req)
	}
}
