package ntrip

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"time"
)

const (
	Version1 = 1
	Version2 = 2
)

type Request struct {
	Version    int
	Method     string
	Path       string
	Header     textproto.MIMEHeader
	RemoteAddr string
	Username   string
	Password   string
	Secret     string
}

func ReadRequest(conn net.Conn) (*Request, *bufio.Reader, error) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, nil, errors.New("empty request line")
	}

	req := &Request{
		Version:    Version1,
		Header:     make(textproto.MIMEHeader),
		RemoteAddr: conn.RemoteAddr().String(),
	}

	parts := strings.Split(line, " ")
	switch {
	case len(parts) >= 3 && strings.EqualFold(parts[0], "SOURCE"):
		req.Method = "SOURCE"
		req.Secret = parts[1]
		req.Path = normalizePath(parts[2])
		req.Version = Version1
	case len(parts) >= 3:
		req.Method = strings.ToUpper(parts[0])
		req.Path = normalizePath(parts[1])
	default:
		return nil, nil, fmt.Errorf("invalid request line: %q", line)
	}

	tp := textproto.NewReader(reader)
	header, err := tp.ReadMIMEHeader()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	req.Header = header
	if strings.EqualFold(req.Header.Get("Ntrip-Version"), "Ntrip/2.0") {
		req.Version = Version2
	}
	req.Username, req.Password = parseBasicAuth(req.Header.Get("Authorization"))

	_ = conn.SetReadDeadline(time.Time{})
	return req, reader, nil
}

func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func parseBasicAuth(auth string) (string, string) {
	if auth == "" {
		return "", ""
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Basic") {
		return "", ""
	}
	raw, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ""
	}
	pair := strings.SplitN(string(raw), ":", 2)
	if len(pair) != 2 {
		return "", ""
	}
	return pair[0], pair[1]
}
