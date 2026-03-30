package ntrip

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"hdcaster/internal/app"
	"hdcaster/internal/runtime"
	"hdcaster/internal/storage"
)

type Server struct {
	addr     string
	svc      *app.Service
	log      *log.Logger
	onListen func(net.Addr)
}

func NewServer(addr string, svc *app.Service, logger *log.Logger) *Server {
	return &Server{addr: addr, svc: svc, log: logger}
}

func (s *Server) OnListen(fn func(net.Addr)) *Server {
	s.onListen = fn
	return s
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.log.Printf("ntrip listening on %s", s.addr)
	if s.onListen != nil {
		s.onListen(ln.Addr())
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	req, reader, err := ReadRequest(conn)
	if err != nil {
		s.log.Printf("read request error: %v", err)
		return
	}

	if s.svc.CheckBlocked(req.RemoteAddr) {
		writeError(conn, req.Version, req.Method, 403, "Forbidden")
		return
	}

	switch {
	case req.Method == "GET" && req.Path == "/":
		s.handleSourcetable(conn, req)
	case req.Method == "GET":
		s.handleClient(conn, req, reader)
	case req.Method == "POST" || req.Method == "SOURCE":
		s.handleSource(conn, req, reader)
	default:
		writeError(conn, req.Version, req.Method, 405, "Method Not Allowed")
	}
}

func (s *Server) handleSourcetable(conn net.Conn, req *Request) {
	body := s.svc.Sourcetable()
	if req.Version == Version1 {
		fmt.Fprintf(conn, "SOURCETABLE 200 OK\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
		return
	}
	fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\nContent-Length: %d\r\nNtrip-Version: Ntrip/2.0\r\n\r\n%s", len(body), body)
}

func (s *Server) handleClient(conn net.Conn, req *Request, reader *bufio.Reader) {
	mount := strings.TrimPrefix(req.Path, "/")
	if err := s.svc.AuthenticateClient(req.RemoteAddr, req.Username, req.Password, mount); err != nil {
		writeAuthError(conn, req.Version, req.Method, err)
		return
	}
	if s.svc.HasRelayMount(mount) {
		handle, ch, err := s.svc.AttachRelayClient(mount, req.Username, req.RemoteAddr)
		if err != nil {
			writeError(conn, req.Version, req.Method, 404, "Mount Not Available")
			return
		}
		defer handle.Close()

		if req.Version == Version1 {
			fmt.Fprint(conn, "ICY 200 OK\r\n\r\n")
		} else {
			fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nContent-Type: gnss/data\r\nConnection: close\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
		}

		go consumeClientBackchannel(reader, func(sentence string) {
			s.svc.UpdateRelayClientGGA(handle.ID(), sentence)
		})
		if err := runtime.StreamToWriter(ch, conn); err != nil && !errors.Is(err, io.EOF) {
			s.log.Printf("relay client stream closed: %v", err)
		}
		return
	}
	handle, ch, err := s.svc.Hub().RegisterClient(mount, req.Username, req.RemoteAddr)
	if err != nil {
		writeError(conn, req.Version, req.Method, 404, "Mount Not Available")
		return
	}
	defer func() {
		handle.Close()
	}()

	if req.Version == Version1 {
		fmt.Fprint(conn, "ICY 200 OK\r\n\r\n")
	} else {
		fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nContent-Type: gnss/data\r\nConnection: close\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
	}

	go consumeClientBackchannel(reader, func(sentence string) {
		_ = sentence
	})
	if err := runtime.StreamToWriter(ch, conn); err != nil && !errors.Is(err, io.EOF) {
		s.log.Printf("client stream closed: %v", err)
	}
}

func (s *Server) handleSource(conn net.Conn, req *Request, reader *bufio.Reader) {
	mount := strings.TrimPrefix(req.Path, "/")
	if err := s.svc.AuthenticateSource(req.RemoteAddr, req.Username, req.Password, req.Secret, mount); err != nil {
		writeAuthError(conn, req.Version, req.Method, err)
		return
	}
	handle, err := s.svc.Hub().RegisterSource(mount, s.svc.ResolveSourceUsername(req.Username, req.Password, req.Secret, mount), req.RemoteAddr)
	if err != nil {
		writeError(conn, req.Version, req.Method, 409, "Mount Busy")
		return
	}
	s.svc.AppendAuditEvent(storage.AuditEvent{
		Actor:      handle.Snapshot().Username,
		Action:     "source.connect",
		Resource:   "mountpoint",
		ResourceID: mount,
		Status:     "ok",
		RemoteAddr: req.RemoteAddr,
		Message:    "source connected",
	})
	defer func() {
		snap := handle.Snapshot()
		s.svc.AppendAuditEvent(storage.AuditEvent{
			Actor:      snap.Username,
			Action:     "source.disconnect",
			Resource:   "mountpoint",
			ResourceID: snap.Mount,
			Status:     "ok",
			RemoteAddr: snap.RemoteAddr,
			Message:    "source disconnected",
			Details: map[string]string{
				"bytes_in":     strconv.FormatUint(snap.BytesIn, 10),
				"bytes_out":    strconv.FormatUint(snap.BytesOut, 10),
				"client_count": strconv.Itoa(snap.ClientCount),
			},
		})
		handle.Close()
	}()

	if req.Version == Version1 {
		fmt.Fprint(conn, "ICY 200 OK\r\n\r\n")
	} else {
		fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nConnection: close\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
	}

	buf := make([]byte, 4096)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		n, err := reader.Read(buf)
		if n > 0 {
			handle.Publish(buf[:n])
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.log.Printf("source stream closed: %v", err)
			}
			return
		}
	}
}

func writeAuthError(conn net.Conn, version int, method string, err error) {
	if errors.Is(err, app.ErrForbiddenIP) {
		writeError(conn, version, method, 403, "Forbidden")
		return
	}
	writeError(conn, version, method, 401, "Unauthorized")
}

func writeError(conn net.Conn, version int, method string, status int, text string) {
	if version == Version1 {
		switch {
		case method == "SOURCE" && status == 401:
			fmt.Fprint(conn, "ERROR - Bad Password\r\n")
		case method == "SOURCE":
			fmt.Fprint(conn, "ERROR - Bad Mountpoint\r\n")
		case status == 401:
			fmt.Fprint(conn, "HTTP/1.0 401 Unauthorized\r\n\r\n")
		case status == 403:
			fmt.Fprint(conn, "HTTP/1.0 403 Forbidden\r\n\r\n")
		case status == 404:
			fmt.Fprint(conn, "SOURCETABLE 200 OK\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: 17\r\n\r\nENDSOURCETABLE\r\n")
		default:
			fmt.Fprintf(conn, "HTTP/1.0 %d %s\r\n\r\n", status, text)
		}
		return
	}
	fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\nConnection: close\r\nNtrip-Version: Ntrip/2.0\r\n\r\n", status, text)
}

func consumeClientBackchannel(reader *bufio.Reader, onGGA func(string)) {
	if onGGA == nil {
		_, _ = io.Copy(io.Discard, reader)
		return
	}
	
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 256)
	scanner.Buffer(buf, 4096)
	
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			if strings.HasPrefix(line, "$") && strings.Contains(strings.ToUpper(line), "GGA") {
				onGGA(line)
			}
		}
	}
	
	if err := scanner.Err(); err != nil {
		return
	}
}
