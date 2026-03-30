package ntrip

import (
	"encoding/json"
	"sort"
	"sync"
)

type RTCMStats struct {
	mu             sync.Mutex
	buffer         []byte
	MessageCounts  map[int]int `json:"message_counts"`
	BytesObserved  uint64      `json:"bytes_observed"`
	FramesObserved uint64      `json:"frames_observed"`
}

func NewRTCMStats() *RTCMStats {
	return &RTCMStats{
		MessageCounts: make(map[int]int),
	}
}

func (s *RTCMStats) Consume(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.BytesObserved += uint64(len(data))
	s.buffer = append(s.buffer, data...)

	for {
		if len(s.buffer) < 6 {
			return
		}
		if s.buffer[0] != 0xD3 {
			s.buffer = s.buffer[1:]
			continue
		}

		length := int(s.buffer[1]&0x03)<<8 | int(s.buffer[2])
		frameLen := 3 + length + 3
		if len(s.buffer) < frameLen {
			return
		}
		if length >= 2 {
			msg := int(s.buffer[3])<<4 | int(s.buffer[4]>>4)
			s.MessageCounts[msg]++
			s.FramesObserved++
		}
		s.buffer = s.buffer[frameLen:]
		if len(s.buffer) > 8192 {
			s.buffer = append([]byte(nil), s.buffer[len(s.buffer)-4096:]...)
		}
	}
}

func (s *RTCMStats) Snapshot() RTCMSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(map[int]int, len(s.MessageCounts))
	for k, v := range s.MessageCounts {
		counts[k] = v
	}
	sorted := make([]int, 0, len(counts))
	for k := range counts {
		sorted = append(sorted, k)
	}
	sort.Ints(sorted)

	return RTCMSnapshot{
		MessageCounts:  counts,
		MessageTypes:   sorted,
		Constellations: inferConstellations(sorted),
		BytesObserved:  s.BytesObserved,
		FramesObserved: s.FramesObserved,
	}
}

type RTCMSnapshot struct {
	MessageCounts  map[int]int `json:"message_counts"`
	MessageTypes   []int       `json:"message_types"`
	Constellations []string    `json:"constellations"`
	BytesObserved  uint64      `json:"bytes_observed"`
	FramesObserved uint64      `json:"frames_observed"`
}

func (s RTCMSnapshot) MarshalJSON() ([]byte, error) {
	type alias RTCMSnapshot
	return json.Marshal(alias(s))
}

func inferConstellations(messageTypes []int) []string {
	set := map[string]struct{}{}
	for _, mt := range messageTypes {
		switch {
		case mt == 1005 || mt == 1006 || mt == 1007 || mt == 1008 || mt == 1033:
			set["Reference Station"] = struct{}{}
		case mt >= 1071 && mt <= 1127:
			set["GPS"] = struct{}{}
		case mt >= 1081 && mt <= 1087:
			set["GLONASS"] = struct{}{}
		case mt >= 1091 && mt <= 1097:
			set["Galileo"] = struct{}{}
		case mt >= 1101 && mt <= 1107:
			set["SBAS"] = struct{}{}
		case mt >= 1111 && mt <= 1117:
			set["QZSS"] = struct{}{}
		case mt >= 1121 && mt <= 1127:
			set["BeiDou"] = struct{}{}
		case mt >= 1131 && mt <= 1137:
			set["NavIC"] = struct{}{}
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
