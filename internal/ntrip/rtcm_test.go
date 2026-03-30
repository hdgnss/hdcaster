package ntrip

import "testing"

func TestRTCMStatsConsume(t *testing.T) {
	stats := NewRTCMStats()
	frame := []byte{
		0xD3, 0x00, 0x02,
		0x43, 0x50,
		0x00, 0x00, 0x00,
	}
	stats.Consume(frame)
	snap := stats.Snapshot()
	if snap.FramesObserved != 1 {
		t.Fatalf("expected 1 frame, got %d", snap.FramesObserved)
	}
	if snap.MessageCounts[1077] != 1 {
		t.Fatalf("expected message 1077 count 1, got %+v", snap.MessageCounts)
	}
}
