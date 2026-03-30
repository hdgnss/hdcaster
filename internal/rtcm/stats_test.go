package rtcm

import (
	"math"
	"slices"
	"testing"
)

func TestStatsConsumeDecodesReferenceStationMetadata(t *testing.T) {
	xMeters := 1113194.907
	yMeters := 1113194.907
	zMeters := 0.0

	payload := buildMessage1006Payload(42, int64(math.Round(xMeters*10000)), int64(math.Round(yMeters*10000)), int64(math.Round(zMeters*10000)), int64(1850))
	frame := append([]byte{0xD3, byte(len(payload) >> 8), byte(len(payload))}, payload...)
	frame = append(frame, 0, 0, 0)

	stats := NewStats()
	stats.Consume(frame)
	snap := stats.Snapshot()

	if snap.FramesObserved != 1 {
		t.Fatalf("expected 1 frame, got %d", snap.FramesObserved)
	}
	if snap.MessageCounts[1006] != 1 {
		t.Fatalf("expected message 1006 count 1, got %+v", snap.MessageCounts)
	}
	if snap.StationGeo == nil {
		t.Fatal("expected decoded station geo position")
	}
	if snap.StationECEF == nil {
		t.Fatal("expected decoded station ecef position")
	}
	if snap.Reference == nil {
		t.Fatal("expected reference metadata")
	}
	if snap.Reference.StationID != 42 {
		t.Fatalf("unexpected station id: %+v", snap.Reference)
	}
	if !snap.Reference.GPSIndicator || !snap.Reference.GLONASSIndicator || !snap.Reference.GalileoIndicator || !snap.Reference.ReferenceStationIndicator {
		t.Fatalf("expected reference flags to be set: %+v", snap.Reference)
	}
	if !snap.Reference.SingleReceiverOscillator {
		t.Fatalf("expected single receiver oscillator flag to be set: %+v", snap.Reference)
	}
	if snap.Reference.ITRFYear != 0 {
		t.Fatalf("unexpected ITRF year: %+v", snap.Reference)
	}
	if snap.Reference.QuarterCycleIndicator != 0 {
		t.Fatalf("unexpected quarter cycle indicator: %+v", snap.Reference)
	}
	if snap.Reference.AntennaHeightMeters == nil || math.Abs(*snap.Reference.AntennaHeightMeters-0.185) > 0.0001 {
		t.Fatalf("unexpected antenna height: %+v", snap.Reference.AntennaHeightMeters)
	}
	if math.Abs(snap.StationECEF.X-xMeters) > 0.001 || math.Abs(snap.StationECEF.Y-yMeters) > 0.001 || math.Abs(snap.StationECEF.Z-zMeters) > 0.001 {
		t.Fatalf("unexpected ecef position: %+v", snap.StationECEF)
	}
	if math.Abs(snap.StationGeo.Latitude) > 0.01 {
		t.Fatalf("expected near-equator latitude, got %f", snap.StationGeo.Latitude)
	}
	if math.Abs(snap.StationGeo.Longitude-45.0) > 0.01 {
		t.Fatalf("expected near 45 degrees longitude, got %f", snap.StationGeo.Longitude)
	}
}

func TestStatsConsumeDecodesReferenceStationPosition1005(t *testing.T) {
	xMeters := 1113194.907
	yMeters := 1113194.907
	zMeters := 0.0

	payload := buildMessage1005Payload(7, int64(math.Round(xMeters*10000)), int64(math.Round(yMeters*10000)), int64(math.Round(zMeters*10000)))
	frame := frameFromPayload(payload)

	stats := NewStats()
	stats.Consume(frame)
	snap := stats.Snapshot()

	if snap.Reference == nil {
		t.Fatal("expected reference metadata")
	}
	if snap.Reference.StationID != 7 {
		t.Fatalf("unexpected station id: %+v", snap.Reference)
	}
	if snap.Reference.AntennaHeightMeters != nil {
		t.Fatalf("did not expect antenna height in 1005 message: %+v", snap.Reference.AntennaHeightMeters)
	}
	if snap.StationGeo == nil || snap.StationECEF == nil {
		t.Fatalf("expected decoded station position: %+v", snap)
	}
}

func TestStatsConsumeDecodesDescriptorMetadata(t *testing.T) {
	stats := NewStats()
	stats.Consume(frameFromPayload(buildMessage1007Payload(7, "TRM59900.00", 3)))
	stats.Consume(frameFromPayload(buildMessage1008Payload(7, "TRM59900.00", 3, "ANT-12345")))
	stats.Consume(frameFromPayload(buildMessage1033Payload(7, "TRM59900.00", 3, "ANT-12345", "Septentrio mosaic-X5", "fw-1.2.3", "RX-9988")))

	snap := stats.Snapshot()
	if snap.Reference == nil {
		t.Fatal("expected reference metadata")
	}
	if snap.Reference.StationID != 7 {
		t.Fatalf("unexpected station id: %+v", snap.Reference)
	}
	if snap.Reference.AntennaDescriptor != "TRM59900.00" {
		t.Fatalf("unexpected antenna descriptor: %+v", snap.Reference)
	}
	if snap.Reference.AntennaSetupID != 3 {
		t.Fatalf("unexpected antenna setup id: %+v", snap.Reference)
	}
	if snap.Reference.AntennaSerial != "ANT-12345" {
		t.Fatalf("unexpected antenna serial: %+v", snap.Reference)
	}
	if snap.Reference.ReceiverDescriptor != "Septentrio mosaic-X5" {
		t.Fatalf("unexpected receiver descriptor: %+v", snap.Reference)
	}
	if snap.Reference.ReceiverType != "Septentrio mosaic-X5" {
		t.Fatalf("unexpected receiver type alias: %+v", snap.Reference)
	}
	if snap.Reference.ReceiverFirmware != "fw-1.2.3" {
		t.Fatalf("unexpected receiver firmware: %+v", snap.Reference)
	}
	if snap.Reference.ReceiverSerial != "RX-9988" {
		t.Fatalf("unexpected receiver serial: %+v", snap.Reference)
	}
}

func TestStatsSnapshotClassifiesMSMFamilies(t *testing.T) {
	stats := NewStats()
	for _, messageType := range []int{1074, 1075, 1087, 1125, 1137} {
		stats.Consume(frameFromPayload(buildMessagePayload(messageType)))
	}

	snap := stats.Snapshot()
	if !slices.Contains(snap.MSMClasses, "GPS MSM4") {
		t.Fatalf("expected GPS MSM4 in summary: %+v", snap.MSMClasses)
	}
	if !slices.Contains(snap.MSMClasses, "GPS MSM5") {
		t.Fatalf("expected GPS MSM5 in summary: %+v", snap.MSMClasses)
	}
	if len(snap.MSMFamilies) != 4 {
		t.Fatalf("expected 4 MSM families, got %+v", snap.MSMFamilies)
	}
	if got := findMSMFamily(snap.MSMFamilies, "GPS"); got == nil || !slices.Contains(got.MSMClasses, "MSM4") || !slices.Contains(got.MSMClasses, "MSM5") {
		t.Fatalf("unexpected GPS MSM family summary: %+v", got)
	}
}

func frameFromPayload(payload []byte) []byte {
	frame := append([]byte{0xD3, byte(len(payload) >> 8), byte(len(payload))}, payload...)
	return append(frame, 0, 0, 0)
}

func buildMessagePayload(messageType int) []byte {
	bits := newBitWriter()
	bits.writeUnsigned(uint64(messageType), 12)
	bits.writeUnsigned(0, 12)
	return bits.bytes()
}

func buildMessage1006Payload(stationID uint16, x, y, z, height int64) []byte {
	bits := newBitWriter()
	bits.writeUnsigned(1006, 12)
	bits.writeUnsigned(uint64(stationID), 12)
	bits.writeUnsigned(0, 6)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(1, 1)
	bits.writeSigned(x, 38)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(0, 1)
	bits.writeSigned(y, 38)
	bits.writeUnsigned(0, 2)
	bits.writeSigned(z, 38)
	bits.writeSigned(height, 16)
	return bits.bytes()
}

func buildMessage1005Payload(stationID uint16, x, y, z int64) []byte {
	bits := newBitWriter()
	bits.writeUnsigned(1005, 12)
	bits.writeUnsigned(uint64(stationID), 12)
	bits.writeUnsigned(0, 6)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(1, 1)
	bits.writeSigned(x, 38)
	bits.writeUnsigned(1, 1)
	bits.writeUnsigned(0, 1)
	bits.writeSigned(y, 38)
	bits.writeUnsigned(0, 2)
	bits.writeSigned(z, 38)
	return bits.bytes()
}

func buildMessage1007Payload(stationID uint16, descriptor string, setupID uint8) []byte {
	bits := newBitWriter()
	bits.writeUnsigned(1007, 12)
	bits.writeUnsigned(uint64(stationID), 12)
	bits.writeASCII(descriptor)
	bits.writeUnsigned(uint64(setupID), 8)
	return bits.bytes()
}

func buildMessage1008Payload(stationID uint16, descriptor string, setupID uint8, serial string) []byte {
	bits := newBitWriter()
	bits.writeUnsigned(1008, 12)
	bits.writeUnsigned(uint64(stationID), 12)
	bits.writeASCII(descriptor)
	bits.writeUnsigned(uint64(setupID), 8)
	bits.writeASCII(serial)
	return bits.bytes()
}

func buildMessage1033Payload(stationID uint16, antennaDescriptor string, setupID uint8, antennaSerial, receiverDescriptor, firmware, receiverSerial string) []byte {
	bits := newBitWriter()
	bits.writeUnsigned(1033, 12)
	bits.writeUnsigned(uint64(stationID), 12)
	bits.writeASCII(antennaDescriptor)
	bits.writeUnsigned(uint64(setupID), 8)
	bits.writeASCII(antennaSerial)
	bits.writeASCII(receiverDescriptor)
	bits.writeASCII(firmware)
	bits.writeASCII(receiverSerial)
	return bits.bytes()
}

type bitWriter struct {
	bits []byte
}

func newBitWriter() *bitWriter {
	return &bitWriter{}
}

func (w *bitWriter) writeUnsigned(value uint64, width int) {
	for i := width - 1; i >= 0; i-- {
		w.bits = append(w.bits, byte((value>>i)&1))
	}
}

func (w *bitWriter) writeSigned(value int64, width int) {
	var encoded uint64
	if value < 0 {
		encoded = (uint64(1) << width) + uint64(value)
	} else {
		encoded = uint64(value)
	}
	w.writeUnsigned(encoded, width)
}

func (w *bitWriter) writeASCII(value string) {
	w.writeUnsigned(uint64(len(value)), 8)
	for i := 0; i < len(value); i++ {
		w.writeUnsigned(uint64(value[i]), 8)
	}
}

func (w *bitWriter) bytes() []byte {
	out := make([]byte, (len(w.bits)+7)/8)
	for i, bit := range w.bits {
		if bit == 0 {
			continue
		}
		out[i/8] |= 1 << (7 - (i % 8))
	}
	return out
}

func findMSMFamily(families []MSMFamilySummary, system string) *MSMFamilySummary {
	for i := range families {
		if families[i].System == system {
			return &families[i]
		}
	}
	return nil
}
