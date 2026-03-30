package rtcm

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"sync"
)

type Stats struct {
	mu             sync.Mutex
	buffer         []byte
	MessageCounts  map[int]int       `json:"message_counts"`
	BytesObserved  uint64            `json:"bytes_observed"`
	FramesObserved uint64            `json:"frames_observed"`
	StationECEF    *ECEF             `json:"station_ecef,omitempty"`
	StationGeo     *GeoPoint         `json:"station_geo,omitempty"`
	Reference      *ReferenceStation `json:"reference_station,omitempty"`
}

func NewStats() *Stats {
	return &Stats{MessageCounts: make(map[int]int)}
}

func (s *Stats) Consume(data []byte) {
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
			payload := s.buffer[3 : 3+length]
			msg := int(payload[0])<<4 | int(payload[1]>>4)
			s.MessageCounts[msg]++
			s.FramesObserved++
			s.applyReferenceStationMessage(msg, payload)
		}
		s.buffer = s.buffer[frameLen:]
		if len(s.buffer) > 8192 {
			s.buffer = append([]byte(nil), s.buffer[len(s.buffer)-4096:]...)
		}
	}
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(map[int]int, len(s.MessageCounts))
	for k, v := range s.MessageCounts {
		counts[k] = v
	}
	types := make([]int, 0, len(counts))
	for k := range counts {
		types = append(types, k)
	}
	sort.Ints(types)
	return Snapshot{
		MessageCounts:  counts,
		MessageTypes:   types,
		Constellations: inferConstellations(types),
		MSMClasses:     inferMSMClasses(types),
		MSMFamilies:    inferMSMFamilies(types),
		BytesObserved:  s.BytesObserved,
		FramesObserved: s.FramesObserved,
		StationECEF:    cloneECEF(s.StationECEF),
		StationGeo:     cloneGeoPoint(s.StationGeo),
		Reference:      cloneReferenceStation(s.Reference),
	}
}

type Snapshot struct {
	MessageCounts  map[int]int        `json:"message_counts"`
	MessageTypes   []int              `json:"message_types"`
	Constellations []string           `json:"constellations"`
	MSMClasses     []string           `json:"msm_classes,omitempty"`
	MSMFamilies    []MSMFamilySummary `json:"msm_families,omitempty"`
	BytesObserved  uint64             `json:"bytes_observed"`
	FramesObserved uint64             `json:"frames_observed"`
	StationECEF    *ECEF              `json:"station_ecef,omitempty"`
	StationGeo     *GeoPoint          `json:"station_geo,omitempty"`
	Reference      *ReferenceStation  `json:"reference_station,omitempty"`
}

type ReferenceStation struct {
	StationID                 int      `json:"station_id,omitempty"`
	ITRFYear                  int      `json:"itrf_year,omitempty"`
	GPSIndicator              bool     `json:"gps_indicator,omitempty"`
	GLONASSIndicator          bool     `json:"glonass_indicator,omitempty"`
	GalileoIndicator          bool     `json:"galileo_indicator,omitempty"`
	ReferenceStationIndicator bool     `json:"reference_station_indicator,omitempty"`
	SingleReceiverOscillator  bool     `json:"single_receiver_oscillator,omitempty"`
	QuarterCycleIndicator     int      `json:"quarter_cycle_indicator,omitempty"`
	AntennaDescriptor         string   `json:"antenna_descriptor,omitempty"`
	AntennaSetupID            int      `json:"antenna_setup_id,omitempty"`
	AntennaSerial             string   `json:"antenna_serial,omitempty"`
	ReceiverDescriptor        string   `json:"receiver_descriptor,omitempty"`
	ReceiverType              string   `json:"receiver_type,omitempty"`
	ReceiverFirmware          string   `json:"receiver_firmware,omitempty"`
	ReceiverSerial            string   `json:"receiver_serial,omitempty"`
	AntennaHeightMeters       *float64 `json:"antenna_height_meters,omitempty"`
}

type MSMFamilySummary struct {
	System       string   `json:"system"`
	MessageTypes []int    `json:"message_types"`
	MSMClasses   []string `json:"msm_classes,omitempty"`
}

type ECEF struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type GeoPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude"`
}

func (s Snapshot) MarshalJSON() ([]byte, error) {
	type alias Snapshot
	return json.Marshal(alias(s))
}

func inferConstellations(messageTypes []int) []string {
	set := map[string]struct{}{}
	for _, mt := range messageTypes {
		switch {
		case mt == 1005 || mt == 1006 || mt == 1007 || mt == 1008 || mt == 1033:
			set["Reference Station"] = struct{}{}
		case mt >= 1071 && mt <= 1077:
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

func inferMSMClasses(messageTypes []int) []string {
	classes := map[string]struct{}{}
	for _, mt := range messageTypes {
		system, msm, ok := msmClassForMessage(mt)
		if !ok {
			continue
		}
		classes[system+" MSM"+msm] = struct{}{}
	}
	out := make([]string, 0, len(classes))
	for item := range classes {
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func inferMSMFamilies(messageTypes []int) []MSMFamilySummary {
	type familyState struct {
		system  string
		types   map[int]struct{}
		classes map[string]struct{}
	}
	families := map[string]*familyState{}
	for _, mt := range messageTypes {
		system, msm, ok := msmClassForMessage(mt)
		if !ok {
			continue
		}
		state, exists := families[system]
		if !exists {
			state = &familyState{
				system:  system,
				types:   make(map[int]struct{}),
				classes: make(map[string]struct{}),
			}
			families[system] = state
		}
		state.types[mt] = struct{}{}
		state.classes["MSM"+msm] = struct{}{}
	}
	out := make([]MSMFamilySummary, 0, len(families))
	for _, state := range families {
		item := MSMFamilySummary{System: state.system}
		for mt := range state.types {
			item.MessageTypes = append(item.MessageTypes, mt)
		}
		sort.Ints(item.MessageTypes)
		for class := range state.classes {
			item.MSMClasses = append(item.MSMClasses, class)
		}
		sort.Strings(item.MSMClasses)
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].System < out[j].System
	})
	return out
}

func msmClassForMessage(messageType int) (system string, msm string, ok bool) {
	switch {
	case messageType >= 1071 && messageType <= 1077:
		return "GPS", messageTypeStringMSM(messageType), true
	case messageType >= 1081 && messageType <= 1087:
		return "GLONASS", messageTypeStringMSM(messageType), true
	case messageType >= 1091 && messageType <= 1097:
		return "Galileo", messageTypeStringMSM(messageType), true
	case messageType >= 1101 && messageType <= 1107:
		return "SBAS", messageTypeStringMSM(messageType), true
	case messageType >= 1111 && messageType <= 1117:
		return "QZSS", messageTypeStringMSM(messageType), true
	case messageType >= 1121 && messageType <= 1127:
		return "BeiDou", messageTypeStringMSM(messageType), true
	case messageType >= 1131 && messageType <= 1137:
		return "NavIC", messageTypeStringMSM(messageType), true
	default:
		return "", "", false
	}
}

func messageTypeStringMSM(messageType int) string {
	return string(rune('0' + (messageType % 10)))
}

func (s *Stats) applyReferenceStationMessage(messageType int, payload []byte) {
	switch messageType {
	case 1005, 1006:
		ref, geo, ecef, ok := decodeReferenceStationPosition(messageType, payload)
		if !ok {
			return
		}
		base := cloneReferenceStation(s.Reference)
		if base == nil {
			base = &ReferenceStation{}
		}
		base.StationID = ref.StationID
		base.ITRFYear = ref.ITRFYear
		base.GPSIndicator = ref.GPSIndicator
		base.GLONASSIndicator = ref.GLONASSIndicator
		base.GalileoIndicator = ref.GalileoIndicator
		base.ReferenceStationIndicator = ref.ReferenceStationIndicator
		base.SingleReceiverOscillator = ref.SingleReceiverOscillator
		base.QuarterCycleIndicator = ref.QuarterCycleIndicator
		if ref.AntennaHeightMeters != nil {
			height := *ref.AntennaHeightMeters
			base.AntennaHeightMeters = &height
		}
		s.Reference = base
		s.StationGeo = geo
		s.StationECEF = ecef
	case 1007, 1008, 1033:
		ref, ok := decodeDescriptorMessage(messageType, payload)
		if !ok {
			return
		}
		base := cloneReferenceStation(s.Reference)
		if base == nil {
			base = &ReferenceStation{}
		}
		if ref.StationID != 0 {
			base.StationID = ref.StationID
		}
		if ref.AntennaDescriptor != "" {
			base.AntennaDescriptor = ref.AntennaDescriptor
		}
		if ref.AntennaSetupID != 0 {
			base.AntennaSetupID = ref.AntennaSetupID
		}
		if ref.AntennaSerial != "" {
			base.AntennaSerial = ref.AntennaSerial
		}
		if ref.ReceiverDescriptor != "" {
			base.ReceiverDescriptor = ref.ReceiverDescriptor
			base.ReceiverType = ref.ReceiverDescriptor
		}
		if ref.ReceiverFirmware != "" {
			base.ReceiverFirmware = ref.ReceiverFirmware
		}
		if ref.ReceiverSerial != "" {
			base.ReceiverSerial = ref.ReceiverSerial
		}
		s.Reference = base
	}
}

func decodeReferenceStationPosition(messageType int, payload []byte) (*ReferenceStation, *GeoPoint, *ECEF, bool) {
	if messageType != 1005 && messageType != 1006 {
		return nil, nil, nil, false
	}

	const (
		messageBits = 12
		stationID   = 12
		itrfYear    = 6
		flagBits    = 1
		xBits       = 38
		yBits       = 38
		zBits       = 38
	)

	offset := messageBits
	stationRaw, ok := readBits(payload, offset, stationID)
	if !ok {
		return nil, nil, nil, false
	}
	offset += stationID
	itrfYearRaw, ok := readBits(payload, offset, itrfYear)
	if !ok {
		return nil, nil, nil, false
	}
	offset += itrfYear
	gpsIndicator, ok := readBits(payload, offset, flagBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += flagBits
	glonassIndicator, ok := readBits(payload, offset, flagBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += flagBits
	galileoIndicator, ok := readBits(payload, offset, flagBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += flagBits
	referenceStationIndicator, ok := readBits(payload, offset, flagBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += flagBits
	xRaw, ok := readSignedBits(payload, offset, xBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += xBits
	singleReceiverOscillator, ok := readBits(payload, offset, flagBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += flagBits
	_, ok = readBits(payload, offset, flagBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += flagBits
	yRaw, ok := readSignedBits(payload, offset, yBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += yBits
	quarterCycleIndicator, ok := readBits(payload, offset, 2)
	if !ok {
		return nil, nil, nil, false
	}
	offset += 2
	zRaw, ok := readSignedBits(payload, offset, zBits)
	if !ok {
		return nil, nil, nil, false
	}
	offset += zBits

	ecef := &ECEF{
		X: float64(xRaw) * 0.0001,
		Y: float64(yRaw) * 0.0001,
		Z: float64(zRaw) * 0.0001,
	}
	geo := ecefToGeodetic(ecef)
	if geo == nil {
		return nil, nil, nil, false
	}
	ref := &ReferenceStation{
		StationID:                 int(stationRaw),
		ITRFYear:                  int(itrfYearRaw),
		GPSIndicator:              gpsIndicator != 0,
		GLONASSIndicator:          glonassIndicator != 0,
		GalileoIndicator:          galileoIndicator != 0,
		ReferenceStationIndicator: referenceStationIndicator != 0,
		SingleReceiverOscillator:  singleReceiverOscillator != 0,
		QuarterCycleIndicator:     int(quarterCycleIndicator),
	}
	if messageType == 1006 {
		heightRaw, ok := readBits(payload, offset, 16)
		if !ok {
			return nil, nil, nil, false
		}
		antennaHeight := float64(heightRaw) * 0.0001
		ref.AntennaHeightMeters = &antennaHeight
	}
	return ref, geo, ecef, true
}

func decodeDescriptorMessage(messageType int, payload []byte) (*ReferenceStation, bool) {
	switch messageType {
	case 1007:
		return decodeAntennaDescriptorMessage(payload, false)
	case 1008:
		return decodeAntennaDescriptorMessage(payload, true)
	case 1033:
		return decodeMsg1033(payload)
	default:
		return nil, false
	}
}

func decodeAntennaDescriptorMessage(payload []byte, withSerial bool) (*ReferenceStation, bool) {
	ref := &ReferenceStation{}
	stationID, ok := readBits(payload, 12, 12)
	if !ok {
		return nil, false
	}
	ref.StationID = int(stationID)
	descriptor, nextOffset, ok := readLengthPrefixedASCII(payload, 24)
	if !ok {
		return nil, false
	}
	ref.AntennaDescriptor = descriptor
	setupID, ok := readBits(payload, nextOffset, 8)
	if !ok {
		return nil, false
	}
	ref.AntennaSetupID = int(setupID)
	offset := nextOffset + 8
	if !withSerial {
		return ref, true
	}
	serial, _, ok := readLengthPrefixedASCII(payload, offset)
	if !ok {
		return nil, false
	}
	ref.AntennaSerial = serial
	return ref, true
}

func decodeMsg1033(payload []byte) (*ReferenceStation, bool) {
	ref := &ReferenceStation{}
	stationID, ok := readBits(payload, 12, 12)
	if !ok {
		return nil, false
	}
	ref.StationID = int(stationID)
	descriptor, nextOffset, ok := readLengthPrefixedASCII(payload, 24)
	if !ok {
		return nil, false
	}
	ref.AntennaDescriptor = descriptor
	setupID, ok := readBits(payload, nextOffset, 8)
	if !ok {
		return nil, false
	}
	ref.AntennaSetupID = int(setupID)
	offset := nextOffset + 8
	serial, nextOffset, ok := readLengthPrefixedASCII(payload, offset)
	if !ok {
		return nil, false
	}
	ref.AntennaSerial = serial
	receiverDescriptor, nextOffset, ok := readLengthPrefixedASCII(payload, nextOffset)
	if !ok {
		return nil, false
	}
	ref.ReceiverDescriptor = receiverDescriptor
	ref.ReceiverType = receiverDescriptor
	receiverFirmware, nextOffset, ok := readLengthPrefixedASCII(payload, nextOffset)
	if !ok {
		return nil, false
	}
	ref.ReceiverFirmware = receiverFirmware
	receiverSerial, _, ok := readLengthPrefixedASCII(payload, nextOffset)
	if !ok {
		return nil, false
	}
	ref.ReceiverSerial = receiverSerial
	return ref, true
}

func readLengthPrefixedASCII(data []byte, bitOffset int) (string, int, bool) {
	length, ok := readBits(data, bitOffset, 8)
	if !ok {
		return "", bitOffset, false
	}
	return readASCII(data, bitOffset+8, int(length))
}

func readASCII(data []byte, bitOffset, length int) (string, int, bool) {
	if length < 0 {
		return "", bitOffset, false
	}
	out := make([]byte, 0, length)
	offset := bitOffset
	for i := 0; i < length; i++ {
		raw, ok := readBits(data, offset, 8)
		if !ok {
			return "", offset, false
		}
		out = append(out, byte(raw))
		offset += 8
	}
	return strings.TrimRight(string(out), "\x00 "), offset, true
}

func decodeAntennaHeightMeters(payload []byte) (float64, bool) {
	ref, _, _, ok := decodeReferenceStationPosition(1006, payload)
	if !ok || ref == nil || ref.AntennaHeightMeters == nil {
		return 0, false
	}
	return *ref.AntennaHeightMeters, true
}

func readSignedBits(data []byte, bitOffset, bitLength int) (int64, bool) {
	raw, ok := readBits(data, bitOffset, bitLength)
	if !ok || bitLength <= 0 || bitLength >= 64 {
		return 0, false
	}
	signBit := uint64(1) << (bitLength - 1)
	if raw&signBit == 0 {
		return int64(raw), true
	}
	return int64(raw - (uint64(1) << bitLength)), true
}

func readBits(data []byte, bitOffset, bitLength int) (uint64, bool) {
	if bitLength < 0 || bitLength > 64 || bitOffset < 0 {
		return 0, false
	}
	totalBits := len(data) * 8
	if bitOffset+bitLength > totalBits {
		return 0, false
	}
	var out uint64
	for i := 0; i < bitLength; i++ {
		pos := bitOffset + i
		b := data[pos/8]
		shift := 7 - (pos % 8)
		out = (out << 1) | uint64((b>>shift)&0x01)
	}
	return out, true
}

func ecefToGeodetic(ecef *ECEF) *GeoPoint {
	if ecef == nil {
		return nil
	}
	const (
		a = 6378137.0
		f = 1 / 298.257223563
	)
	b := a * (1 - f)
	e2 := f * (2 - f)
	ep2 := (a*a - b*b) / (b * b)

	x, y, z := ecef.X, ecef.Y, ecef.Z
	p := math.Hypot(x, y)
	if p == 0 && z == 0 {
		return nil
	}

	lon := math.Atan2(y, x)
	if p == 0 {
		lat := math.Copysign(math.Pi/2, z)
		return &GeoPoint{
			Latitude:  lat * 180 / math.Pi,
			Longitude: 0,
			Altitude:  math.Abs(z) - b,
		}
	}

	theta := math.Atan2(z*a, p*b)
	sinTheta := math.Sin(theta)
	cosTheta := math.Cos(theta)
	lat := math.Atan2(z+ep2*b*sinTheta*sinTheta*sinTheta, p-e2*a*cosTheta*cosTheta*cosTheta)
	sinLat := math.Sin(lat)
	n := a / math.Sqrt(1-e2*sinLat*sinLat)
	alt := p/math.Cos(lat) - n

	return &GeoPoint{
		Latitude:  lat * 180 / math.Pi,
		Longitude: lon * 180 / math.Pi,
		Altitude:  alt,
	}
}

func cloneECEF(in *ECEF) *ECEF {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneGeoPoint(in *GeoPoint) *GeoPoint {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneReferenceStation(in *ReferenceStation) *ReferenceStation {
	if in == nil {
		return nil
	}
	out := *in
	if in.AntennaHeightMeters != nil {
		value := *in.AntennaHeightMeters
		out.AntennaHeightMeters = &value
	}
	return &out
}
