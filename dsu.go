package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"
	"os"
	"strings"
	"fmt"
	"encoding/hex"
)

// --- START ZMIENNYCH GLOBALNYCH DLA FILTRA ---
var (
	gyroBiasX float64
	gyroBiasY float64
	gyroBiasZ float64

	filtGX float64
	filtGY float64
	filtGZ float64

	initialized bool

	calibSamples int
	calibX       float64
	calibY       float64
	calibZ       float64
)

const (
	calibrationFrames = 300
	alpha             = 0.90
	gyroDeadzone      = 0.004
	spikeLimit        = 8.0
)

func lowPass(prev, current float64) float64 {
	return alpha*prev + (1.0-alpha)*current
}

// --- STRUKTURA PRZETWARZANIA IMU ---
type IMUProcessor struct {
	mu sync.Mutex

	// Kalibracja
	calibrating  bool
	samplesCount int
	biasX, biasY, biasZ float64

	// Freeze detector
	lastRawX, lastRawY, lastRawZ float64
	duplicateCount int
	isFrozen       bool

	// LPF
	lpfX, lpfY, lpfZ float64
}

var processor = &IMUProcessor{calibrating: true}

const (
	eps                   = 0.0005 // Epsilon dla błędu pływającego
	freezeLimit           = 20     // Ilość klatek do uznania zamrożenia (ok. 50-80ms)
	alpha                 = 0.85   // Współczynnik Low Pass Filter
	maxCalibrationSamples = 200    // Pula próbek do wyliczenia biasu
)

func equal(a, b float64) bool {
	return math.Abs(a-b) < eps
}

func (p *IMUProcessor) Process(sample *IMUSample) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 1. Freeze Detector na surowych danych
	if equal(sample.Gyro.X, p.lastRawX) && equal(sample.Gyro.Y, p.lastRawY) && equal(sample.Gyro.Z, p.lastRawZ) {
		p.duplicateCount++
		if p.duplicateCount >= freezeLimit {
			p.isFrozen = true
		}
	} else {
		p.duplicateCount = 0
		p.isFrozen = false
		p.lastRawX = sample.Gyro.X
		p.lastRawY = sample.Gyro.Y
		p.lastRawZ = sample.Gyro.Z
	}

	if p.isFrozen {
		sample.Gyro.X, sample.Gyro.Y, sample.Gyro.Z = 0, 0, 0
		p.lpfX, p.lpfY, p.lpfZ = 0, 0, 0 
		return
	}

	// 2. Zbieranie biasu po uruchomieniu (konsola musi leżeć płasko)
	if p.calibrating {
		p.biasX += sample.Gyro.X
		p.biasY += sample.Gyro.Y
		p.biasZ += sample.Gyro.Z
		p.samplesCount++
		if p.samplesCount >= maxCalibrationSamples {
			p.biasX /= float64(maxCalibrationSamples)
			p.biasY /= float64(maxCalibrationSamples)
			p.biasZ /= float64(maxCalibrationSamples)
			p.calibrating = false
		}
		sample.Gyro.X, sample.Gyro.Y, sample.Gyro.Z = 0, 0, 0
		return
	}

	// 3. Bias Correction
	gx := sample.Gyro.X - p.biasX
	gy := sample.Gyro.Y - p.biasY
	gz := sample.Gyro.Z - p.biasZ

	// 4. Low Pass Filter (wyciszenie szumu wysokiej częstotliwości)
	p.lpfX = alpha*p.lpfX + (1.0-alpha)*gx
	p.lpfY = alpha*p.lpfY + (1.0-alpha)*gy
	p.lpfZ = alpha*p.lpfZ + (1.0-alpha)*gz

	// Zapis do oryginalnej struktury
	sample.Gyro.X = p.lpfX
	sample.Gyro.Y = p.lpfY
	sample.Gyro.Z = p.lpfZ
}
// --- KONIEC STRUKTURY ---

const (
	dsuProtoVersion uint16 = 1001
	// Message types (little endian values)
	dsuMsgVersion   uint32 = 0x00100000
	dsuMsgInfo      uint32 = 0x00100001
	dsuMsgData      uint32 = 0x00100002
	// magic
	dsuMagicServer = "DSUS" // server → client
	dsuMagicClient = "DSUC" // client → server
)

var dsuMAC = [6]byte{0x02, 0x20, 0x6A, 0x7E, 0x51, 0x01}

func init() {
    if s := os.Getenv("DSU_MAC"); s != "" {
        // admite "02:20:6A:7E:51:01" o "02-20-6A-7E-51-01"
        clean := strings.NewReplacer(":", "", "-", "").Replace(s)
        if len(clean) == 12 {
            if b, err := hex.DecodeString(clean); err == nil {
                copy(dsuMAC[:], b[:6])
            }
        }
    }
}

// A single-slot server (slot 0). Enough for our case.
type DSUServer struct {
	mu       sync.Mutex
	serverID uint32
	conn     *net.UDPConn

	// active clients subscribed to slot 0 (key = addr.String())
	subs map[string]*net.UDPAddr

	// packet counter per client
	pkt uint32

	// flag to debug req resp and packet sizes
	debug bool
	lastInfo time.Time
}

func NewDSUServer(bind string) (*DSUServer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	s := &DSUServer{
		serverID: randUint32(),
		conn:     conn,
		subs:     make(map[string]*net.UDPAddr),
		debug:    os.Getenv("DSU_DEBUG") == "1", 
	}
	go s.readLoop()
	return s, nil
}

func dumpPacket(prefix string, b []byte) {
    if !strings.HasPrefix(prefix, "DSU") { fmt.Println(prefix) } // opcional
    if len(b) < 20 { fmt.Printf("%s <len=%d>\n", prefix, len(b)); return }
    magic := string(b[0:4])
    ver := binary.LittleEndian.Uint16(b[4:6])
    ln  := binary.LittleEndian.Uint16(b[6:8])
    crc := binary.LittleEndian.Uint32(b[8:12])
    id  := binary.LittleEndian.Uint32(b[12:16])
    mt  := binary.LittleEndian.Uint32(b[16:20])
    fmt.Printf("%s %s v=%d len=%d crc=0x%08x id=0x%08x msgType=0x%08x total=%d\n",
        prefix, magic, ver, ln, crc, id, mt, len(b))
}

func randUint32() uint32 {
	rand.Seed(time.Now().UnixNano())
	return rand.Uint32()
}

func (s *DSUServer) Close() error {
	return s.conn.Close()
}

func (s *DSUServer) readLoop() {
	buf := make([]byte, 2048)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < 20 {
			continue
		}
		// parse header
		magic := string(buf[0:4])
		if magic != dsuMagicClient {
			continue
		}
		ver := binary.LittleEndian.Uint16(buf[4:6])
		if ver != dsuProtoVersion {
			// ignore unknown versions
			continue
		}
		// fast debug output
		if n >= 24 {
			mt := binary.LittleEndian.Uint32(buf[16:20])
			println("DSU<-", magic, "v", ver, "len", binary.LittleEndian.Uint16(buf[6:8]), "msgType", mt)
		}
		if s.debug {
			dumpPacket("RX", buf[:n])
		}
		// length := binary.LittleEndian.Uint16(buf[6:8]) // not needed here
		// crc := binary.LittleEndian.Uint32(buf[8:12])   // could verify if you want
		// clientID := binary.LittleEndian.Uint32(buf[12:16])
		msgType := binary.LittleEndian.Uint32(buf[16:20])

		switch msgType {
		case dsuMsgVersion:
			s.replyVersion(addr)
		case dsuMsgInfo:
			s.replyVersion(addr)  
			s.replyInfoRequest(buf[:n], addr)
		case dsuMsgData:
			s.replyVersion(addr) 
			s.handleDataSubscribe(buf[:n], addr)
			// spec: no immediate response; start/continue streaming after this
		default:
			// ignore others
		}
	}
}

func (s *DSUServer) replyVersion(addr *net.UDPAddr) {
    payload := make([]byte, 2)
    binary.LittleEndian.PutUint16(payload[0:2], dsuProtoVersion)
    pkt := s.buildPacket(dsuMsgVersion, payload)
	if s.debug { dumpPacket("TX", pkt) }
    s.conn.WriteToUDP(pkt, addr)
}

func (s *DSUServer) replyInfoRequest(req []byte, addr *net.UDPAddr) {
	// Incoming: [0..4) int32 count, [4..] slots (bytes)
	if len(req) < 24 {
		return
	}
	count := int(int32(binary.LittleEndian.Uint32(req[20:24])))
	offset := 24
	for i := 0; i < count; i++ {
		if offset >= len(req) {
			break
		}
		slot := req[offset]
		offset++

		// we only serve slot 0; if they ask a different slot, report "not connected"
		if slot != 0 {
			pkt := s.buildControllerInfo(slot, 0)
			if s.debug { dumpPacket("TX", pkt) }
			s.conn.WriteToUDP(pkt, addr)
			continue
		}
		// connected=2 in shared beginning, but the "Info" response requires an extra trailing 0 byte
		pkt := s.buildControllerInfo(0, 2)
		if s.debug { dumpPacket("TX", pkt) }
		s.conn.WriteToUDP(pkt, addr)
	}
}

func (s *DSUServer) handleDataSubscribe(req []byte, addr *net.UDPAddr) {
	// Incoming: bitmask, slot, mac(6)
	if len(req) < 28 {
		return
	}
	flags := req[20]
	slot := req[21]
	// mac := req[22:28] // not used

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case flags == 0:
		// subscribe to all → add client for our slot 0
		s.subs[addr.String()] = addr

		if s.debug { fmt.Println("info: sending immediate ControllerInfo after subscribe") }
		pkt := s.buildControllerInfo(0, 2)
		if s.debug { dumpPacket("TX", pkt) }
		s.conn.WriteToUDP(pkt, addr)
	case (flags&0x01) != 0 && slot == 0:
		s.subs[addr.String()] = addr

		if s.debug { fmt.Println("info: sending immediate ControllerInfo after subscribe") }
		pkt := s.buildControllerInfo(0, 2)
		if s.debug { dumpPacket("TX", pkt) }
		s.conn.WriteToUDP(pkt, addr)
	default:
		// not our slot → ignore
	}
}

// sanitizeFloat32 returns 0 if the value is NaN or Infinity to prevent crashes in DSU clients.
func sanitizeFloat32(v float32) float32 {
	if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
		return 0
	}
	return v
}

var serverStartTime = time.Now()
var lastBroadcast time.Time

// Zmienne do płynnego filtra EMA dla akcelerometru (Sensor Fusion Fix)
var emaAx, emaAy, emaAz float64
var emaInitialized bool

func (s *DSUServer) Broadcast(sample IMUSample) {
	gx := sample.Gyro.X
	gy := sample.Gyro.Y
	gz := sample.Gyro.Z

	// 1. KALIBRACJA BIASU
	// Po uruchomieniu programu przez pierwsze 300 klatek zbieramy próbki
	// WAŻNE: Konsola musi w tym czasie leżeć płasko i nieruchomo!
	if !initialized {
		calibX += gx
		calibY += gy
		calibZ += gz
		calibSamples++

		if calibSamples >= calibrationFrames {
			gyroBiasX = calibX / float64(calibSamples)
			gyroBiasY = calibY / float64(calibSamples)
			gyroBiasZ = calibZ / float64(calibSamples)
			initialized = true
			fmt.Printf("Kalibracja zakonczona. Bias -> X: %f, Y: %f, Z: %f\n", gyroBiasX, gyroBiasY, gyroBiasZ)
		}
		return // Czekamy na koniec kalibracji, nie wysyłamy śmieci do emulatora
	}

	// 2. ODEJMOWANIE BIASU (usunięcie głównego źródła dryftu)
	gx -= gyroBiasX
	gy -= gyroBiasY
	gz -= gyroBiasZ

	// 3. SPIKE KILLER (odrzucanie pojedynczych błędnych próbek z IIO)
	if math.Abs(gx) > spikeLimit {
		gx = filtGX
	}
	if math.Abs(gy) > spikeLimit {
		gy = filtGY
	}
	if math.Abs(gz) > spikeLimit {
		gz = filtGZ
	}

	// 4. LOW PASS FILTER (wygładzanie szumu)
	filtGX = lowPass(filtGX, gx)
	filtGY = lowPass(filtGY, gy)
	filtGZ = lowPass(filtGZ, gz)

	gx = filtGX
	gy = filtGY
	gz = filtGZ

	// 5. ADAPTACYJNA DEADZONE
	if math.Abs(gx) < gyroDeadzone {
		gx = 0
	}
	if math.Abs(gy) < gyroDeadzone {
		gy = 0
	}
	if math.Abs(gz) < gyroDeadzone {
		gz = 0
	}

	// 6. AKCELEROMETR (Prawdziwe dane, bez fałszowania grawitacji!)
	ax := sanitizeFloat32(float32(sample.Accel.X / 9.80665))
	ay := sanitizeFloat32(float32(sample.Accel.Y / 9.80665))
	az := sanitizeFloat32(float32(sample.Accel.Z / 9.80665))

	// 7. KONWERSJA I WYSYŁKA
	const rad2deg = 180.0 / math.Pi
	gxOut := sanitizeFloat32(float32(gx * rad2deg))
	gyOut := sanitizeFloat32(float32(gy * rad2deg))
	gzOut := sanitizeFloat32(float32(gz * rad2deg))

	// Utrzymujemy oryginalny timestamp (sample.TSus), aby zachować interwały kernela
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.subs {
		s.pkt++
		pkt := s.buildControllerData(0, true, s.pkt, sample.TSus, ax, ay, az, gxOut, gyOut, gzOut)
		if s.debug && (s.pkt%100 == 1) { 
			dumpPacket("TX", pkt) 
		}
		s.conn.WriteToUDP(pkt, a)
	}

	now := time.Now()
	if now.Sub(s.lastInfo) >= 500*time.Millisecond {
		pktInfo := s.buildControllerInfo(0, 2)
		for _, a := range s.subs {
			if s.debug { 
				dumpPacket("TX", pktInfo) 
			}
			s.conn.WriteToUDP(pktInfo, a)
		}
		s.lastInfo = now
	}
}
func (s *DSUServer) buildPacket(msgType uint32, payload []byte) []byte {
    // Citron/Yuzu expects payload_length = sizeof(Type) + sizeof(Data)
    // Type in protocol is u32 (4 bytes).
    const typeSize = 4

	payloadLenForHeader := len(payload) + typeSize
	
    total := 20 + len(payload)
    out := make([]byte, total)
	// Header
    copy(out[0:4], []byte(dsuMagicServer))
    binary.LittleEndian.PutUint16(out[4:6], dsuProtoVersion)
    binary.LittleEndian.PutUint16(out[6:8], uint16(payloadLenForHeader))
    
	// CRC leave zero for now
    binary.LittleEndian.PutUint32(out[12:16], s.serverID)
    binary.LittleEndian.PutUint32(out[16:20], msgType)
    copy(out[20:], payload)

    // compute crc over entire packet with crc field zero
    out[8], out[9], out[10], out[11] = 0, 0, 0, 0
    crc := crc32.ChecksumIEEE(out)
    binary.LittleEndian.PutUint32(out[8:12], crc)
    
	return out
}

// Shared beginning (11 bytes): slot, state, model, connection, MAC(6), battery
func sharedBeginning(slot uint8, state uint8) []byte {
	b := make([]byte, 11)
	b[0] = slot
	b[1] = state         // 0=not connected, 1=reserved?, 2=connected
	b[2] = 2             // device model: full gyro
	b[3] = 1             // connection: 1=USB, 2=BT, 0=NA
	// MAC 6 bytes
	copy(b[4:10], dsuMAC[:])
	b[10] = 0x05         // battery: "Full (or almost)" (cosmético)
	return b
}

// ControllerInfo reply (message type 0x100001)
// Payload: 11 bytes shared beginning + 1 zero byte
func (s *DSUServer) buildControllerInfo(slot uint8, state uint8) []byte {
	p := make([]byte, 12)
	// info bytes
	copy(p[0:11], sharedBeginning(slot, state))
	// byte 11: is_pad_active
    if state == 2 {
        p[11] = 1 // active
    } else {
        p[11] = 0 // inactive
    }
	return s.buildPacket(dsuMsgInfo, p)
}

// ControllerData (message type 0x100002)
// Payload structure length = 80 bytes (header says total packet is 100)
func (s *DSUServer) buildControllerData(slot uint8, connected bool, pktNo uint32, tsUS uint64,
	ax, ay, az, gx, gy, gz float32) []byte {

	p := make([]byte, 80)
	// 0..10 shared beginning
	state := uint8(0)
	if connected {
		state = 2
	}
	copy(p[0:11], sharedBeginning(slot, state))

	// 11: isConnected (2/0)
	if connected { p[11] = 1 } else { p[11] = 0 }

	// 12..15: packet number
	binary.LittleEndian.PutUint32(p[12:16], pktNo)

	// 16: buttons bitmask 1 (we report none)
	// 17: buttons bitmask 2
	// 18: HOME (0/1)
	// 19: TOUCH (0/1)
	// 20..23: sticks 128 neutral
	p[20] = 128
	p[21] = 128
	p[22] = 128
	p[23] = 128
	// 24..35: analog dpad/ABXY/L1R1L2R2 → zeros

	// 36..47: two touches (each 6 bytes) → zeros

	// 48..55: motion timestamp (microseconds). Spec: "update only with accel changes", pero los clientes toleran valores crecientes.
	binary.LittleEndian.PutUint64(p[48:56], tsUS)

	// 56..67: accel X/Y/Z (float32, in g)
	putF32 := func(off int, v float32) { binary.LittleEndian.PutUint32(p[off:off+4], math.Float32bits(v)) }
	putF32(56, ax)
	putF32(60, ay)
	putF32(64, az)

	// 68..79: gyro pitch / yaw / roll (float32, deg/s)
	// Mapeo simple: X→pitch, Y→yaw, Z→roll
	putF32(68, gx) // pitch
	putF32(72, gy) // yaw
	putF32(76, gz) // roll

	return s.buildPacket(dsuMsgData, p)
}

// ----- helpers for tests -----
var _ = errors.Is
