// Package yjs serves y-protocols binary WebSocket clients (y-websocket and
// compatible providers) from an Action Cable channel.
//
// One socket syncs one document. The client's first frame is a y-protocols
// auth message carrying the channel identifier:
//
//	[2][1][varString identifier JSON]
//
// The identifier is the same JSON an Action Cable client subscribes with, for
// example {"channel":"RoomChannel","name":"abc","grant":"..."}. Sending it as
// a frame keeps signed grants out of the URL and access logs.
//
// After the subscription, frames map to the channel's JSON envelope:
//
//	[0 ...]                  -> receive {"update": b64}
//	[100][varUint id][0 ...] -> receive {"update": b64, "id": id}
//	[1 ...]                  -> whisper {"awareness": b64}
//	{"update": b64}          -> raw frame
//	{"awareness": b64}       -> raw frame
//	{"ack": id}              -> [101][varUint id]
//	anything else            -> JSON text frame
//
// Awareness is whispered unless the endpoint is told otherwise, in which case
// it goes to the channel as {"update": b64} and the channel relays it.
//
// A rejected subscription or connection gets a y-protocols permission-denied
// message, [2][0][varString reason], and the socket closes with 4403.
// AnyCable pings become an empty awareness update, [1][1][0], which clients
// ignore but which keeps y-websocket from reconnecting after 30 seconds of
// silence.
package yjs

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/encoders"
	"github.com/anycable/anycable-go/ws"
)

const encoderID = "yjs"

// y-protocols message types, plus the two reliable-delivery frames.
const (
	MessageSync      = 0
	MessageAwareness = 1
	MessageAuth      = 2
	MessageUpdate    = 100
	MessageAck       = 101
)

// Auth submessages. Permission denied is y-protocols' own; the token
// submessage is the client-to-server direction y-protocols leaves undefined.
const (
	AuthPermissionDenied = 0
	AuthToken            = 1
)

// CloseUnauthorized tells a client not to reconnect with the same credentials.
const CloseUnauthorized = 4403

// CloseRetry tells a client to reconnect (the server is restarting or asked
// for it).
const CloseRetry = 1012

var keepalive = []byte{MessageAwareness, 1, 0}

// Encoder is per session: it remembers the identifier from the auth frame.
type Encoder struct {
	// Whisper sends awareness frames straight to the other clients. When
	// false, they go to the channel like document frames, so the server can
	// validate and throttle them.
	Whisper bool

	mu         sync.Mutex
	identifier string
}

var _ encoders.Encoder = (*Encoder)(nil)

func NewEncoder() *Encoder {
	return &Encoder{Whisper: true}
}

func (*Encoder) ID() string {
	return encoderID
}

// Identifier returns the subscription identifier, or "" before the auth frame.
func (e *Encoder) Identifier() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.identifier
}

func (e *Encoder) Decode(raw []byte) (*common.Message, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	identifier := e.Identifier()

	if identifier == "" {
		return e.decodeAuth(raw)
	}

	switch raw[0] {
	case MessageSync:
		return receive(identifier, raw, nil), nil
	case MessageUpdate:
		id, n, err := readVarUint(raw[1:])
		if err != nil || len(raw) <= 1+n || raw[1+n] != MessageSync {
			return nil, nil
		}
		return receive(identifier, raw[1+n:], &id), nil
	case MessageAwareness:
		if !e.Whisper {
			return receive(identifier, raw, nil), nil
		}
		return &common.Message{
			Command:    "whisper",
			Identifier: identifier,
			Data:       map[string]string{"awareness": base64.StdEncoding.EncodeToString(raw)},
		}, nil
	}

	// Everything else (a second auth frame, query-awareness, unknown types)
	// is dropped, as y-websocket drops what it doesn't know.
	return nil, nil
}

func (e *Encoder) decodeAuth(raw []byte) (*common.Message, error) {
	if len(raw) < 2 || raw[0] != MessageAuth || raw[1] != AuthToken {
		// Frames sent before the auth frame are dropped. The server's
		// SyncStep1 after subscribing starts the handshake again.
		return nil, nil
	}

	identifier, err := readVarString(raw[2:])
	if err != nil {
		return nil, err
	}

	var params map[string]interface{}
	if err := json.Unmarshal([]byte(identifier), &params); err != nil {
		return nil, fmt.Errorf("auth identifier is not a JSON object: %w", err)
	}
	if _, ok := params["channel"].(string); !ok {
		return nil, errors.New("auth identifier has no channel")
	}

	e.mu.Lock()
	e.identifier = identifier
	e.mu.Unlock()

	return &common.Message{Command: "subscribe", Identifier: identifier}, nil
}

func receive(identifier string, frame []byte, id *uint64) *common.Message {
	data := map[string]interface{}{"update": base64.StdEncoding.EncodeToString(frame)}
	if id != nil {
		data["id"] = *id
	}
	payload, _ := json.Marshal(data)

	return &common.Message{Command: "message", Identifier: identifier, Data: string(payload)}
}

func (e *Encoder) Encode(msg encoders.EncodedMessage) (*ws.SentFrame, error) {
	switch m := msg.(type) {
	case *common.PingMessage:
		return binary(keepalive), nil
	case *common.DisconnectMessage:
		return closeFrame(m.Reconnect, m.Reason), nil
	}

	r, ok := msg.(*common.Reply)
	if !ok {
		return nil, nil
	}

	switch r.Type {
	case common.RejectedType:
		return closeFrame(false, "unauthorized"), nil
	case common.DisconnectType:
		return closeFrame(r.Reconnect, r.Reason), nil
	case "":
		return encodeMessage(r.Message)
	}

	// Welcome, confirmation, and presence replies have no y-protocols
	// counterpart.
	return nil, nil
}

func (e *Encoder) EncodeTransmission(raw string) (*ws.SentFrame, error) {
	reply := common.Reply{}

	if err := json.Unmarshal([]byte(raw), &reply); err != nil {
		return nil, err
	}

	return e.Encode(&reply)
}

func encodeMessage(message interface{}) (*ws.SentFrame, error) {
	m, ok := message.(map[string]interface{})
	if !ok {
		return text(message)
	}

	if id, ok := m["ack"].(float64); ok {
		return binary(appendVarUint([]byte{MessageAck}, uint64(id))), nil
	}

	for _, key := range []string{"update", "awareness"} {
		if encoded, ok := m[key].(string); ok {
			frame, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return nil, err
			}
			return binary(frame), nil
		}
	}

	return text(message)
}

// text sends an application message that has no y-protocols form, such as a
// channel's own notice, as a JSON text frame. y-websocket expects only binary
// frames, so only clients that read text frames should rely on these.
func text(message interface{}) (*ws.SentFrame, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	return &ws.SentFrame{FrameType: ws.TextFrame, Payload: payload}, nil
}

func binary(payload []byte) *ws.SentFrame {
	return &ws.SentFrame{FrameType: ws.BinaryFrame, Payload: payload}
}

// closeFrame closes the socket. A permission-denied auth message would be the
// y-protocols way to say why, but the session closes right after this frame,
// so the close code and reason carry it instead.
func closeFrame(reconnect bool, reason string) *ws.SentFrame {
	code := CloseUnauthorized
	if reconnect {
		code = CloseRetry
	}
	return &ws.SentFrame{FrameType: ws.CloseFrame, CloseCode: code, CloseReason: reason}
}

// lib0 encoding: unsigned LEB128 integers, strings prefixed with their length.

func appendVarUint(buf []byte, n uint64) []byte {
	for n >= 0x80 {
		buf = append(buf, byte(n)|0x80)
		n >>= 7
	}
	return append(buf, byte(n))
}

func readVarUint(buf []byte) (uint64, int, error) {
	var n uint64
	for i, b := range buf {
		if i == 9 {
			break
		}
		n |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return n, i + 1, nil
		}
	}
	return 0, 0, errors.New("malformed varUint")
}

func readVarString(buf []byte) (string, error) {
	length, n, err := readVarUint(buf)
	if err != nil {
		return "", err
	}
	if uint64(len(buf)-n) < length {
		return "", errors.New("truncated varString")
	}
	return string(buf[n : n+int(length)]), nil
}
