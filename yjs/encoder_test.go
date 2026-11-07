package yjs

import (
	"encoding/base64"
	"testing"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const identifier = `{"channel":"RoomChannel","name":"abc","grant":"signed--grant"}`

func authFrame(id string) []byte {
	return append(appendVarUint([]byte{MessageAuth, AuthToken}, uint64(len(id))), id...)
}

func authed(t *testing.T) *Encoder {
	t.Helper()
	enc := NewEncoder()
	msg, err := enc.Decode(authFrame(identifier))
	require.NoError(t, err)
	require.Equal(t, "subscribe", msg.Command)
	return enc
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func TestAuthFrameSubscribes(t *testing.T) {
	enc := NewEncoder()

	msg, err := enc.Decode(authFrame(identifier))

	require.NoError(t, err)
	assert.Equal(t, &common.Message{Command: "subscribe", Identifier: identifier}, msg)
	assert.Equal(t, identifier, enc.Identifier())
}

func TestFramesBeforeAuthAreDropped(t *testing.T) {
	enc := NewEncoder()

	msg, err := enc.Decode([]byte{MessageSync, 0, 0})

	require.NoError(t, err)
	assert.Nil(t, msg)
	assert.Equal(t, "", enc.Identifier())
}

func TestAuthFrameNeedsAChannel(t *testing.T) {
	_, err := NewEncoder().Decode(authFrame(`{"name":"abc"}`))
	assert.Error(t, err)

	_, err = NewEncoder().Decode(authFrame(`not json`))
	assert.Error(t, err)

	_, err = NewEncoder().Decode([]byte{MessageAuth, AuthToken, 50, '{'})
	assert.Error(t, err)
}

func TestASecondAuthFrameIsDropped(t *testing.T) {
	enc := authed(t)

	msg, err := enc.Decode(authFrame(`{"channel":"Other"}`))

	require.NoError(t, err)
	assert.Nil(t, msg)
	assert.Equal(t, identifier, enc.Identifier())
}

func TestSyncFrameIsReceived(t *testing.T) {
	frame := []byte{MessageSync, 2, 3, 9, 9, 9}

	msg, err := authed(t).Decode(frame)

	require.NoError(t, err)
	assert.Equal(t, "message", msg.Command)
	assert.Equal(t, identifier, msg.Identifier)
	assert.JSONEq(t, `{"update":"`+b64(frame)+`"}`, msg.Data.(string))
}

func TestUpdateFrameCarriesItsID(t *testing.T) {
	frame := []byte{MessageSync, 2, 3, 9, 9, 9}
	wire := append(appendVarUint([]byte{MessageUpdate}, 300), frame...)

	msg, err := authed(t).Decode(wire)

	require.NoError(t, err)
	assert.JSONEq(t, `{"update":"`+b64(frame)+`","id":300}`, msg.Data.(string))
}

func TestMalformedUpdateFrameIsDropped(t *testing.T) {
	enc := authed(t)

	for _, wire := range [][]byte{{MessageUpdate}, {MessageUpdate, 5}, {MessageUpdate, 5, MessageAwareness, 0}, {MessageUpdate, 0xff}} {
		msg, err := enc.Decode(wire)
		require.NoError(t, err)
		assert.Nil(t, msg, "%v", wire)
	}
}

func TestAwarenessFrameIsWhispered(t *testing.T) {
	frame := []byte{MessageAwareness, 4, 1, 2, 3, 4}

	msg, err := authed(t).Decode(frame)

	require.NoError(t, err)
	assert.Equal(t, &common.Message{
		Command:    "whisper",
		Identifier: identifier,
		Data:       map[string]string{"awareness": b64(frame)},
	}, msg)
}

func TestOtherFramesAreDropped(t *testing.T) {
	enc := authed(t)

	for _, wire := range [][]byte{{3}, {MessageAuth, AuthPermissionDenied, 0}, {42, 1}, {}} {
		msg, err := enc.Decode(wire)
		require.NoError(t, err)
		assert.Nil(t, msg, "%v", wire)
	}
}

func TestBroadcastsBecomeRawFrames(t *testing.T) {
	update := []byte{MessageSync, 2, 1, 2, 3}
	awareness := []byte{MessageAwareness, 1, 0}
	enc := NewEncoder()

	frame, err := enc.Encode(&common.Reply{Identifier: identifier, Message: map[string]interface{}{"update": b64(update)}})
	require.NoError(t, err)
	assert.Equal(t, &ws.SentFrame{FrameType: ws.BinaryFrame, Payload: update}, frame)

	frame, err = enc.Encode(&common.Reply{Identifier: identifier, Message: map[string]interface{}{"awareness": b64(awareness)}})
	require.NoError(t, err)
	assert.Equal(t, awareness, frame.Payload)
}

func TestTransmissionsAreEncoded(t *testing.T) {
	enc := NewEncoder()

	frame, err := enc.EncodeTransmission(`{"identifier":"x","message":{"ack":300}}`)
	require.NoError(t, err)
	assert.Equal(t, &ws.SentFrame{FrameType: ws.BinaryFrame, Payload: []byte{MessageAck, 0xac, 0x02}}, frame)

	frame, err = enc.EncodeTransmission(`{"type":"welcome","sid":"s1"}`)
	require.NoError(t, err)
	assert.Nil(t, frame)

	frame, err = enc.EncodeTransmission(`{"type":"disconnect","reason":"unauthorized","reconnect":false}`)
	require.NoError(t, err)
	assert.Equal(t, &ws.SentFrame{FrameType: ws.CloseFrame, CloseCode: CloseUnauthorized, CloseReason: "unauthorized"}, frame)
}

func TestRejectionClosesWithUnauthorized(t *testing.T) {
	frame, err := NewEncoder().Encode(&common.Reply{Type: common.RejectedType, Identifier: identifier})

	require.NoError(t, err)
	assert.Equal(t, CloseUnauthorized, frame.CloseCode)
}

func TestRestartDisconnectAsksForAReconnect(t *testing.T) {
	frame, err := NewEncoder().Encode(common.NewDisconnectMessage("server_restart", true))

	require.NoError(t, err)
	assert.Equal(t, ws.CloseFrame, frame.FrameType)
	assert.Equal(t, CloseRetry, frame.CloseCode)
}

func TestPingIsAnEmptyAwarenessUpdate(t *testing.T) {
	frame, err := NewEncoder().Encode(&common.PingMessage{Type: common.PingType})

	require.NoError(t, err)
	assert.Equal(t, []byte{MessageAwareness, 1, 0}, frame.Payload)
}

func TestConfirmationsHaveNoFrame(t *testing.T) {
	frame, err := NewEncoder().Encode(&common.Reply{Type: common.ConfirmedType, Identifier: identifier})

	require.NoError(t, err)
	assert.Nil(t, frame)
}

func TestVarUintRoundTrip(t *testing.T) {
	for _, n := range []uint64{0, 1, 127, 128, 300, 1 << 32, 1<<63 - 1} {
		got, size, err := readVarUint(appendVarUint(nil, n))
		require.NoError(t, err)
		assert.Equal(t, n, got)
		assert.Equal(t, len(appendVarUint(nil, n)), size)
	}
}

func TestAwarenessGoesToTheChannelWithoutWhisper(t *testing.T) {
	enc := authed(t)
	enc.Whisper = false
	frame := []byte{MessageAwareness, 4, 1, 2, 3, 4}

	msg, err := enc.Decode(frame)

	require.NoError(t, err)
	assert.Equal(t, "message", msg.Command)
	assert.JSONEq(t, `{"update":"`+b64(frame)+`"}`, msg.Data.(string))
}

func TestOtherMessagesBecomeTextFrames(t *testing.T) {
	frame, err := NewEncoder().Encode(&common.Reply{Identifier: identifier, Message: map[string]interface{}{"notice": "full"}})

	require.NoError(t, err)
	assert.Equal(t, ws.TextFrame, frame.FrameType)
	assert.JSONEq(t, `{"notice":"full"}`, string(frame.Payload))
}
