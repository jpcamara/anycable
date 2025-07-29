//go:build !nopostgres
// +build !nopostgres

package broadcast

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"log/slog"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPostgresURL(t *testing.T) string {
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		t.Skip("POSTGRES_URL not set, skipping postgres subscriber tests")
	}
	return url
}

func createTestSubscriber(t *testing.T, channel string) (*PostgresSubscriber, *testHandler) {
	config := &PostgresConfig{
		URL:     getTestPostgresURL(t),
		Channel: channel,
	}

	m := metrics.NewMetrics(nil, 1, slog.Default())
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	testHandler := &testHandler{
		messages: make([][]byte, 0),
	}

	subscriber, err := NewPostgresSubscriber(testHandler, config, m, log)
	require.NoError(t, err)

	return subscriber, testHandler
}

type testHandler struct {
	mu       sync.RWMutex
	messages [][]byte
}

func (h *testHandler) HandleBroadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, data)
}

func (h *testHandler) HandlePubSub(data []byte) {
	h.HandleBroadcast(data)
}

func (h *testHandler) getMessages() [][]byte {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make([][]byte, len(h.messages))
	copy(result, h.messages)
	return result
}

func (h *testHandler) waitForMessages(count int, timeout time.Duration) [][]byte {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		messages := h.getMessages()
		if len(messages) >= count {
			return messages[:count]
		}
		time.Sleep(10 * time.Millisecond)
	}
	return h.getMessages()
}

func TestNewPostgresSubscriber_WithoutURL(t *testing.T) {
	config := &PostgresConfig{Channel: "test"}
	m := metrics.NewMetrics(nil, 1, slog.Default())
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	_, err := NewPostgresSubscriber(&testHandler{}, config, m, log)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "postgres URL is required")
}

func TestPostgresConfig_Defaults(t *testing.T) {
	config := NewPostgresConfig()
	assert.Equal(t, defaultChannel, config.Channel)
	assert.Empty(t, config.URL)
}

func TestPostgresConfig_ToToml(t *testing.T) {
	config := &PostgresConfig{
		URL:               "postgres://test",
		Channel:           "test_channel",
		LargePayloadTable: "my_large_payloads",
	}
	
	toml := config.ToToml()
	assert.Equal(t, "channel = \"test_channel\"\nlarge_payload_table = \"my_large_payloads\"\n", toml)
}

func TestPostgresSubscriber_IsFanout(t *testing.T) {
	subscriber, _ := createTestSubscriber(t, "test")
	assert.True(t, subscriber.IsFanout())
}

func TestPostgresSubscriber_StartAndShutdown(t *testing.T) {
	subscriber, _ := createTestSubscriber(t, "test_start_shutdown")

	done := make(chan error, 1)
	err := subscriber.Start(done)
	require.NoError(t, err)

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Test graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = subscriber.Shutdown(ctx)
	assert.NoError(t, err)

	// Ensure the done channel receives a message
	select {
	case <-done:
		// Expected
	case <-time.After(1 * time.Second):
		t.Fatal("Expected done channel to receive a message")
	}
}

func TestPostgresSubscriber_ReceiveMessages(t *testing.T) {
	channelName := "test_receive_messages"
	subscriber, testHandler := createTestSubscriber(t, channelName)

	// Start the subscriber
	done := make(chan error, 1)
	err := subscriber.Start(done)
	require.NoError(t, err)

	// Give subscriber time to start and establish connection
	time.Sleep(500 * time.Millisecond)

	// Create a separate connection to send notifications
	pool, err := pgxpool.New(context.Background(), getTestPostgresURL(t))
	require.NoError(t, err)
	defer pool.Close()

	// Send test messages
	testMessages := []string{
		`{"stream":"test_stream_1","data":"hello"}`,
		`{"stream":"test_stream_2","data":"world"}`,
		`{"stream":"test_stream_3","data":"postgres"}`,
	}

	for _, msg := range testMessages {
		_, err = pool.Exec(context.Background(), fmt.Sprintf("NOTIFY %s, '%s'", channelName, msg))
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond) // Small delay between messages
	}

	// Wait for messages to be received
	receivedMessages := testHandler.waitForMessages(len(testMessages), 2*time.Second)
	require.Len(t, receivedMessages, len(testMessages))

	// Verify message contents
	for i, expectedMsg := range testMessages {
		assert.JSONEq(t, expectedMsg, string(receivedMessages[i]))
		
		// Also verify it can be unmarshaled as StreamMessage
		var streamMsg common.StreamMessage
		err := json.Unmarshal(receivedMessages[i], &streamMsg)
		require.NoError(t, err)
		
		var expectedStreamMsg common.StreamMessage
		err = json.Unmarshal([]byte(expectedMsg), &expectedStreamMsg)
		require.NoError(t, err)
		
		assert.Equal(t, expectedStreamMsg.Stream, streamMsg.Stream)
		assert.Equal(t, expectedStreamMsg.Data, streamMsg.Data)
	}

	// Cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = subscriber.Shutdown(ctx)
	assert.NoError(t, err)
}

func TestPostgresSubscriber_MultipleSubscribers(t *testing.T) {
	channelName := "test_multiple_subscribers"
	
	// Create two subscribers on the same channel
	subscriber1, testHandler1 := createTestSubscriber(t, channelName)
	subscriber2, testHandler2 := createTestSubscriber(t, channelName)

	// Start both subscribers
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	
	err := subscriber1.Start(done1)
	require.NoError(t, err)
	
	err = subscriber2.Start(done2)
	require.NoError(t, err)

	// Give subscribers time to start
	time.Sleep(500 * time.Millisecond)

	// Send a message
	pool, err := pgxpool.New(context.Background(), getTestPostgresURL(t))
	require.NoError(t, err)
	defer pool.Close()

	testMessage := `{"stream":"fanout_test","data":"broadcast"}`
	_, err = pool.Exec(context.Background(), fmt.Sprintf("NOTIFY %s, '%s'", channelName, testMessage))
	require.NoError(t, err)

	// Both subscribers should receive the message (fanout behavior)
	messages1 := testHandler1.waitForMessages(1, 2*time.Second)
	messages2 := testHandler2.waitForMessages(1, 2*time.Second)
	
	require.Len(t, messages1, 1)
	require.Len(t, messages2, 1)
	
	assert.JSONEq(t, testMessage, string(messages1[0]))
	assert.JSONEq(t, testMessage, string(messages2[0]))

	// Cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = subscriber1.Shutdown(ctx)
	assert.NoError(t, err)
	
	err = subscriber2.Shutdown(ctx)
	assert.NoError(t, err)
}

func TestPostgresSubscriber_InvalidConnection(t *testing.T) {
	config := &PostgresConfig{
		URL:     "postgres://invalid:invalid@nonexistent:5432/db",
		Channel: "test",
	}

	m := metrics.NewMetrics(nil, 1, slog.Default())
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	testHandler := &testHandler{}

	subscriber, err := NewPostgresSubscriber(testHandler, config, m, log)
	require.NoError(t, err)

	done := make(chan error, 1)
	err = subscriber.Start(done)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to postgres")
}