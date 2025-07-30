//go:build !nopostgres
// +build !nopostgres

package broadcast

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/anycable/anycable-go/metrics"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	postgresMetricPrefix     = "postgres_subscriber"
	defaultChannel           = "__anycable__"
	maxReconnectDelay        = 30 * time.Second
	initialReconnectDelay    = 1 * time.Second
	largePayloadMarker       = "__large_payload:"
	defaultLargePayloadTable = "anycable_large_payloads"
)

// PostgresConfig contains Postgres Pub/Sub configuration.
// Postgres Pub/Sub is a fanout broadcaster, so it's not compatible with broker.
type PostgresConfig struct {
	URL                   string `mapstructure:"url" toml:"url"`
	Channel               string `mapstructure:"channel" toml:"channel"`
	LargePayloadTable     string `mapstructure:"large_payload_table" toml:"large_payload_table"`
}

// NewPostgresConfig builds a new config for Postgres Pub/Sub
func NewPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Channel:           defaultChannel,
		LargePayloadTable: defaultLargePayloadTable,
	}
}

// ToToml returns a TOML representation of the config
func (c *PostgresConfig) ToToml() string {
	return fmt.Sprintf("channel = \"%s\"\nlarge_payload_table = \"%s\"\n", c.Channel, c.LargePayloadTable)
}

// PostgresSubscriber implements Postgres LISTEN/NOTIFY based broadcasting
type PostgresSubscriber struct {
	handler Handler
	config  *PostgresConfig
	log     *slog.Logger
	metrics *metrics.Metrics

	pool   *pgxpool.Pool
	cancel context.CancelFunc
	done   chan error
	
	// Connection management following River's pattern
	mu   sync.Mutex
	conn *pgx.Conn
}

var _ Broadcaster = (*PostgresSubscriber)(nil)

// NewPostgresSubscriber creates a new PostgresSubscriber
func NewPostgresSubscriber(h Handler, config *PostgresConfig, m *metrics.Metrics, l *slog.Logger) (*PostgresSubscriber, error) {
	if config.URL == "" {
		return nil, errors.New("postgres URL is required")
	}

	// Register metrics
	if m != nil {
		m.RegisterCounter(postgresMetricPrefix+"_messages_total", "The total number of messages received from postgres")
		m.RegisterCounter(postgresMetricPrefix+"_connections_total", "The total number of postgres connections established")
		m.RegisterCounter(postgresMetricPrefix+"_reconnects_total", "The total number of postgres reconnection attempts")
	}

	return &PostgresSubscriber{
		handler: h,
		config:  config,
		log:     l,
		metrics: m,
		done:    make(chan error, 1),
	}, nil
}

// Start listening for new messages
func (s *PostgresSubscriber) Start(done chan error) error {
	if err := s.connect(); err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.log.Info("Starting postgres subscriber", "channel", s.config.Channel)

	go s.listen(ctx, done)

	return nil
}

// Shutdown closes connection and stops listening
func (s *PostgresSubscriber) Shutdown(ctx context.Context) error {
	s.log.Info("Shutting down postgres subscriber")

	if s.cancel != nil {
		s.cancel()
	}

	// Clean up hijacked connection following River's pattern
	s.mu.Lock()
	if s.conn != nil {
		// Connection will be cleaned up by the defer in listenOnce
		// We just nil it here to prevent double cleanup
		s.conn = nil
	}
	s.mu.Unlock()

	if s.pool != nil {
		s.pool.Close()
	}

	// Wait for graceful shutdown or timeout
	select {
	case <-s.done:
		// Listener has cleanly shut down
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// IsFanout returns true as postgres broadcaster is fanout
func (s *PostgresSubscriber) IsFanout() bool {
	return true
}

// connect establishes connection pool to postgres
func (s *PostgresSubscriber) connect() error {
	poolConfig, err := pgxpool.ParseConfig(s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to parse postgres URL: %w", err)
	}

	// Configure pool for listening connections
	poolConfig.MaxConns = 2
	poolConfig.MinConns = 1

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create postgres pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	s.pool = pool
	return nil
}

// listen runs the main listening loop with reconnection logic
func (s *PostgresSubscriber) listen(ctx context.Context, done chan error) {
	defer func() {
		done <- nil
		s.done <- nil
	}()

	retryDelay := initialReconnectDelay

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := s.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}

			s.log.Error("postgres listen error, retrying", "error", err, "retry_delay", retryDelay)
			if s.metrics != nil {
				s.metrics.CounterIncrement(postgresMetricPrefix + "_reconnects_total")
			}

			// Exponential backoff with jitter
			select {
			case <-time.After(retryDelay):
				retryDelay = time.Duration(float64(retryDelay) * 1.5)
				if retryDelay > maxReconnectDelay {
					retryDelay = maxReconnectDelay
				}
			case <-ctx.Done():
				return
			}
		} else {
			// Reset retry delay on successful connection
			retryDelay = initialReconnectDelay
		}
	}
}

// listenOnce performs a single listen cycle following River's connection pattern
func (s *PostgresSubscriber) listenOnce(ctx context.Context) error {
	// Acquire connection from pool and take exclusive ownership
	poolConn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Assume full ownership of the conn so that it doesn't get released back to
	// the pool or auto-closed by the pool (following River's pattern)
	conn := poolConn.Hijack()

	// Store connection for shutdown access
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	// Ensure connection is cleaned up
	defer func() {
		s.mu.Lock()
		if s.conn == conn {
			// Only close if this is still the active connection
			conn.Close(context.Background())
			s.conn = nil
		}
		s.mu.Unlock()
	}()

	// Start listening on the channel
	if _, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s", pgx.Identifier{s.config.Channel}.Sanitize())); err != nil {
		return fmt.Errorf("failed to listen on channel %s: %w", s.config.Channel, err)
	}

	s.log.Info("subscribed to postgres channel", "channel", s.config.Channel)
	if s.metrics != nil {
		s.metrics.CounterIncrement(postgresMetricPrefix + "_connections_total")
	}

	// Listen for notifications
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("error waiting for notification: %w", err)
		}

		s.handleNotification(ctx, notification)
	}
}

// handleNotification processes a received postgres notification
func (s *PostgresSubscriber) handleNotification(ctx context.Context, notification *pgconn.Notification) {
	s.log.Debug("received postgres notification", "channel", notification.Channel, "payload_size", len(notification.Payload))
	if s.metrics != nil {
		s.metrics.CounterIncrement(postgresMetricPrefix + "_messages_total")
	}

	payload := notification.Payload

	// Check if this is a large payload marker
	if strings.HasPrefix(payload, largePayloadMarker) {
		resolvedPayload, err := s.resolveLargePayload(ctx, payload)
		if err != nil {
			s.log.Error("failed to resolve large payload", "error", err, "marker", payload)
			if s.metrics != nil {
				s.metrics.CounterIncrement(postgresMetricPrefix + "_large_payload_errors_total")
			}
			return
		}
		payload = resolvedPayload
		s.log.Debug("resolved large payload", "original_size", len(notification.Payload), "resolved_size", len(payload))
		if s.metrics != nil {
			s.metrics.CounterIncrement(postgresMetricPrefix + "_large_payloads_resolved_total")
		}
	}

	// Forward to handler for processing
	s.handler.HandleBroadcast([]byte(payload))
}

// resolveLargePayload fetches the actual payload from the unlogged table
func (s *PostgresSubscriber) resolveLargePayload(ctx context.Context, marker string) (string, error) {
	// Extract encrypted ID from marker
	encryptedID := strings.TrimPrefix(marker, largePayloadMarker)
	
	// Decode the payload ID (simple base64 decoding to match Ruby implementation)
	payloadID, err := base64.StdEncoding.DecodeString(encryptedID)
	if err != nil {
		return "", fmt.Errorf("failed to decode payload ID: %w", err)
	}

	// Get connection from pool
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query the payload from the unlogged table
	var payload string
	query := fmt.Sprintf("SELECT payload FROM %s WHERE id = $1", 
		pgx.Identifier{s.config.LargePayloadTable}.Sanitize())
	
	err = conn.QueryRow(ctx, query, string(payloadID)).Scan(&payload)
	if err != nil {
		return "", fmt.Errorf("failed to fetch large payload: %w", err)
	}

	return payload, nil
}