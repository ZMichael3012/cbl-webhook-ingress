//go:build e2e

// Package e2e contains the webhook-ingress end-to-end scenario: a signed webhook
// travels over HTTP into PostgreSQL (inbox and outbox) and is published to Kafka.
package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/ginmw"
	"github.com/ZMichael3012/cbl-kit/hmac"
	"github.com/ZMichael3012/cbl-kit/outbox"
	"github.com/ZMichael3012/cbl-kit/outbox/kafkapub"
	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
	"github.com/cbl/cbl-webhook-ingress/internal/transport"
	"github.com/cbl/cbl-webhook-ingress/internal/usecase"
)

const (
	e2eSecret = "e2e-secret"
	e2eTopic  = "gateway.webhooks.raw"
)

func setupPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("webhook_e2e"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func setupKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("cbl-webhook-e2e"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	return brokers
}

func TestE2EWebhookToKafka(t *testing.T) {
	db := setupPostgres(t)
	brokers := setupKafka(t)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	admin := kadm.NewClient(adminClient)
	_, err = admin.CreateTopic(context.Background(), 1, 1, nil, e2eTopic)
	require.NoError(t, err)
	adminClient.Close()

	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relay := outbox.NewRelay(db, publisher, zap.NewNop(), outbox.WithInterval(100*time.Millisecond))
	go func() { _ = relay.Run(ctx) }()

	providers := map[string]domain.ProviderConfig{
		"mock": {
			Name:            "mock",
			SignatureHeader: "X-Signature",
			EventIDHeader:   "X-Event-Id",
			Secret:          []byte(e2eSecret),
		},
	}
	handler := usecase.NewHandler(db, "cbl-webhook-ingress", zap.NewNop())
	server := transport.NewServer(providers, handler, zap.NewNop())

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(ginmw.RequestID())
	server.Register(router)

	body := `{"gateway_id":"g-e2e","status":"detected"}`
	now := time.Now().UTC()
	req := httptest.NewRequest(http.MethodPost, "/hooks/mock/ep1", strings.NewReader(body))
	req.Header.Set("X-Event-Id", "vendor-e2e-1")
	req.Header.Set("X-Signature", hmac.BuildHeader(hmac.Signature{
		Timestamp: now,
		Hex:       hmac.Sign([]byte(e2eSecret), now, []byte(body)),
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(e2eTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	deadline := time.Now().Add(45 * time.Second)
	var record *kgo.Record
	for time.Now().Before(deadline) && record == nil {
		fetches := consumer.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) { record = r })
		if record == nil {
			time.Sleep(500 * time.Millisecond)
		}
	}
	require.NotNil(t, record, "message was not delivered to Kafka within 45 seconds")
	assert.Equal(t, "mock:g-e2e", string(record.Key))

	var raw eventsv1.GatewayWebhookRaw
	require.NoError(t, proto.Unmarshal(record.Value, &raw))
	assert.Equal(t, "mock", raw.GetProvider())
	assert.Equal(t, "g-e2e", raw.GetGatewayId())
	assert.True(t, raw.GetSignatureVerified())
	assert.Equal(t, body, string(raw.GetRawBody()))

	eventType := ""
	for _, header := range record.Headers {
		if header.Key == outbox.HeaderEventType {
			eventType = string(header.Value)
		}
	}
	assert.Equal(t, "gateway.webhook.raw", eventType)
}
