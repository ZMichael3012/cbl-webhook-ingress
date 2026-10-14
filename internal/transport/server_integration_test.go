//go:build integration

package transport_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-webhook-ingress/internal/usecase"
)

func setupWebhookDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("waas"),
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

func TestIntegrationWebhookSavedAndDeduplicated(t *testing.T) {
	db := setupWebhookDB(t)
	realHandler := usecase.NewHandler(db, "cbl-webhook-ingress", zap.NewNop())
	router := newTestRouter(realHandler)

	body := `{"gateway_id":"g-1","status":"detected"}`

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(body, "vendor-1", fixedNow))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"duplicate":false`)

	var outboxCount, inboxCount int64
	require.NoError(t, db.Table("outbox").Count(&outboxCount).Error)
	require.NoError(t, db.Table("inbox").Count(&inboxCount).Error)
	require.Equal(t, int64(1), outboxCount)
	require.Equal(t, int64(1), inboxCount)

	var payload []byte
	var topic, key string
	row := db.Raw("SELECT payload, key, topic FROM outbox LIMIT 1").Row()
	require.NoError(t, row.Scan(&payload, &key, &topic))
	assert.Equal(t, "gateway.webhooks.raw", topic)
	assert.Equal(t, "mock:g-1", key)

	var raw eventsv1.GatewayWebhookRaw
	require.NoError(t, proto.Unmarshal(payload, &raw))
	assert.Equal(t, "mock", raw.GetProvider())
	assert.Equal(t, "g-1", raw.GetGatewayId())
	assert.True(t, raw.GetSignatureVerified())
	assert.Equal(t, body, string(raw.GetRawBody()))

	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, signedRequest(body, "vendor-1", fixedNow))
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), `"duplicate":true`)
	require.NoError(t, db.Table("outbox").Count(&outboxCount).Error)
	require.Equal(t, int64(1), outboxCount)

	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, signedRequest(body, "vendor-2", fixedNow))
	require.Equal(t, http.StatusOK, rec3.Code)
	require.NoError(t, db.Table("outbox").Count(&outboxCount).Error)
	require.Equal(t, int64(2), outboxCount)
}

type recordingPublisher struct {
	events []outbox.Envelope
}

// Publish записывает конверты для проверки relay.
func (p *recordingPublisher) Publish(_ context.Context, env outbox.Envelope) error {
	p.events = append(p.events, env)
	return nil
}

func TestIntegrationRelayPublishesWebhook(t *testing.T) {
	db := setupWebhookDB(t)
	realHandler := usecase.NewHandler(db, "cbl-webhook-ingress", zap.NewNop())
	router := newTestRouter(realHandler)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(`{"gateway_id":"g-9"}`, "vendor-9", fixedNow))
	require.Equal(t, http.StatusOK, rec.Code)

	publisher := &recordingPublisher{}
	relay := outbox.NewRelay(db, publisher, zap.NewNop(), outbox.WithBatch(10))
	published, err := relay.PublishBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, published)
	require.Len(t, publisher.events, 1)
	assert.Equal(t, "gateway.webhooks.raw", publisher.events[0].Topic)

	var publishedRows int64
	require.NoError(t, db.Table("outbox").Where("published_at IS NOT NULL").Count(&publishedRows).Error)
	assert.Equal(t, int64(1), publishedRows)
}

func TestIntegrationWebhookPartitionKeyFallback(t *testing.T) {
	db := setupWebhookDB(t)
	realHandler := usecase.NewHandler(db, "cbl-webhook-ingress", zap.NewNop())
	router := newTestRouter(realHandler)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(`{}`, "vendor-10", fixedNow))
	require.Equal(t, http.StatusOK, rec.Code)

	var key string
	require.NoError(t, db.Raw("SELECT key FROM outbox LIMIT 1").Row().Scan(&key))
	assert.Equal(t, "mock:ep1", key)
}
