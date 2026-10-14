//go:build integration

package transport_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
	"github.com/cbl/cbl-webhook-ingress/internal/transport"
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

	var headerJSON []byte
	require.NoError(t, db.Raw("SELECT headers FROM outbox LIMIT 1").Row().Scan(&headerJSON))
	var headers map[string]string
	require.NoError(t, json.Unmarshal(headerJSON, &headers))
	assert.Equal(t, "gateway.webhook.raw", headers[outbox.HeaderEventType])
	assert.Equal(t, "v1", headers[outbox.HeaderSchemaVersion])
	assert.Equal(t, "cbl-webhook-ingress", headers[outbox.HeaderProducer])
	assert.Equal(t, "mock", headers["provider"])
	assert.Equal(t, "vendor-1", headers["vendor_event_id"])
	assert.Equal(t, "ep1", headers["endpoint_id"])
	expectedEventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("cbl-webhook:mock:vendor-1")).String()
	assert.Equal(t, expectedEventID, headers[outbox.HeaderEventID])

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

func TestIntegrationWriteTimeoutRollsBackAndRetries(t *testing.T) {
	db := setupWebhookDB(t)
	realHandler := usecase.NewHandler(db, "cbl-webhook-ingress", zap.NewNop())
	router := newTestRouter(realHandler, transport.WithWriteTimeout(150*time.Millisecond))

	// Блокировка таблицы inbox воспроизводит медленную запись: транзакция
	// обработчика зависает на вставке и прерывается бюджетом.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	conn, err := sqlDB.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	lockTx, err := conn.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = lockTx.Rollback() }()
	_, err = lockTx.ExecContext(context.Background(), "LOCK TABLE inbox IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	body := `{"gateway_id":"g-timeout"}`
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(body, "vendor-timeout", fixedNow))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), "write_timeout")

	require.NoError(t, lockTx.Rollback())

	var count int64
	require.NoError(t, db.Table("outbox").Count(&count).Error)
	assert.Equal(t, int64(0), count, "прерванная транзакция не оставляет outbox-строк")
	require.NoError(t, db.Table("inbox").Count(&count).Error)
	assert.Equal(t, int64(0), count, "прерванная транзакция не оставляет inbox-строк")

	retry := httptest.NewRecorder()
	router.ServeHTTP(retry, signedRequest(body, "vendor-timeout", fixedNow))
	require.Equal(t, http.StatusOK, retry.Code, "повтор после снятия блокировки")
	assert.Contains(t, retry.Body.String(), `"duplicate":false`)
	require.NoError(t, db.Table("outbox").Count(&count).Error)
	assert.Equal(t, int64(1), count)
	require.NoError(t, db.Table("inbox").Count(&count).Error)
	assert.Equal(t, int64(1), count)
}

func TestIntegrationAllowlistActive(t *testing.T) {
	db := setupWebhookDB(t)
	realHandler := usecase.NewHandler(db, "cbl-webhook-ingress", zap.NewNop())

	providers, err := domain.ParseProviders(
		`{"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id","secret_env":"MOCK_SECRET","ip_allowlist":["192.0.2.0/24"]}}`,
		func(key string) string {
			if key == "MOCK_SECRET" {
				return testSecret
			}
			return ""
		},
	)
	require.NoError(t, err)
	router := newTestRouterWithProviders(providers, realHandler)

	// Адрес по умолчанию httptest (192.0.2.1) входит в allowlist.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(`{"gateway_id":"g-al"}`, "vendor-al", fixedNow))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"duplicate":false`)

	var outboxCount int64
	require.NoError(t, db.Table("outbox").Count(&outboxCount).Error)
	assert.Equal(t, int64(1), outboxCount)
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

func TestIntegrationConcurrentDuplicates(t *testing.T) {
	db := setupWebhookDB(t)
	realHandler := usecase.NewHandler(db, "cbl-webhook-ingress", zap.NewNop())
	router := newTestRouter(realHandler)

	const workers = 8
	body := `{"gateway_id":"g-conc"}`
	start := make(chan struct{})
	codes := make([]int, workers)
	accepted := make([]bool, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			<-start
			router.ServeHTTP(rec, signedRequest(body, "vendor-conc", fixedNow))
			codes[index] = rec.Code
			accepted[index] = strings.Contains(rec.Body.String(), `"duplicate":false`)
		}(i)
	}
	close(start)
	wg.Wait()

	fresh := 0
	for i := 0; i < workers; i++ {
		require.Equal(t, http.StatusOK, codes[i], "worker %d", i)
		if accepted[i] {
			fresh++
		}
	}
	assert.Equal(t, 1, fresh, "ровно один конкурентный запрос создает событие")

	var outboxCount, inboxCount int64
	require.NoError(t, db.Table("outbox").Count(&outboxCount).Error)
	require.NoError(t, db.Table("inbox").Count(&inboxCount).Error)
	assert.Equal(t, int64(1), outboxCount)
	assert.Equal(t, int64(1), inboxCount)
}
