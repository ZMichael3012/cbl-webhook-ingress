package transport_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-kit/ginmw"
	"github.com/cbl/cbl-kit/hmac"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
	"github.com/cbl/cbl-webhook-ingress/internal/transport"
	"github.com/cbl/cbl-webhook-ingress/internal/usecase"
)

const testSecret = "test-secret"

var fixedNow = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

type fakeHandler struct {
	calls     int
	lastInput usecase.WebhookInput
	processed bool
	err       error
}

// Handle записывает вызовы для проверок транспорта.
func (f *fakeHandler) Handle(_ context.Context, in usecase.WebhookInput) (bool, error) {
	f.calls++
	f.lastInput = in
	return f.processed, f.err
}

func newTestRouter(handler transport.Handler, opts ...transport.Option) *gin.Engine {
	gin.SetMode(gin.TestMode)
	providers := map[string]domain.ProviderConfig{
		"mock": {
			Name:            "mock",
			SignatureHeader: "X-Signature",
			EventIDHeader:   "X-Event-Id",
			SecretEnv:       "MOCK_SECRET",
			Secret:          []byte(testSecret),
		},
	}
	options := append([]transport.Option{transport.WithClock(func() time.Time { return fixedNow })}, opts...)
	server := transport.NewServer(providers, handler, zap.NewNop(), options...)
	router := gin.New()
	router.Use(ginmw.RequestID())
	server.Register(router)
	return router
}

func signedRequest(body, eventID string, timestamp time.Time) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/hooks/mock/ep1", strings.NewReader(body))
	req.Header.Set("X-Event-Id", eventID)
	req.Header.Set("X-Signature", hmac.BuildHeader(hmac.Signature{
		Timestamp: timestamp,
		Hex:       hmac.Sign([]byte(testSecret), timestamp, []byte(body)),
	}))
	return req
}

func TestHandleWebhookSuccess(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{processed: true}
	router := newTestRouter(handler)

	body := `{"gateway_id":"g-1","status":"detected"}`
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(body, "vendor-1", fixedNow))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"duplicate":false`)
	require.Equal(t, 1, handler.calls)
	assert.Equal(t, "mock", handler.lastInput.Provider)
	assert.Equal(t, "ep1", handler.lastInput.EndpointID)
	assert.Equal(t, "vendor-1", handler.lastInput.VendorEventID)
	assert.Equal(t, "g-1", handler.lastInput.GatewayID)
	assert.Equal(t, body, string(handler.lastInput.RawBody))
}

func TestHandleWebhookDuplicate(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{processed: false}
	router := newTestRouter(handler)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(`{"id":"x"}`, "vendor-1", fixedNow))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"duplicate":true`)
}

func TestHandleWebhookUnknownProvider(t *testing.T) {
	t.Parallel()

	router := newTestRouter(&fakeHandler{})
	req := httptest.NewRequest(http.MethodPost, "/hooks/unknown/ep1", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "provider_not_found")
}

func TestHandleWebhookMissingEventID(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	router := newTestRouter(handler)

	body := "{}"
	req := httptest.NewRequest(http.MethodPost, "/hooks/mock/ep1", strings.NewReader(body))
	req.Header.Set("X-Signature", hmac.BuildHeader(hmac.Signature{
		Timestamp: fixedNow,
		Hex:       hmac.Sign([]byte(testSecret), fixedNow, []byte(body)),
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "missing_event_id")
	assert.Equal(t, 0, handler.calls)
}

func TestHandleWebhookFiltersSensitiveHeaders(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{processed: true}
	router := newTestRouter(handler)

	req := signedRequest("{}", "vendor-1", fixedNow)
	req.Header.Set("Authorization", "Bearer super-secret")
	req.Header.Set("Cookie", "session=1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	for key := range handler.lastInput.Headers {
		assert.Falsef(t, strings.EqualFold(key, "authorization"), "заголовок %s не должен сохраняться", key)
		assert.Falsef(t, strings.EqualFold(key, "cookie"), "заголовок %s не должен сохраняться", key)
	}
}

func TestHandleWebhookUnknownEndpoint(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	providers := map[string]domain.ProviderConfig{
		"mock": {
			Name:            "mock",
			SignatureHeader: "X-Signature",
			EventIDHeader:   "X-Event-Id",
			Endpoints:       []string{"ep1"},
			Secret:          []byte(testSecret),
		},
	}
	server := transport.NewServer(providers, &fakeHandler{}, zap.NewNop(),
		transport.WithClock(func() time.Time { return fixedNow }))
	router := gin.New()
	router.Use(ginmw.RequestID())
	server.Register(router)

	body := "{}"
	req := httptest.NewRequest(http.MethodPost, "/hooks/mock/other", strings.NewReader(body))
	req.Header.Set("X-Event-Id", "vendor-1")
	req.Header.Set("X-Signature", hmac.BuildHeader(hmac.Signature{
		Timestamp: fixedNow,
		Hex:       hmac.Sign([]byte(testSecret), fixedNow, []byte(body)),
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "endpoint_not_found")
}

func TestHandleWebhookBadSignature(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	router := newTestRouter(handler)

	body := "{}"
	req := httptest.NewRequest(http.MethodPost, "/hooks/mock/ep1", strings.NewReader(body))
	req.Header.Set("X-Event-Id", "vendor-1")
	req.Header.Set("X-Signature", hmac.BuildHeader(hmac.Signature{
		Timestamp: fixedNow,
		Hex:       hmac.Sign([]byte("wrong-secret"), fixedNow, []byte(body)),
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid_signature")
	assert.Equal(t, 0, handler.calls)
}

func TestHandleWebhookExpiredSignature(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	router := newTestRouter(handler)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest("{}", "vendor-1", fixedNow.Add(-10*time.Minute)))

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, 0, handler.calls)
}

func TestHandleWebhookBodyTooLarge(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	router := newTestRouter(handler, transport.WithMaxBody(16))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest(strings.Repeat("a", 100), "vendor-1", fixedNow))

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.Equal(t, 0, handler.calls)
}

func TestHandleWebhookHandlerError(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{err: errors.New("db down")}
	router := newTestRouter(handler)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest("{}", "vendor-1", fixedNow))

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "internal_error")
}

func TestParseProviders(t *testing.T) {
	t.Parallel()

	raw := `{"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id","secret_env":"MOCK_SECRET"}}`
	providers, err := domain.ParseProviders(raw, func(key string) string {
		if key == "MOCK_SECRET" {
			return "s3cret"
		}
		return ""
	})
	require.NoError(t, err)
	require.Contains(t, providers, "mock")
	assert.Equal(t, "X-Signature", providers["mock"].SignatureHeader)
	assert.Equal(t, []byte("s3cret"), providers["mock"].Secret)

	_, err = domain.ParseProviders("", func(string) string { return "x" })
	require.ErrorIs(t, err, domain.ErrInvalidConfig)

	_, err = domain.ParseProviders(`{"mock":{}}`, func(string) string { return "x" })
	require.ErrorIs(t, err, domain.ErrInvalidConfig)

	_, err = domain.ParseProviders(`{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"EMPTY"}}`,
		func(string) string { return "" })
	require.ErrorIs(t, err, domain.ErrInvalidConfig)
}
