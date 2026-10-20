package transport_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ZMichael3012/cbl-kit/ginmw"
	"github.com/ZMichael3012/cbl-kit/hmac"

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

// Handle records calls for transport checks.
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
	return newTestRouterWithProviders(providers, handler, opts...)
}

func newTestRouterWithProviders(providers map[string]domain.ProviderConfig, handler transport.Handler, opts ...transport.Option) *gin.Engine {
	gin.SetMode(gin.TestMode)
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
	for key := range handler.lastInput.Headers {
		assert.Falsef(t, strings.EqualFold(key, "X-Signature"), "signature header %s must not be stored", key)
	}
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
		assert.Falsef(t, strings.EqualFold(key, "authorization"), "header %s must not be stored", key)
		assert.Falsef(t, strings.EqualFold(key, "cookie"), "header %s must not be stored", key)
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

func allowlistProviders(t *testing.T) map[string]domain.ProviderConfig {
	t.Helper()
	providers, err := domain.ParseProviders(
		`{"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id","secret_env":"MOCK_SECRET","ip_allowlist":["203.0.113.0/24","2001:db8::/32","192.0.2.7"]}}`,
		func(key string) string {
			if key == "MOCK_SECRET" {
				return testSecret
			}
			return ""
		},
	)
	require.NoError(t, err)
	return providers
}

func requestFrom(remoteAddr, body, eventID string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/hooks/mock/ep1", strings.NewReader(body))
	req.RemoteAddr = remoteAddr
	req.Header.Set("X-Event-Id", eventID)
	req.Header.Set("X-Signature", hmac.BuildHeader(hmac.Signature{
		Timestamp: fixedNow,
		Hex:       hmac.Sign([]byte(testSecret), fixedNow, []byte(body)),
	}))
	return req
}

func TestHandleWebhookIPAllowlist(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		remoteAddr string
		wantCode   int
	}{
		"allowed subnet":   {remoteAddr: "203.0.113.7:41234", wantCode: http.StatusOK},
		"allowed no port":  {remoteAddr: "203.0.113.7", wantCode: http.StatusOK},
		"allowed single":   {remoteAddr: "192.0.2.7:41234", wantCode: http.StatusOK},
		"allowed ipv6":     {remoteAddr: "[2001:db8::1]:41234", wantCode: http.StatusOK},
		"denied ipv4":      {remoteAddr: "198.51.100.9:41234", wantCode: http.StatusForbidden},
		"denied ipv6":      {remoteAddr: "[2001:db9::1]:41234", wantCode: http.StatusForbidden},
		"denied ipv6 zone": {remoteAddr: "[fe80::1%eth0]:41234", wantCode: http.StatusForbidden},
		"garbage peer":     {remoteAddr: "not-an-address", wantCode: http.StatusForbidden},
		"empty peer":       {remoteAddr: "", wantCode: http.StatusForbidden},
		"ipv4 mapped":      {remoteAddr: "[::ffff:203.0.113.7]:41234", wantCode: http.StatusOK},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			handler := &fakeHandler{processed: true}
			router := newTestRouterWithProviders(allowlistProviders(t), handler)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, requestFrom(tc.remoteAddr, "{}", "vendor-1"))
			require.Equal(t, tc.wantCode, rec.Code)
			if tc.wantCode != http.StatusOK {
				assert.Contains(t, rec.Body.String(), "ip_forbidden")
				assert.Equal(t, 0, handler.calls)
			}
		})
	}
}

func TestHandleWebhookIPAllowlistIgnoresForwardedHeaders(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(allowlistProviders(t), handler)

	req := requestFrom("198.51.100.9:41234", "{}", "vendor-1")
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	req.Header.Set("X-Real-IP", "203.0.113.7")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, 0, handler.calls)
}

func TestHandleWebhookIPAllowlistBeforeBodyAndSignature(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(allowlistProviders(t), handler, transport.WithMaxBody(16))

	// Body over the limit and a missing signature: if the body were read or the
	// signature verified before the allowlist, the response would be 413 or 401.
	req := httptest.NewRequest(http.MethodPost, "/hooks/mock/ep1", strings.NewReader(strings.Repeat("a", 100)))
	req.RemoteAddr = "198.51.100.9:41234"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "ip_forbidden")
	assert.Equal(t, 0, handler.calls)
}

func TestHandleWebhookRateLimited(t *testing.T) {
	t.Parallel()

	providers, err := domain.ParseProviders(
		`{"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id","secret_env":"MOCK_SECRET","rate_rps":1,"rate_burst":1}}`,
		func(key string) string {
			if key == "MOCK_SECRET" {
				return testSecret
			}
			return ""
		},
	)
	require.NoError(t, err)

	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	first := httptest.NewRecorder()
	router.ServeHTTP(first, requestFrom("203.0.113.7:1", "{}", "vendor-1"))
	require.Equal(t, http.StatusOK, first.Code)

	// Body over the limit and a missing signature: the rate limit is checked earlier.
	second := httptest.NewRequest(http.MethodPost, "/hooks/mock/ep1", strings.NewReader(strings.Repeat("a", 1024)))
	second.RemoteAddr = "203.0.113.7:1"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, second)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), "rate_limited")
	assert.Equal(t, 1, handler.calls, "a rejected request does not reach the write")
}

func TestHandleWebhookRateLimitRefill(t *testing.T) {
	t.Parallel()

	providers, err := domain.ParseProviders(
		`{"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id","secret_env":"MOCK_SECRET","rate_rps":10,"rate_burst":1}}`,
		func(key string) string {
			if key == "MOCK_SECRET" {
				return testSecret
			}
			return ""
		},
	)
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		now = fixedNow
	)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler, transport.WithClock(clock))

	first := httptest.NewRecorder()
	router.ServeHTTP(first, requestFrom("203.0.113.7:1", "{}", "vendor-1"))
	require.Equal(t, http.StatusOK, first.Code)

	second := httptest.NewRecorder()
	router.ServeHTTP(second, requestFrom("203.0.113.7:1", "{}", "vendor-2"))
	require.Equal(t, http.StatusTooManyRequests, second.Code)

	mu.Lock()
	now = now.Add(100 * time.Millisecond)
	mu.Unlock()

	third := httptest.NewRecorder()
	router.ServeHTTP(third, requestFrom("203.0.113.7:1", "{}", "vendor-3"))
	require.Equal(t, http.StatusOK, third.Code, "the token is refilled over time")
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

// blockingHandler simulates a slow PostgreSQL write that
// does not fit into the response budget.
type blockingHandler struct {
	calls int
}

// Handle waits for the context deadline and returns a wrapped error, the same way
// the usecase wrapper does on top of an aborted transaction.
func (h *blockingHandler) Handle(ctx context.Context, _ usecase.WebhookInput) (bool, error) {
	h.calls++
	<-ctx.Done()
	return false, fmt.Errorf("usecase: save webhook: %w", ctx.Err())
}

func TestHandleWebhookWriteTimeout(t *testing.T) {
	t.Parallel()

	handler := &blockingHandler{}
	router := newTestRouter(handler, transport.WithWriteTimeout(10*time.Millisecond))

	req := signedRequest("{}", "vendor-1", fixedNow)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		router.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not finish within the write budget")
	}

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), "write_timeout")
	assert.Equal(t, 1, handler.calls)
}

// dialTimeoutHandler simulates budget exhaustion on PostgreSQL connection
// establishment (Linux): dial terminates with os.ErrDeadlineExceeded ("i/o timeout").
type dialTimeoutHandler struct {
	calls int
}

// Handle waits for the context deadline and returns an error in the Linux pgx form.
func (h *dialTimeoutHandler) Handle(ctx context.Context, _ usecase.WebhookInput) (bool, error) {
	h.calls++
	<-ctx.Done()
	return false, fmt.Errorf("usecase: save webhook: dial error: timeout: dial tcp: %w", os.ErrDeadlineExceeded)
}

func TestHandleWebhookWriteTimeoutOnDialError(t *testing.T) {
	t.Parallel()

	handler := &dialTimeoutHandler{}
	router := newTestRouter(handler, transport.WithWriteTimeout(10*time.Millisecond))

	req := signedRequest("{}", "vendor-1", fixedNow)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		router.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not finish within the write budget")
	}

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), "write_timeout")
	assert.Equal(t, 1, handler.calls)
}

func TestHandleWebhookIoTimeoutIsWriteTimeout(t *testing.T) {
	t.Parallel()

	// An i/o timeout in the write path (the only external I/O is PostgreSQL under
	// the budget) is classified as budget exhaustion.
	handler := &fakeHandler{err: fmt.Errorf("usecase: save webhook: dial error: %w", os.ErrDeadlineExceeded)}
	router := newTestRouter(handler)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signedRequest("{}", "vendor-1", fixedNow))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), "write_timeout")
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
