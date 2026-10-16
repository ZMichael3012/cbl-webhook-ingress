package transport_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
	"github.com/cbl/cbl-webhook-ingress/internal/transport"
)

func parsedProviders(t *testing.T, raw string) map[string]domain.ProviderConfig {
	t.Helper()
	providers, err := domain.ParseProviders(raw, func(string) string { return testSecret })
	require.NoError(t, err)
	return providers
}

func TestHandleWebhookTokenEchoRecordsUnverified(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"verification":{"strategy":"token_echo","field":"token"},
		"event_key":{"template":"{id}:{status}"},
		"fetch_required":true
	}}`)

	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"id":1,"status":"paid","token":"wrong"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, handler.calls)
	require.Equal(t, domain.VerifyEchoMismatch, handler.lastInput.SignatureState)
	require.True(t, handler.lastInput.FetchRequired)
	require.Equal(t, "1:paid", handler.lastInput.VendorEventID)

	var redacted map[string]any
	require.NoError(t, json.Unmarshal(handler.lastInput.RawBody, &redacted))
	require.Equal(t, "", redacted["token"])
}

func TestHandleWebhookTokenEchoVerified(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{id}:{status}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"id":7,"status":"new","token":"` + testSecret + `"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, domain.VerifyVerified, handler.lastInput.SignatureState)
	require.Equal(t, "7:new", handler.lastInput.VendorEventID)
}

func TestHandleWebhookRawHMACRejectsMismatch(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"btcpay":{
		"secret_env":"E",
		"verification":{"strategy":"raw_hmac","header":"X-Sig","prefix":"sha256="},
		"event_key":{"header":"X-Delivery-Id"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"id":"inv1"}`
	bad := httptest.NewRequest(http.MethodPost, "/hooks/btcpay/ep1", strings.NewReader(body))
	bad.Header.Set("X-Sig", "sha256="+strings.Repeat("0", 64))
	bad.Header.Set("X-Delivery-Id", "delivery-1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, bad)
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Zero(t, handler.calls)

	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(body))
	good := httptest.NewRequest(http.MethodPost, "/hooks/btcpay/ep1", strings.NewReader(body))
	good.Header.Set("X-Sig", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	good.Header.Set("X-Delivery-Id", "delivery-1")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, good)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, handler.calls)
	require.Equal(t, domain.VerifyVerified, handler.lastInput.SignatureState)
	require.Equal(t, "delivery-1", handler.lastInput.VendorEventID)
}

func TestHandleWebhookVerifiedTemplateMissingKey(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{id}:{status}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"id":1,"token":"` + testSecret + `"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	require.Zero(t, handler.calls)
}

func TestHandleWebhookDynamicAllowlist(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{id}:{status}"}
	}}`)
	dynamic := domain.NewDynamicAllowlist()
	dynamic.Replace("coingate", []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")})

	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler, transport.WithDynamicAllowlist(dynamic))

	body := `{"id":1,"status":"new","token":"` + testSecret + `"}`
	allowed := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	allowed.RemoteAddr = "198.51.100.7:3456"
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, allowed)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	denied := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	denied.RemoteAddr = "203.0.113.9:3456"
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, denied)
	require.Equal(t, http.StatusForbidden, recorder.Code)
}
