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
		"fetch_required":true,
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

func TestHandleWebhookGatewayIDTemplate(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"nowpayments":{
		"secret_env":"E",
		"fetch_required":true,
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{payment_id}:{payment_status}"},
		"gateway_id":{"template":"{payment_id}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"payment_id":"np-1","payment_status":"finished","token":"` + testSecret + `"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/nowpayments/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "np-1", handler.lastInput.GatewayID,
		"the configured template resolves the provider id the connector fetch needs")
}

func TestHandleWebhookGatewayIDLegacyNumeric(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"fetch_required":true,
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{id}:{status}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"id":1042,"status":"paid","token":"` + testSecret + `"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "1042", handler.lastInput.GatewayID,
		"the real CoinGate callback carries a numeric id")
}

func TestHandleWebhookGatewayIDBitPayNested(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"bitpay":{
		"secret_env":"E",
		"fetch_required":true,
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{data.id}:{data.status}"},
		"gateway_id":{"template":"{data.id}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"data":{"id":"inv-1","status":"complete"},"token":"` + testSecret + `"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/bitpay/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "inv-1", handler.lastInput.GatewayID, "the nested BitPay invoice id resolves")
}

func TestHandleWebhookGatewayIDOversizedTemplateResultDropped(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"nowpayments":{
		"secret_env":"E",
		"fetch_required":true,
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{payment_id}:{payment_status}"},
		"gateway_id":{"template":"{payment_id}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	oversized := strings.Repeat("a", 129)
	body := `{"payment_id":"` + oversized + `","payment_status":"finished","token":"` + testSecret + `"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/nowpayments/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "", handler.lastInput.GatewayID,
		"an oversized resolved id is dropped so the Kafka key stays bounded")
}

func TestHandleWebhookTokenEchoMissingKeyUsesRandomID(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"fetch_required":true,
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{id}:{status}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	body := `{"id":1,"token":"` + testSecret + `"}`
	req := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	// Token echo must never answer 4xx: the delivery is recorded and the
	// handler assigns a random event id.
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, handler.calls)
	require.Equal(t, domain.VerifyVerified, handler.lastInput.SignatureState)
	require.Equal(t, "", handler.lastInput.VendorEventID)
}

func TestHandleWebhookOversizedVendorKeyRejected(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"bitpay":{
		"secret_env":"E",
		"verification":{"strategy":"raw_hmac","header":"X-Sig"},
		"event_key":{"template":"{data.id}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	longID := strings.Repeat("a", 200)
	body := `{"data":{"id":"` + longID + `"}}`
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(body))
	req := httptest.NewRequest(http.MethodPost, "/hooks/bitpay/ep1", strings.NewReader(body))
	req.Header.Set("X-Sig", hex.EncodeToString(mac.Sum(nil)))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	require.Zero(t, handler.calls)
}

func TestHandleWebhookRedactionFailureDropsBody(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"fetch_required":true,
		"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{id}:{status}"}
	}}`)
	handler := &fakeHandler{processed: true}
	router := newTestRouterWithProviders(providers, handler)

	// The body cannot be redacted (trailing data): the raw body must not be stored.
	body := `{"id":1,"status":"new","token":"` + testSecret + `"} trailing`
	req := httptest.NewRequest(http.MethodPost, "/hooks/coingate/ep1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, handler.calls)
	require.Nil(t, handler.lastInput.RawBody)
}

func TestHandleWebhookDynamicAllowlist(t *testing.T) {
	t.Parallel()
	providers := parsedProviders(t, `{"coingate":{
		"secret_env":"E",
		"fetch_required":true,
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
