package domain_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

func parseVerified(t *testing.T, raw string) domain.ProviderConfig {
	t.Helper()
	providers, err := domain.ParseProviders(raw, func(key string) string {
		if key == "SECRET_ENV" {
			return "s3cret"
		}
		return ""
	})
	require.NoError(t, err)
	require.Len(t, providers, 1)
	for _, config := range providers {
		return config
	}
	return domain.ProviderConfig{}
}

func TestParseAndVerifyRawHMAC(t *testing.T) {
	t.Parallel()
	config := parseVerified(t, `{"btcpay":{
		"secret_env":"SECRET_ENV",
		"verification":{"strategy":"raw_hmac","header":"BTCPay-Sig","prefix":"sha256="},
		"fetch_required":true
	}}`)
	require.Equal(t, domain.StrategyRawHMAC, config.StrategyName())
	require.Equal(t, "BTCPay-Sig", config.SignatureHeaderName())
	require.True(t, config.FetchRequired)

	body := []byte(`{"deliveryId":"abc"}`)
	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write(body)
	header := http.Header{}
	header.Set("Btcpay-Sig", "sha256="+hex.EncodeToString(mac.Sum(nil)))

	state, err := config.VerifyDelivery(header, body, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyVerified, state)

	wrong := http.Header{}
	wrong.Set("Btcpay-Sig", "sha256="+hex.EncodeToString([]byte("nope")))
	state, err = config.VerifyDelivery(wrong, body, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyHMACFailed, state)

	state, err = config.VerifyDelivery(http.Header{}, body, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyAbsent, state)
}

func TestVerifyCanonicalJSONHMAC(t *testing.T) {
	t.Parallel()
	config := parseVerified(t, `{"nowpayments":{
		"secret_env":"SECRET_ENV",
		"verification":{"strategy":"canonical_json_hmac","header":"x-nowpayments-sig","algorithm":"sha512"}
	}}`)
	body := []byte(`{"price_amount": 10.5, "order_id": "a"}`)
	canonical, err := webhooksig.CanonicalJSON(body)
	require.NoError(t, err)
	mac := hmac.New(sha512.New, []byte("s3cret"))
	mac.Write(canonical)
	header := http.Header{}
	header.Set("X-Nowpayments-Sig", hex.EncodeToString(mac.Sum(nil)))

	state, err := config.VerifyDelivery(header, body, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyVerified, state)

	rawMac := hmac.New(sha512.New, []byte("s3cret"))
	rawMac.Write(body)
	rawHeader := http.Header{}
	rawHeader.Set("X-Nowpayments-Sig", hex.EncodeToString(rawMac.Sum(nil)))
	state, err = config.VerifyDelivery(rawHeader, body, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyHMACFailed, state)
}

func TestVerifyTokenEcho(t *testing.T) {
	t.Parallel()
	config := parseVerified(t, `{"coingate":{
		"secret_env":"SECRET_ENV",
		"verification":{"strategy":"token_echo","field":"token"},
		"fetch_required":true
	}}`)
	require.Equal(t, "", config.SignatureHeaderName())

	body := []byte(`{"order_id":1,"token":"s3cret"}`)
	state, err := config.VerifyDelivery(http.Header{}, body, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyVerified, state)

	mismatch := []byte(`{"order_id":1,"token":"other"}`)
	state, err = config.VerifyDelivery(http.Header{}, mismatch, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyEchoMismatch, state)

	absent := []byte(`{"order_id":1}`)
	state, err = config.VerifyDelivery(http.Header{}, absent, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyAbsent, state)

	notString := []byte(`{"order_id":1,"token":2}`)
	state, err = config.VerifyDelivery(http.Header{}, notString, time.Now(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, domain.VerifyHMACFailed, state)
}

func TestResponsePolicy(t *testing.T) {
	t.Parallel()
	token := parseVerified(t, `{"coingate":{"secret_env":"SECRET_ENV","verification":{"strategy":"token_echo"}}}`)
	status, record := token.ResponsePolicy(domain.VerifyEchoMismatch)
	require.Equal(t, http.StatusOK, status)
	require.True(t, record)

	hmacConfig := parseVerified(t, `{"btcpay":{"secret_env":"SECRET_ENV","verification":{"strategy":"raw_hmac","header":"BTCPay-Sig"}}}`)
	status, record = hmacConfig.ResponsePolicy(domain.VerifyVerified)
	require.Equal(t, http.StatusOK, status)
	require.True(t, record)
	status, record = hmacConfig.ResponsePolicy(domain.VerifyHMACFailed)
	require.Equal(t, http.StatusUnauthorized, status)
	require.False(t, record)

	legacy := domain.ProviderConfig{Name: "mock", SignatureHeader: "X-Signature"}
	status, record = legacy.ResponsePolicy(domain.VerifyAbsent)
	require.Equal(t, http.StatusUnauthorized, status)
	require.False(t, record)
}

func TestRedactToken(t *testing.T) {
	t.Parallel()
	config := parseVerified(t, `{"coingate":{"secret_env":"SECRET_ENV","verification":{"strategy":"token_echo"}}}`)
	redacted := config.RedactToken([]byte(`{"order_id":1,"token":"s3cret"}`))
	require.JSONEq(t, `{"order_id":1,"token":""}`, string(redacted))

	unchanged := config.RedactToken([]byte(`not json`))
	require.Equal(t, `not json`, string(unchanged))

	raw := parseVerified(t, `{"btcpay":{"secret_env":"SECRET_ENV","verification":{"strategy":"raw_hmac","header":"BTCPay-Sig"}}}`)
	body := []byte(`{"token":"s3cret"}`)
	require.Equal(t, body, raw.RedactToken(body))
}

func TestParseProvidersVerificationValidation(t *testing.T) {
	t.Parallel()
	lookup := func(string) string { return "s" }
	cases := map[string]string{
		"unknown strategy":       `{"p":{"secret_env":"E","verification":{"strategy":"magic"}}}`,
		"raw without header":     `{"p":{"secret_env":"E","verification":{"strategy":"raw_hmac"}}}`,
		"bad algorithm":          `{"p":{"secret_env":"E","verification":{"strategy":"raw_hmac","header":"X","algorithm":"md5"}}}`,
		"bad encoding":           `{"p":{"secret_env":"E","verification":{"strategy":"raw_hmac","header":"X","encoding":"oct"}}}`,
		"no strategy":            `{"p":{"secret_env":"E"}}`,
		"event key both":         `{"p":{"secret_env":"E","verification":{"strategy":"token_echo"},"event_key":{"header":"X","template":"{a}"}}}`,
		"event key empty":        `{"p":{"secret_env":"E","verification":{"strategy":"token_echo"},"event_key":{}}}`,
		"event key template bad": `{"p":{"secret_env":"E","verification":{"strategy":"token_echo"},"event_key":{"template":"constant"}}}`,
		"allowlist url bad":      `{"p":{"secret_env":"E","verification":{"strategy":"token_echo"},"ip_allowlist_url":"ftp://x"}}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := domain.ParseProviders(raw, lookup)
			require.ErrorIs(t, err, domain.ErrInvalidConfig)
		})
	}
}
