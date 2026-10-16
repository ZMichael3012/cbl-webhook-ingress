package domain_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

func keyConfig(t *testing.T, raw string) domain.ProviderConfig {
	t.Helper()
	providers, err := domain.ParseProviders(raw, func(string) string { return "s" })
	require.NoError(t, err)
	for _, config := range providers {
		return config
	}
	return domain.ProviderConfig{}
}

func TestVendorEventKeyFromHeader(t *testing.T) {
	t.Parallel()
	config := keyConfig(t, `{"btcpay":{"secret_env":"E","verification":{"strategy":"raw_hmac","header":"X"},
		"event_key":{"header":"X-Delivery-Id"}}}`)
	header := http.Header{}
	header.Set("X-Delivery-Id", "delivery-1")
	key, err := config.VendorEventKey(header, []byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, "delivery-1", key)

	_, err = config.VendorEventKey(http.Header{}, []byte(`{}`))
	require.ErrorIs(t, err, domain.ErrMissingEventKey)
}

func TestVendorEventKeyFromTemplate(t *testing.T) {
	t.Parallel()
	config := keyConfig(t, `{"coingate":{"secret_env":"E","fetch_required":true,"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{id}:{status}"}}}`)

	key, err := config.VendorEventKey(http.Header{}, []byte(`{"id":123,"status":"paid"}`))
	require.NoError(t, err)
	require.Equal(t, "123:paid", key)

	_, err = config.VendorEventKey(http.Header{}, []byte(`{"id":123}`))
	require.ErrorIs(t, err, domain.ErrMissingEventKey)

	_, err = config.VendorEventKey(http.Header{}, []byte(`{"id":123,"status":"  "}`))
	require.ErrorIs(t, err, domain.ErrMissingEventKey)

	_, err = config.VendorEventKey(http.Header{}, []byte(`{"id":123,"status":true}`))
	require.ErrorIs(t, err, domain.ErrMissingEventKey)

	_, err = config.VendorEventKey(http.Header{}, []byte(`not json`))
	require.ErrorIs(t, err, domain.ErrMissingEventKey)
}

func TestVendorEventKeyNestedTemplate(t *testing.T) {
	t.Parallel()
	config := keyConfig(t, `{"bitpay":{"secret_env":"E","verification":{"strategy":"raw_hmac","header":"X"},
		"event_key":{"template":"{data.id}-{data.status}"}}}`)
	key, err := config.VendorEventKey(http.Header{}, []byte(`{"data":{"id":"inv1","status":"paid"}}`))
	require.NoError(t, err)
	require.Equal(t, "inv1-paid", key)
}

func TestVendorEventKeyTemplateAlternatives(t *testing.T) {
	t.Parallel()
	config := keyConfig(t, `{"coingate":{"secret_env":"E","fetch_required":true,"verification":{"strategy":"token_echo"},
		"event_key":{"template":"{refund_id}:{refund_status}|{id}:{status}"}}}`)

	key, err := config.VendorEventKey(http.Header{}, []byte(`{"id":5,"status":"paid"}`))
	require.NoError(t, err)
	require.Equal(t, "5:paid", key)

	key, err = config.VendorEventKey(http.Header{}, []byte(`{"refund_id":9,"refund_status":"refunded"}`))
	require.NoError(t, err)
	require.Equal(t, "9:refunded", key)

	_, err = config.VendorEventKey(http.Header{}, []byte(`{"other":1}`))
	require.ErrorIs(t, err, domain.ErrMissingEventKey)
}

func TestVendorEventKeyOversized(t *testing.T) {
	t.Parallel()
	config := keyConfig(t, `{"bitpay":{"secret_env":"E","verification":{"strategy":"raw_hmac","header":"X"},
		"event_key":{"template":"{data.id}"}}}`)
	long := strings.Repeat("a", 129)
	_, err := config.VendorEventKey(http.Header{}, []byte(`{"data":{"id":"`+long+`"}}`))
	require.ErrorIs(t, err, domain.ErrMissingEventKey)
}

func TestVendorEventKeyLegacyFallback(t *testing.T) {
	t.Parallel()
	config := keyConfig(t, `{"mock":{"signature_header":"S","event_id_header":"X-Event-Id","secret_env":"E"}}`)
	header := http.Header{}
	header.Set("X-Event-Id", "legacy-1")
	key, err := config.VendorEventKey(header, []byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, "legacy-1", key)
}
