package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

func TestExtractGatewayIDTemplate(t *testing.T) {
	t.Parallel()

	now := domain.ProviderConfig{Name: "nowpayments", GatewayID: &domain.GatewayIDConfig{Template: "{payment_id}"}}
	assert.Equal(t, "np-1", now.ExtractGatewayID([]byte(`{"payment_id":"np-1","order_id":"order-1"}`)))
	assert.Equal(t, "", now.ExtractGatewayID([]byte(`{"order_id":"order-1"}`)), "an unresolvable template fails to unknown")

	bitpay := domain.ProviderConfig{Name: "bitpay", GatewayID: &domain.GatewayIDConfig{Template: "{data.id}"}}
	assert.Equal(t, "inv-1", bitpay.ExtractGatewayID([]byte(`{"data":{"id":"inv-1","status":"paid"}}`)))

	alternative := domain.ProviderConfig{Name: "p", GatewayID: &domain.GatewayIDConfig{Template: "{missing}|{payment_id}"}}
	assert.Equal(t, "np-2", alternative.ExtractGatewayID([]byte(`{"payment_id":"np-2"}`)),
		"alternative templates are tried in order, the event_key grammar")
}

func TestExtractGatewayIDLegacyFallback(t *testing.T) {
	t.Parallel()

	config := domain.ProviderConfig{Name: "coingate"}
	assert.Equal(t, "g-1", config.ExtractGatewayID([]byte(`{"gateway_id":"g-1"}`)))
	assert.Equal(t, "g-2", config.ExtractGatewayID([]byte(`{"gatewayId":"g-2"}`)))
	assert.Equal(t, "1042", config.ExtractGatewayID([]byte(`{"id":1042,"status":"paid"}`)),
		"the real CoinGate callback carries a numeric id")
	assert.Equal(t, "9007199254740993", config.ExtractGatewayID([]byte(`{"id":9007199254740993}`)),
		"a numeric id renders exactly, without float rounding")
	assert.Equal(t, "", config.ExtractGatewayID([]byte(`{"data":{"id":"nested"}}`)),
		"the legacy scan stays top-level only")
	assert.Equal(t, "", config.ExtractGatewayID([]byte(`not json`)))
}

func TestGatewayIDConfigValidation(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"empty template":       `{"p":{"secret_env":"E","signature_header":"S","event_id_header":"E","gateway_id":{}}}`,
		"unclosed placeholder": `{"p":{"secret_env":"E","signature_header":"S","event_id_header":"E","gateway_id":{"template":"{data.id"}}}`,
		"constant template":    `{"p":{"secret_env":"E","signature_header":"S","event_id_header":"E","gateway_id":{"template":"const"}}}`,
	}
	for name, raw := range cases {
		name, raw := name, raw
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := domain.ParseProviders(raw, func(string) string { return "secret" })
			require.Error(t, err)
		})
	}

	providers, err := domain.ParseProviders(`{"p":{"secret_env":"E","signature_header":"S","event_id_header":"E","gateway_id":{"template":"{payment_id}"}}}`, func(string) string { return "secret" })
	require.NoError(t, err)
	assert.Equal(t, "{payment_id}", providers["p"].GatewayID.Template)
}
