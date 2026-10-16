package domain_test

import (
	"net/netip"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

func parseOne(t *testing.T, raw string) domain.ProviderConfig {
	t.Helper()
	providers, err := domain.ParseProviders(raw, func(string) string { return "secret" })
	require.NoError(t, err)
	return providers["mock"]
}

func TestParseProvidersAllowlist(t *testing.T) {
	t.Parallel()

	config := parseOne(t, `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC",
		"ip_allowlist":["10.0.0.0/8","192.0.2.7","2001:db8::/32"]}}`)
	require.Len(t, config.Allowlist, 3)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/8"), config.Allowlist[0])
	assert.Equal(t, netip.MustParsePrefix("192.0.2.7/32"), config.Allowlist[1])
	assert.Equal(t, netip.MustParsePrefix("2001:db8::/32"), config.Allowlist[2])
}

func TestParseProvidersAllowlistEntriesBoundary(t *testing.T) {
	t.Parallel()

	entries := make([]string, 0, domain.MaxAllowlistEntries)
	for i := 0; i < domain.MaxAllowlistEntries; i++ {
		entries = append(entries, `"10.0.`+strconv.Itoa(i)+`.0/24"`)
	}
	raw := `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":[` +
		strings.Join(entries, ",") + `]}}`
	config := parseOne(t, raw)
	assert.Len(t, config.Allowlist, domain.MaxAllowlistEntries)
}

func TestParseProvidersAllowlistErrors(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"invalid cidr":     `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":["10.0.0.0/33"]}}`,
		"empty entry":      `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":[" "]}}`,
		"hostname":         `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":["gateway.example.com"]}}`,
		"wide mapped cidr": `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":["::ffff:0.0.0.0/90"]}}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := domain.ParseProviders(raw, func(string) string { return "secret" })
			require.ErrorIs(t, err, domain.ErrInvalidConfig)
		})
	}

	entries := make([]string, 0, domain.MaxAllowlistEntries+1)
	for i := 0; i <= domain.MaxAllowlistEntries; i++ {
		entries = append(entries, `"10.0.0.`+strconv.Itoa(i%250)+`/32"`)
	}
	raw := `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":[` +
		strings.Join(entries, ",") + `]}}`
	_, err := domain.ParseProviders(raw, func(string) string { return "secret" })
	require.ErrorIs(t, err, domain.ErrInvalidConfig)
}

func TestParseProvidersMappedAllowlistNormalized(t *testing.T) {
	t.Parallel()

	config := parseOne(t, `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":["::ffff:10.0.0.0/104"]}}`)
	require.Len(t, config.Allowlist, 1)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/8"), config.Allowlist[0])
	assert.True(t, config.AllowsIP(netip.MustParseAddr("10.1.2.3")))
}

func TestMissingAllowlist(t *testing.T) {
	t.Parallel()

	providers, err := domain.ParseProviders(
		`{"alpha":{"signature_header":"S","event_id_header":"E","secret_env":"SEC"},"beta":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":["10.0.0.0/8"]}}`,
		func(string) string { return "secret" })
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha"}, domain.MissingAllowlist(providers))
}

func TestParseProvidersRateLimits(t *testing.T) {
	t.Parallel()

	config := parseOne(t, `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC",
		"rate_rps":100,"rate_burst":200}}`)
	assert.Equal(t, 100, config.RateRPS)
	assert.Equal(t, 200, config.RateBurst)

	defaultBurst := parseOne(t, `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","rate_rps":50}}`)
	assert.Equal(t, 50, defaultBurst.RateBurst, "an empty burst is normalized to rate_rps")

	boundary := parseOne(t, `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC",
		"rate_rps":100000,"rate_burst":1000000}}`)
	assert.Equal(t, domain.MaxRateRPS, boundary.RateRPS)
	assert.Equal(t, domain.MaxRateBurst, boundary.RateBurst)

	tests := map[string]string{
		"negative rps":       `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","rate_rps":-1}}`,
		"rps out of range":   `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","rate_rps":100001}}`,
		"negative burst":     `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","rate_rps":1,"rate_burst":-1}}`,
		"burst out of range": `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","rate_rps":1,"rate_burst":1000001}}`,
		"burst without rps":  `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","rate_burst":10}}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := domain.ParseProviders(raw, func(string) string { return "secret" })
			require.ErrorIs(t, err, domain.ErrInvalidConfig)
		})
	}
}

func TestProviderConfigAllowsIP(t *testing.T) {
	t.Parallel()

	empty := domain.ProviderConfig{}
	assert.True(t, empty.AllowsIP(netip.MustParseAddr("203.0.113.9")), "an empty allowlist allows everything")

	config := parseOne(t, `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC",
		"ip_allowlist":["10.0.0.0/8","192.0.2.7","2001:db8::/32"]}}`)
	assert.True(t, config.AllowsIP(netip.MustParseAddr("10.1.2.3")))
	assert.True(t, config.AllowsIP(netip.MustParseAddr("192.0.2.7")))
	assert.False(t, config.AllowsIP(netip.MustParseAddr("192.0.2.8")))
	assert.True(t, config.AllowsIP(netip.MustParseAddr("2001:db8::1")))
	assert.False(t, config.AllowsIP(netip.MustParseAddr("2001:db9::1")))
	assert.True(t, config.AllowsIP(netip.MustParseAddr("::ffff:10.1.2.3")), "an IPv4-mapped address is unmapped")
}

func TestParseProvidersAllowAllPrefix(t *testing.T) {
	t.Parallel()

	config := parseOne(t, `{"mock":{"signature_header":"S","event_id_header":"E","secret_env":"SEC","ip_allowlist":["0.0.0.0/0"]}}`)
	assert.True(t, config.AllowsIP(netip.MustParseAddr("203.0.113.9")))
	assert.False(t, config.AllowsIP(netip.MustParseAddr("2001:db8::1")))
}
