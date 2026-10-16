package domain_test

import (
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

func TestParseAllowlistText(t *testing.T) {
	t.Parallel()
	prefixes, err := domain.ParseAllowlistText("203.0.113.5\n198.51.100.0/24, 2001:db8::1")
	require.NoError(t, err)
	require.Len(t, prefixes, 3)

	_, err = domain.ParseAllowlistText("   ")
	require.Error(t, err)

	_, err = domain.ParseAllowlistText("not-an-ip")
	require.Error(t, err)

	_, err = domain.ParseAllowlistText("0.0.0.0/0")
	require.ErrorContains(t, err, "catch-all")

	_, err = domain.ParseAllowlistText("::/0")
	require.ErrorContains(t, err, "catch-all")

	_, err = domain.ParseAllowlistText("127.0.0.1")
	require.ErrorContains(t, err, "loopback")

	_, err = domain.ParseAllowlistText("169.254.1.1")
	require.ErrorContains(t, err, "link-local")
}

func TestDynamicAllowlist(t *testing.T) {
	t.Parallel()
	dynamic := domain.NewDynamicAllowlist()
	require.Zero(t, dynamic.Size("coingate"))
	require.False(t, dynamic.Allows("coingate", netip.MustParseAddr("203.0.113.5")))

	dynamic.Replace("coingate", []netip.Prefix{netip.MustParsePrefix("203.0.113.0/24")})
	require.Equal(t, 1, dynamic.Size("coingate"))
	require.True(t, dynamic.Allows("coingate", netip.MustParseAddr("203.0.113.5")))
	require.False(t, dynamic.Allows("coingate", netip.MustParseAddr("198.51.100.5")))
	require.False(t, dynamic.Allows("btcpay", netip.MustParseAddr("203.0.113.5")))

	dynamic.Replace("coingate", nil)
	require.Zero(t, dynamic.Size("coingate"))
}

func TestParseAllowlistRefresh(t *testing.T) {
	t.Parallel()
	interval, err := domain.ParseAllowlistRefresh("")
	require.NoError(t, err)
	require.Equal(t, domain.DefaultAllowlistRefresh, interval)

	interval, err = domain.ParseAllowlistRefresh("0")
	require.NoError(t, err)
	require.Zero(t, interval)

	interval, err = domain.ParseAllowlistRefresh("1s")
	require.NoError(t, err)
	require.Equal(t, domain.MinAllowlistRefresh, interval)

	interval, err = domain.ParseAllowlistRefresh("48h")
	require.NoError(t, err)
	require.Equal(t, domain.MaxAllowlistRefresh, interval)

	_, err = domain.ParseAllowlistRefresh("soon")
	require.ErrorIs(t, err, domain.ErrInvalidConfig)

	interval, err = domain.ParseAllowlistRefresh("5m")
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, interval)
}
