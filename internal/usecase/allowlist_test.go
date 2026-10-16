package usecase_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
	"github.com/cbl/cbl-webhook-ingress/internal/usecase"
)

func TestAllowlistRefresherLoadsAndReplaces(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("203.0.113.5\n198.51.100.0/24\n"))
	}))
	defer server.Close()

	target := domain.NewDynamicAllowlist()
	refresher := usecase.NewAllowlistRefresher(
		map[string]string{"coingate": server.URL},
		target,
		time.Minute,
		zap.NewNop(),
	)
	require.NoError(t, refresher.Refresh(context.Background()))
	require.Equal(t, 2, target.Size("coingate"))
	require.True(t, target.Allows("coingate", netip.MustParseAddr("203.0.113.5")))
}

func TestAllowlistRefresherKeepsLastKnownOnError(t *testing.T) {
	t.Parallel()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()

	target := domain.NewDynamicAllowlist()
	target.Replace("coingate", []netip.Prefix{netip.MustParsePrefix("203.0.113.0/24")})
	refresher := usecase.NewAllowlistRefresher(
		map[string]string{"coingate": failing.URL},
		target,
		time.Minute,
		zap.NewNop(),
	)
	require.Error(t, refresher.Refresh(context.Background()))
	require.Equal(t, 1, target.Size("coingate"))

	badBody := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("garbage"))
	}))
	defer badBody.Close()
	refresher = usecase.NewAllowlistRefresher(
		map[string]string{"coingate": badBody.URL},
		target,
		time.Minute,
		zap.NewNop(),
	)
	require.Error(t, refresher.Refresh(context.Background()))
	require.Equal(t, 1, target.Size("coingate"))
}

func TestAllowlistRefresherContinuesOtherProviders(t *testing.T) {
	t.Parallel()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("203.0.113.5"))
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer bad.Close()

	target := domain.NewDynamicAllowlist()
	refresher := usecase.NewAllowlistRefresher(
		map[string]string{"a-bad": bad.URL, "good": good.URL},
		target,
		time.Minute,
		zap.NewNop(),
	)
	require.Error(t, refresher.Refresh(context.Background()))
	require.Equal(t, 1, target.Size("good"))
	require.Zero(t, target.Size("a-bad"))
}
