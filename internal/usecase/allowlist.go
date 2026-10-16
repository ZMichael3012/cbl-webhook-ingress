package usecase

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/netip"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

// allowlistFetchTimeout bounds a single provider list request.
const allowlistFetchTimeout = 10 * time.Second

// maxAllowlistBodyBytes bounds a provider list response.
const maxAllowlistBodyBytes int64 = 1 << 20

// AllowlistRefresher loads provider IP lists from configured URLs on a schedule
// and atomically replaces the dynamic allowlist sets; on a fetch or parse error
// the last known set of that provider is kept (17.4).
type AllowlistRefresher struct {
	sources  map[string]string
	target   *domain.DynamicAllowlist
	client   *http.Client
	interval time.Duration
	log      *zap.Logger
}

// NewAllowlistRefresher creates a refresher; a nil logger becomes a no-op.
// The HTTP client rejects redirects: a redirected source could silently widen
// the allowlist of a provider (T-WH-08).
func NewAllowlistRefresher(sources map[string]string, target *domain.DynamicAllowlist, interval time.Duration, log *zap.Logger) *AllowlistRefresher {
	if log == nil {
		log = zap.NewNop()
	}
	return &AllowlistRefresher{
		sources: sources,
		target:  target,
		client: &http.Client{
			Timeout: allowlistFetchTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		interval: interval,
		log:      log,
	}
}

// Refresh loads all sources once; every provider is processed independently and
// the first error is returned for observability while other providers still update.
func (r *AllowlistRefresher) Refresh(ctx context.Context) error {
	var firstErr error
	for _, provider := range slices.Sorted(maps.Keys(r.sources)) {
		prefixes, err := r.fetch(ctx, r.sources[provider])
		if err != nil {
			r.log.Warn("dynamic allowlist refresh failed",
				zap.String("provider", provider),
				zap.Error(err),
			)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		r.target.Replace(provider, prefixes)
		r.log.Info("dynamic allowlist updated",
			zap.String("provider", provider),
			zap.Int("entries", len(prefixes)),
		)
	}
	return firstErr
}

// Run refreshes on every tick until the context is done; the first load is
// performed by the caller before the intake starts, so the service does not
// serve requests in the fail-closed window (17.4).
func (r *AllowlistRefresher) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Refresh(ctx); err != nil {
				// Already logged per provider; keep the last known sets.
				continue
			}
		}
	}
}

// fetch downloads and parses one provider list.
func (r *AllowlistRefresher) fetch(ctx context.Context, url string) ([]netip.Prefix, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("usecase: build allowlist request: %w", err)
	}
	response, err := r.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("usecase: fetch allowlist: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("usecase: allowlist source %s returned %d", url, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxAllowlistBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("usecase: read allowlist: %w", err)
	}
	if int64(len(body)) > maxAllowlistBodyBytes {
		// A silently truncated list could replace the last known set with a
		// partial one: an oversized response is an error instead.
		return nil, fmt.Errorf("usecase: allowlist source %s exceeds %d bytes", url, maxAllowlistBodyBytes)
	}
	prefixes, err := domain.ParseAllowlistText(string(body))
	if err != nil {
		return nil, fmt.Errorf("usecase: parse allowlist %s: %w", url, err)
	}
	return prefixes, nil
}
