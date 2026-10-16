package domain_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

var limiterNow = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

func TestLimiterDisabled(t *testing.T) {
	t.Parallel()

	limiter := domain.NewLimiter(0, 0)
	for i := 0; i < 100; i++ {
		assert.True(t, limiter.Allow(limiterNow))
	}
	assert.True(t, (*domain.Limiter)(nil).Allow(limiterNow))
}

func TestLimiterBurstAndRefill(t *testing.T) {
	t.Parallel()

	limiter := domain.NewLimiter(10, 3)
	for i := 0; i < 3; i++ {
		assert.Truef(t, limiter.Allow(limiterNow), "token %d is within the burst", i)
	}
	assert.False(t, limiter.Allow(limiterNow), "burst is exhausted")

	assert.False(t, limiter.Allow(limiterNow.Add(50*time.Millisecond)), "half a token is not accepted")
	assert.True(t, limiter.Allow(limiterNow.Add(100*time.Millisecond)), "token is refilled")
	assert.False(t, limiter.Allow(limiterNow.Add(100*time.Millisecond)), "token is consumed")
}

func TestLimiterBurstDefaultsToRate(t *testing.T) {
	t.Parallel()

	limiter := domain.NewLimiter(5, 0)
	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow(limiterNow))
	}
	assert.False(t, limiter.Allow(limiterNow))
}

func TestLimiterCapsAtBurst(t *testing.T) {
	t.Parallel()

	limiter := domain.NewLimiter(10, 2)
	assert.True(t, limiter.Allow(limiterNow))
	assert.True(t, limiter.Allow(limiterNow))
	assert.False(t, limiter.Allow(limiterNow))

	later := limiterNow.Add(time.Hour)
	for i := 0; i < 2; i++ {
		assert.True(t, limiter.Allow(later), "refill does not exceed the burst")
	}
	assert.False(t, limiter.Allow(later))
}

func TestLimiterClockSkew(t *testing.T) {
	t.Parallel()

	limiter := domain.NewLimiter(10, 1)
	assert.True(t, limiter.Allow(limiterNow))
	assert.False(t, limiter.Allow(limiterNow.Add(-time.Second)), "a clock going backwards does not refill the bucket")
}

func TestLimiterConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100
	limiter := domain.NewLimiter(1000, burst)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 400; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.Allow(limiterNow) {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(burst), allowed.Load(), "concurrent calls consume exactly the burst")
}
