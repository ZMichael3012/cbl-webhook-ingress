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
		assert.Truef(t, limiter.Allow(limiterNow), "токен %d в пределах всплеска", i)
	}
	assert.False(t, limiter.Allow(limiterNow), "всплеск исчерпан")

	assert.False(t, limiter.Allow(limiterNow.Add(50*time.Millisecond)), "половина токена не принимается")
	assert.True(t, limiter.Allow(limiterNow.Add(100*time.Millisecond)), "токен восстановлен")
	assert.False(t, limiter.Allow(limiterNow.Add(100*time.Millisecond)), "токен израсходован")
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
		assert.True(t, limiter.Allow(later), "пополнение не превышает всплеск")
	}
	assert.False(t, limiter.Allow(later))
}

func TestLimiterClockSkew(t *testing.T) {
	t.Parallel()

	limiter := domain.NewLimiter(10, 1)
	assert.True(t, limiter.Allow(limiterNow))
	assert.False(t, limiter.Allow(limiterNow.Add(-time.Second)), "обратный ход часов не пополняет bucket")
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
	assert.Equal(t, int64(burst), allowed.Load(), "конкурентные вызовы расходуют ровно всплеск")
}
