package domain

import (
	"sync"
	"time"
)

// Limiter реализует потокобезопасный token bucket с ленивым пополнением:
// фоновые горутины не нужны, состояние меняется только при вызовах Allow.
type Limiter struct {
	mu     sync.Mutex
	rps    float64
	burst  float64
	tokens float64
	last   time.Time
}

// NewLimiter создает Limiter со скоростью rps запросов в секунду и всплеском burst.
// Нулевая или отрицательная скорость отключает ограничение; нулевой всплеск
// принимается равным скорости.
func NewLimiter(rps, burst int) *Limiter {
	l := &Limiter{}
	if rps <= 0 {
		return l
	}
	if burst <= 0 {
		burst = rps
	}
	l.rps = float64(rps)
	l.burst = float64(burst)
	l.tokens = float64(burst)
	return l
}

// Allow сообщает, можно ли принять запрос в момент now: пополняет bucket
// пропорционально прошедшему времени и расходует один токен.
func (l *Limiter) Allow(now time.Time) bool {
	if l == nil || l.rps <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last.IsZero() {
		l.last = now
	}
	if elapsed := now.Sub(l.last).Seconds(); elapsed > 0 {
		l.tokens += elapsed * l.rps
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.last = now
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
