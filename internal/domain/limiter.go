package domain

import (
	"sync"
	"time"
)

// Limiter implements a thread-safe token bucket with lazy refill:
// no background goroutines are needed, the state changes only in Allow calls.
type Limiter struct {
	mu     sync.Mutex
	rps    float64
	burst  float64
	tokens float64
	last   time.Time
}

// NewLimiter creates a Limiter with the rate of rps requests per second and burst capacity.
// A zero or negative rate disables limiting; a zero burst defaults to the rate.
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

// Allow reports whether a request can be accepted at now: it refills the bucket
// proportionally to the elapsed time and consumes one token.
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
