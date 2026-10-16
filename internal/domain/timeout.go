package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// DefaultWriteTimeout sets the default webhook write budget.
const DefaultWriteTimeout = 450 * time.Millisecond

// Bounds of the allowed webhook write budget.
const (
	// MinWriteTimeout sets the lower bound of the write budget.
	MinWriteTimeout = 100 * time.Millisecond
	// MaxWriteTimeout sets the upper bound of the write budget.
	MaxWriteTimeout = 5 * time.Second
)

// ErrInvalidWriteTimeout reports an invalid webhook write budget.
var ErrInvalidWriteTimeout = errors.New("webhook-ingress: invalid write timeout")

// ParseWriteTimeout parses the webhook write budget configuration:
// an empty value yields DefaultWriteTimeout, out-of-bounds values
// are clamped to MinWriteTimeout and MaxWriteTimeout, and syntactically
// invalid values are rejected.
func ParseWriteTimeout(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultWriteTimeout, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidWriteTimeout, raw)
	}
	if parsed < MinWriteTimeout {
		return MinWriteTimeout, nil
	}
	if parsed > MaxWriteTimeout {
		return MaxWriteTimeout, nil
	}
	return parsed, nil
}
