package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// DefaultWriteTimeout задает бюджет записи вебхука по умолчанию.
const DefaultWriteTimeout = 450 * time.Millisecond

// Границы допустимого бюджета записи вебхука.
const (
	// MinWriteTimeout задает нижнюю границу бюджета записи.
	MinWriteTimeout = 100 * time.Millisecond
	// MaxWriteTimeout задает верхнюю границу бюджета записи.
	MaxWriteTimeout = 5 * time.Second
)

// ErrInvalidWriteTimeout сообщает о некорректном бюджете записи вебхука.
var ErrInvalidWriteTimeout = errors.New("webhook-ingress: invalid write timeout")

// ParseWriteTimeout разбирает конфигурацию бюджета записи вебхука:
// пустое значение дает DefaultWriteTimeout, значения вне границ
// прижимаются к MinWriteTimeout и MaxWriteTimeout, синтаксически
// невалидные отклоняются.
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
