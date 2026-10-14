// Пакет domain содержит модель настроек приема вебхуков провайдеров.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
)

// ErrInvalidConfig сообщает о некорректной конфигурации провайдеров.
var ErrInvalidConfig = errors.New("webhook-ingress: invalid provider config")

// Границы конфигурации защиты провайдера.
const (
	// MaxAllowlistEntries задает предельное число записей allowlist.
	MaxAllowlistEntries = 64
	// MaxRateRPS задает верхнюю границу скорости приема в секунду.
	MaxRateRPS = 100_000
	// MaxRateBurst задает верхнюю границу всплеска лимита.
	MaxRateBurst = 1_000_000
)

// ProviderConfig описывает настройки приема вебхуков одного провайдера.
type ProviderConfig struct {
	// Name задает имя провайдера (ключ конфигурации).
	Name string `json:"-"`
	// SignatureHeader задает имя заголовка подписи.
	SignatureHeader string `json:"signature_header"`
	// EventIDHeader задает имя заголовка идентификатора события.
	EventIDHeader string `json:"event_id_header"`
	// SecretEnv задает имя переменной окружения с секретом подписи.
	SecretEnv string `json:"secret_env"`
	// Endpoints задает allowlist допустимых endpoint_id; пустое значение разрешает любые.
	Endpoints []string `json:"endpoints"`
	// IPAllowlist задает допустимые адреса шлюза (CIDR или одиночные адреса);
	// пустое значение разрешает любые адреса.
	IPAllowlist []string `json:"ip_allowlist"`
	// RateRPS задает устойчивую скорость приема запросов в секунду (0 - без ограничения).
	RateRPS int `json:"rate_rps"`
	// RateBurst задает вместимость всплеска лимита (0 - равен RateRPS).
	RateBurst int `json:"rate_burst"`
	// Secret задает секрет подписи, подставленный из окружения.
	Secret []byte `json:"-"`
	// Allowlist задает разобранные подсети шлюза.
	Allowlist []netip.Prefix `json:"-"`
}

// AllowsIP проверяет адрес пира по allowlist провайдера;
// пустой allowlist разрешает любой адрес (dev-режим).
func (c ProviderConfig) AllowsIP(addr netip.Addr) bool {
	if len(c.Allowlist) == 0 {
		return true
	}
	addr = addr.Unmap()
	for _, prefix := range c.Allowlist {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// ParseProviders разбирает JSON-конфигурацию провайдеров вида
// {"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id",
// "secret_env":"MOCK_WEBHOOK_SECRET"}} и подставляет секреты через lookupEnv.
// Необязательные поля ip_allowlist, rate_rps и rate_burst описывают защиту приема.
func ParseProviders(raw string, lookupEnv func(string) string) (map[string]ProviderConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%w: empty config", ErrInvalidConfig)
	}
	var parsed map[string]ProviderConfig
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if len(parsed) == 0 {
		return nil, fmt.Errorf("%w: no providers", ErrInvalidConfig)
	}
	providers := make(map[string]ProviderConfig, len(parsed))
	for name, config := range parsed {
		config.Name = name
		if strings.TrimSpace(config.SignatureHeader) == "" ||
			strings.TrimSpace(config.EventIDHeader) == "" ||
			strings.TrimSpace(config.SecretEnv) == "" {
			return nil, fmt.Errorf("%w: provider %q has empty fields", ErrInvalidConfig, name)
		}
		secret := lookupEnv(config.SecretEnv)
		if secret == "" {
			return nil, fmt.Errorf("%w: secret env %q is empty", ErrInvalidConfig, config.SecretEnv)
		}
		config.Secret = []byte(secret)
		allowlist, err := parseAllowlist(config.IPAllowlist)
		if err != nil {
			return nil, fmt.Errorf("%w: provider %q: %v", ErrInvalidConfig, name, err)
		}
		config.Allowlist = allowlist
		if err := validateRate(config, name); err != nil {
			return nil, err
		}
		if config.RateRPS > 0 && config.RateBurst == 0 {
			config.RateBurst = config.RateRPS
		}
		providers[name] = config
	}
	return providers, nil
}

// MissingAllowlist возвращает имена провайдеров с пустым ip_allowlist
// в стабильном порядке: сервис предупреждает о fail-open dev-режиме.
func MissingAllowlist(providers map[string]ProviderConfig) []string {
	names := make([]string, 0, len(providers))
	for name, config := range providers {
		if len(config.Allowlist) == 0 {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// parseAllowlist разбирает записи allowlist в подсети; одиночные адреса
// трактуются как /32 или /128, IPv4-mapped подсети нормализуются к IPv4.
func parseAllowlist(raw []string) ([]netip.Prefix, error) {
	if len(raw) > MaxAllowlistEntries {
		return nil, fmt.Errorf("allowlist entries %d exceed %d", len(raw), MaxAllowlistEntries)
	}
	prefixes := make([]netip.Prefix, 0, len(raw))
	for _, entry := range raw {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, errors.New("empty allowlist entry")
		}
		if prefix, err := netip.ParsePrefix(entry); err == nil {
			normalized, err := normalizePrefix(prefix)
			if err != nil {
				return nil, fmt.Errorf("invalid allowlist entry %q", entry)
			}
			prefixes = append(prefixes, normalized)
			continue
		}
		addr, err := netip.ParseAddr(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid allowlist entry %q", entry)
		}
		prefixes = append(prefixes, netip.PrefixFrom(addr.Unmap(), addr.Unmap().BitLen()))
	}
	return prefixes, nil
}

// normalizePrefix приводит IPv4-mapped подсеть к IPv4-диапазону,
// чтобы запись конфигурации не оставалась мертвой.
func normalizePrefix(prefix netip.Prefix) (netip.Prefix, error) {
	if !prefix.Addr().Is4In6() {
		return prefix.Masked(), nil
	}
	if prefix.Bits() < 96 {
		return netip.Prefix{}, errors.New("mapped prefix wider than /96")
	}
	return netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()-96).Masked(), nil
}

// validateRate проверяет и нормализует поля лимита приема провайдера.
func validateRate(config ProviderConfig, name string) error {
	if config.RateRPS < 0 || config.RateRPS > MaxRateRPS {
		return fmt.Errorf("%w: provider %q rate_rps %d out of range", ErrInvalidConfig, name, config.RateRPS)
	}
	if config.RateBurst < 0 || config.RateBurst > MaxRateBurst {
		return fmt.Errorf("%w: provider %q rate_burst %d out of range", ErrInvalidConfig, name, config.RateBurst)
	}
	if config.RateRPS == 0 && config.RateBurst > 0 {
		return fmt.Errorf("%w: provider %q rate_burst without rate_rps", ErrInvalidConfig, name)
	}
	return nil
}
