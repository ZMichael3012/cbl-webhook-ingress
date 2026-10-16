// Package domain contains the model of provider webhook intake settings.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
)

// ErrInvalidConfig reports an invalid provider configuration.
var ErrInvalidConfig = errors.New("webhook-ingress: invalid provider config")

// Bounds of the provider protection configuration.
const (
	// MaxAllowlistEntries sets the maximum number of allowlist entries.
	MaxAllowlistEntries = 64
	// MaxRateRPS sets the upper bound of the intake rate per second.
	MaxRateRPS = 100_000
	// MaxRateBurst sets the upper bound of the rate limit burst.
	MaxRateBurst = 1_000_000
)

// ProviderConfig describes webhook intake settings of a single provider.
type ProviderConfig struct {
	// Name sets the provider name (configuration key).
	Name string `json:"-"`
	// SignatureHeader sets the signature header name.
	SignatureHeader string `json:"signature_header"`
	// EventIDHeader sets the event identifier header name.
	EventIDHeader string `json:"event_id_header"`
	// SecretEnv sets the name of the environment variable with the signing secret.
	SecretEnv string `json:"secret_env"`
	// Endpoints sets the allowlist of allowed endpoint_id values; an empty value allows any.
	Endpoints []string `json:"endpoints"`
	// IPAllowlist sets the allowed gateway addresses (CIDR or single addresses);
	// an empty value allows any address.
	IPAllowlist []string `json:"ip_allowlist"`
	// RateRPS sets the sustained intake rate in requests per second (0 - no limit).
	RateRPS int `json:"rate_rps"`
	// RateBurst sets the rate limit burst capacity (0 - equal to RateRPS).
	RateBurst int `json:"rate_burst"`
	// Secret sets the signing secret injected from the environment.
	Secret []byte `json:"-"`
	// Allowlist sets the parsed gateway subnets.
	Allowlist []netip.Prefix `json:"-"`
}

// AllowsIP checks a peer address against the provider allowlist;
// an empty allowlist allows any address (dev mode).
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

// ParseProviders parses the JSON provider configuration of the form
// {"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id",
// "secret_env":"MOCK_WEBHOOK_SECRET"}} and injects secrets via lookupEnv.
// The optional fields ip_allowlist, rate_rps and rate_burst describe intake protection.
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

// MissingAllowlist returns the names of providers with an empty ip_allowlist
// in a stable order: the service warns about the fail-open dev mode.
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

// parseAllowlist parses allowlist entries into subnets; single addresses
// are treated as /32 or /128, and IPv4-mapped subnets are normalized to IPv4.
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

// normalizePrefix converts an IPv4-mapped subnet to an IPv4 range
// so that the configuration entry does not stay dead.
func normalizePrefix(prefix netip.Prefix) (netip.Prefix, error) {
	if !prefix.Addr().Is4In6() {
		return prefix.Masked(), nil
	}
	if prefix.Bits() < 96 {
		return netip.Prefix{}, errors.New("mapped prefix wider than /96")
	}
	return netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()-96).Masked(), nil
}

// validateRate validates and normalizes the provider intake rate limit fields.
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
