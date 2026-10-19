// Package domain contains the model of provider webhook intake settings.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
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

// VerificationConfig describes the CF-4 provider webhook verification strategy.
// When absent, the legacy fields (SignatureHeader, EventIDHeader) select the
// legacy t/v1 scheme so existing providers keep working (17.4).
type VerificationConfig struct {
	// Strategy selects raw_hmac, canonical_json_hmac or token_echo.
	Strategy string `json:"strategy"`
	// Header sets the signature header; empty for token_echo.
	Header string `json:"header"`
	// Algorithm selects sha256 (default) or sha512.
	Algorithm string `json:"algorithm"`
	// Encoding selects hex (default) or base64.
	Encoding string `json:"encoding"`
	// Prefix is an optional digest prefix, for example "sha256=" of BTCPay.
	Prefix string `json:"prefix"`
	// Field sets the token echo body field; empty means "token".
	Field string `json:"field"`
}

// EventKeyConfig describes the vendor event key source for deduplication:
// exactly one of Header and Template is set (17.4).
type EventKeyConfig struct {
	// Header sets the header carrying the vendor event id.
	Header string `json:"header"`
	// Template sets a dotted-path template over the JSON body,
	// for example "{data.id}:{data.status}".
	Template string `json:"template"`
}

// ProviderConfig describes webhook intake settings of a single provider.
type ProviderConfig struct {
	// Name sets the provider name (configuration key).
	Name string `json:"-"`
	// SignatureHeader sets the signature header name (legacy scheme).
	SignatureHeader string `json:"signature_header"`
	// EventIDHeader sets the event identifier header name (legacy scheme).
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
	// Verification sets the CF-4 verification strategy; nil means the legacy scheme.
	Verification *VerificationConfig `json:"verification"`
	// FetchRequired asks the connector for a provider fetch before a canonical
	// event (mandatory for CoinGate and BitPay status models, 17.4).
	FetchRequired bool `json:"fetch_required"`
	// EventKey sets the vendor event key source; nil means the legacy header.
	EventKey *EventKeyConfig `json:"event_key"`
	// GatewayID sets the provider payment identifier source; nil means the
	// legacy top-level scan (17.26).
	GatewayID *GatewayIDConfig `json:"gateway_id"`
	// AllowlistURL sets the dynamic allowlist source (CoinGate ips-v4 list).
	AllowlistURL string `json:"ip_allowlist_url"`
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
		if strings.TrimSpace(config.SecretEnv) == "" {
			return nil, fmt.Errorf("%w: provider %q has an empty secret_env", ErrInvalidConfig, name)
		}
		if config.Verification == nil &&
			(strings.TrimSpace(config.SignatureHeader) == "" || strings.TrimSpace(config.EventIDHeader) == "") {
			return nil, fmt.Errorf("%w: provider %q has empty legacy fields", ErrInvalidConfig, name)
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
		if _, err := config.verifier(); err != nil {
			return nil, err
		}
		if config.Verification != nil &&
			config.Verification.Strategy == StrategyTokenEcho && !config.FetchRequired {
			// The CoinGate status model requires a provider fetch before a
			// canonical event (CBL-003 section 4); a config without it would
			// silently ship raws the connector cannot trust.
			return nil, fmt.Errorf("%w: provider %q: token_echo requires fetch_required", ErrInvalidConfig, name)
		}
		if config.Verification != nil && config.EventKey == nil && strings.TrimSpace(config.EventIDHeader) == "" {
			return nil, fmt.Errorf("%w: provider %q: a verification strategy requires event_key or event_id_header",
				ErrInvalidConfig, name)
		}
		if err := validateEventKey(config); err != nil {
			return nil, err
		}
		if err := validateGatewayID(config); err != nil {
			return nil, err
		}
		if err := validateAllowlistURL(config); err != nil {
			return nil, err
		}
		providers[name] = config
	}
	return providers, nil
}

// validateEventKey checks the vendor event key configuration.
func validateEventKey(config ProviderConfig) error {
	if config.EventKey == nil {
		return nil
	}
	header := strings.TrimSpace(config.EventKey.Header)
	template := strings.TrimSpace(config.EventKey.Template)
	switch {
	case header == "" && template == "":
		return fmt.Errorf("%w: provider %q: event_key is empty", ErrInvalidConfig, config.Name)
	case header != "" && template != "":
		return fmt.Errorf("%w: provider %q: event_key sets both header and template", ErrInvalidConfig, config.Name)
	case template != "":
		for _, alternative := range strings.Split(template, "|") {
			if _, err := compileTemplate(strings.TrimSpace(alternative)); err != nil {
				return fmt.Errorf("%w: provider %q: %v", ErrInvalidConfig, config.Name, err)
			}
		}
	}
	return nil
}

// validateGatewayID checks the provider payment identifier configuration: a
// present block must carry a compilable template, because a broken template
// would silently send every delivery to the connector with "unknown" (17.26).
func validateGatewayID(config ProviderConfig) error {
	if config.GatewayID == nil {
		return nil
	}
	template := strings.TrimSpace(config.GatewayID.Template)
	if template == "" {
		return fmt.Errorf("%w: provider %q: gateway_id is empty", ErrInvalidConfig, config.Name)
	}
	for _, alternative := range strings.Split(template, "|") {
		if _, err := compileTemplate(strings.TrimSpace(alternative)); err != nil {
			return fmt.Errorf("%w: provider %q: gateway_id template is invalid", ErrInvalidConfig, config.Name)
		}
	}
	return nil
}

// validateAllowlistURL checks the dynamic allowlist source URL: HTTPS only
// (T-WH-08), so a poisoned plain-HTTP source cannot widen the allowlist.
func validateAllowlistURL(config ProviderConfig) error {
	raw := strings.TrimSpace(config.AllowlistURL)
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("%w: provider %q: invalid ip_allowlist_url %q", ErrInvalidConfig, config.Name, raw)
	}
	return nil
}

// MissingAllowlist returns the names of providers with no static and no dynamic
// allowlist in a stable order: the service warns about the fail-open dev mode.
func MissingAllowlist(providers map[string]ProviderConfig) []string {
	names := make([]string, 0, len(providers))
	for name, config := range providers {
		if len(config.Allowlist) == 0 && strings.TrimSpace(config.AllowlistURL) == "" {
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
		prefix, err := parseAllowlistEntry(entry)
		if err != nil {
			return nil, err
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

// parseAllowlistEntry parses one allowlist entry: a CIDR prefix or a single address.
func parseAllowlistEntry(entry string) (netip.Prefix, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return netip.Prefix{}, errors.New("empty allowlist entry")
	}
	if prefix, err := netip.ParsePrefix(entry); err == nil {
		normalized, err := normalizePrefix(prefix)
		if err != nil {
			return netip.Prefix{}, fmt.Errorf("invalid allowlist entry %q", entry)
		}
		if err := validateUsablePrefix(normalized); err != nil {
			return netip.Prefix{}, err
		}
		return normalized, nil
	}
	addr, err := netip.ParseAddr(entry)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("invalid allowlist entry %q", entry)
	}
	prefix := netip.PrefixFrom(addr.Unmap(), addr.Unmap().BitLen())
	if err := validateUsablePrefix(prefix); err != nil {
		return netip.Prefix{}, err
	}
	return prefix, nil
}

// validateUsablePrefix rejects entries that can never belong to a provider
// endpoint: loopback, multicast and link-local addresses (a poisoned list must
// not widen itself to internal networks). The unspecified form is rejected for
// dynamic lists separately; static operator configuration keeps its explicit
// 0.0.0.0/0 option.
func validateUsablePrefix(prefix netip.Prefix) error {
	addr := prefix.Addr()
	switch {
	case addr.IsLoopback():
		return fmt.Errorf("loopback allowlist entry %q is forbidden", prefix)
	case addr.IsMulticast(), addr.IsLinkLocalUnicast(), addr.IsLinkLocalMulticast():
		return fmt.Errorf("link-local or multicast allowlist entry %q is forbidden", prefix)
	}
	return nil
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
