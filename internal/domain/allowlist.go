package domain

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Dynamic allowlist refresh bounds.
const (
	// DefaultAllowlistRefresh sets the default provider list refresh interval.
	DefaultAllowlistRefresh = 10 * time.Minute
	// MinAllowlistRefresh sets the minimum refresh interval.
	MinAllowlistRefresh = 30 * time.Second
	// MaxAllowlistRefresh sets the maximum refresh interval.
	MaxAllowlistRefresh = 24 * time.Hour
	// MaxDynamicAllowlistEntries bounds a single provider dynamic list.
	MaxDynamicAllowlistEntries = 256
)

// DynamicAllowlist holds provider subnets refreshed from provider URLs; writes
// replace a whole provider set, reads are safe for concurrent use.
type DynamicAllowlist struct {
	mu         sync.RWMutex
	byProvider map[string][]netip.Prefix
}

// NewDynamicAllowlist creates an empty dynamic allowlist.
func NewDynamicAllowlist() *DynamicAllowlist {
	return &DynamicAllowlist{byProvider: make(map[string][]netip.Prefix)}
}

// Replace atomically replaces the subnet set of a provider.
func (d *DynamicAllowlist) Replace(provider string, prefixes []netip.Prefix) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(prefixes) == 0 {
		delete(d.byProvider, provider)
		return
	}
	d.byProvider[provider] = prefixes
}

// Allows checks a peer address against the dynamic set of a provider.
func (d *DynamicAllowlist) Allows(provider string, addr netip.Addr) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	addr = addr.Unmap()
	for _, prefix := range d.byProvider[provider] {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// Size reports the number of loaded subnets of a provider.
func (d *DynamicAllowlist) Size(provider string) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.byProvider[provider])
}

// ParseAllowlistText parses a provider IP list: newline, comma or space
// separated addresses and CIDR subnets.
func ParseAllowlistText(text string) ([]netip.Prefix, error) {
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ',' || r == ' ' || r == '\t'
	})
	if len(fields) == 0 {
		return nil, errors.New("empty allowlist text")
	}
	if len(fields) > MaxDynamicAllowlistEntries {
		return nil, fmt.Errorf("allowlist entries %d exceed %d", len(fields), MaxDynamicAllowlistEntries)
	}
	prefixes := make([]netip.Prefix, 0, len(fields))
	for _, entry := range fields {
		prefix, err := parseAllowlistEntry(entry)
		if err != nil {
			return nil, err
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

// ParseAllowlistRefresh parses the refresh interval value: an empty value means
// the default, zero disables the refresher, other values are clamped to the
// allowed range.
func ParseAllowlistRefresh(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return DefaultAllowlistRefresh, nil
	}
	interval, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("%w: allowlist refresh %q", ErrInvalidConfig, value)
	}
	if interval == 0 {
		return 0, nil
	}
	if interval < MinAllowlistRefresh {
		interval = MinAllowlistRefresh
	}
	if interval > MaxAllowlistRefresh {
		interval = MaxAllowlistRefresh
	}
	return interval, nil
}
