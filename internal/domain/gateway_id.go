package domain

import (
	"encoding/json"
	"errors"
	"strings"
)

// GatewayIDConfig describes the provider payment identifier source: a dotted
// path template over the JSON body for providers whose callbacks do not carry
// a top-level id (17.26).
type GatewayIDConfig struct {
	// Template sets a dotted-path template over the JSON body,
	// for example "{data.id}" or "{payment_id}".
	Template string `json:"template"`
}

// ExtractGatewayID resolves the provider payment identifier of a delivery: the
// configured gateway_id template wins; without one the legacy top-level keys
// (gateway_id, gatewayId, id) are scanned, so existing providers keep working
// (17.26). Alternative templates separated by "|" are tried in order, the same
// grammar as the vendor event key. An unresolvable template yields an empty
// id: the delivery travels with "unknown" and the connector fails closed
// instead of guessing.
func (c ProviderConfig) ExtractGatewayID(body []byte) string {
	if c.GatewayID != nil {
		if template := strings.TrimSpace(c.GatewayID.Template); template != "" {
			if value, err := resolveTemplateValue(template, body); err == nil {
				return value
			}
			return ""
		}
	}
	return legacyGatewayID(body)
}

// resolveTemplateValue renders a dotted-path template against the JSON body;
// alternative templates separated by "|" are tried in order.
func resolveTemplateValue(template string, body []byte) (string, error) {
	parsed, err := decodeObject(body)
	if err != nil {
		return "", err
	}
	var lastErr error
	for _, alternative := range strings.Split(template, "|") {
		parts, err := compileTemplate(strings.TrimSpace(alternative))
		if err != nil {
			lastErr = err
			continue
		}
		value, err := renderKey(parts, parsed)
		if err != nil {
			lastErr = err
			continue
		}
		return value, nil
	}
	if lastErr == nil {
		lastErr = errors.New("template matched nothing")
	}
	return "", lastErr
}

// legacyGatewayID extracts the id from the top-level legacy keys with a strict
// top-level JSON parse; a JSON number renders exactly via json.Number, because
// the real CoinGate callback carries a numeric id (17.26).
func legacyGatewayID(body []byte) string {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(body, &parsed); err != nil {
		return ""
	}
	for _, key := range []string{"gateway_id", "gatewayId", "id"} {
		raw, ok := parsed[key]
		if !ok {
			continue
		}
		var text string
		if err := json.Unmarshal(raw, &text); err == nil {
			return text
		}
		var number json.Number
		if err := json.Unmarshal(raw, &number); err == nil {
			return number.String()
		}
	}
	return ""
}
