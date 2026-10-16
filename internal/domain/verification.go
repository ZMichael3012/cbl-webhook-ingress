package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cbl/cbl-kit/hmac"
	"github.com/cbl/cbl-kit/webhooksig"
)

// VerificationState describes the result of a provider webhook verification (CF-4).
type VerificationState string

// Verification states of the CF-4 signature_state field.
const (
	// VerifyVerified states that the signature or the token echo matched.
	VerifyVerified VerificationState = "verified"
	// VerifyHMACFailed states that a present signature did not match.
	VerifyHMACFailed VerificationState = "hmac_failed"
	// VerifyEchoMismatch states that the token echo did not match.
	VerifyEchoMismatch VerificationState = "echo_mismatch"
	// VerifyAbsent states that the delivery carried no verifiable signature.
	VerifyAbsent VerificationState = "absent"
)

// Verification strategies of the provider configuration.
const (
	// StrategyLegacyHMAC selects the platform t/v1 scheme over "<unix>.<body>".
	StrategyLegacyHMAC = "legacy_hmac"
	// StrategyRawHMAC selects HMAC over the raw body.
	StrategyRawHMAC = "raw_hmac"
	// StrategyCanonicalJSONHMAC selects HMAC over the canonical JSON form.
	StrategyCanonicalJSONHMAC = "canonical_json_hmac"
	// StrategyTokenEcho selects the token echo comparison (CoinGate).
	StrategyTokenEcho = "token_echo"
)

// Verifier checks a delivery and reports the CF-4 verification state.
type Verifier interface {
	// Verify returns the state of the delivery; window bounds the replay window
	// of the legacy scheme and is ignored by provider strategies.
	Verify(secret []byte, req webhooksig.Request, now time.Time, window time.Duration) VerificationState
}

// StrategyName returns the effective verification strategy of the configuration.
func (c ProviderConfig) StrategyName() string {
	if c.Verification != nil {
		return c.Verification.Strategy
	}
	return StrategyLegacyHMAC
}

// SignatureHeaderName returns the header carrying the signature; the token echo
// strategy normally has none, but a configured header is still excluded from
// the stored headers.
func (c ProviderConfig) SignatureHeaderName() string {
	if c.Verification == nil {
		return c.SignatureHeader
	}
	return c.Verification.Header
}

// ResponsePolicy returns the HTTP status and whether the delivery is recorded.
// HMAC mismatches are answered 401 and dropped (the provider retries, storage
// cannot be flooded); token echo (CoinGate) never answers 4xx or redirects
// because that disables callbacks: unverified deliveries are recorded with a
// non-verified state for observability instead (CF-4 hardening, 17.4).
func (c ProviderConfig) ResponsePolicy(state VerificationState) (int, bool) {
	if c.StrategyName() == StrategyTokenEcho {
		return http.StatusOK, true
	}
	if state == VerifyVerified {
		return http.StatusOK, true
	}
	return http.StatusUnauthorized, false
}

// VerifyDelivery verifies a delivery against the provider secret.
func (c ProviderConfig) VerifyDelivery(header http.Header, body []byte, now time.Time, window time.Duration) (VerificationState, error) {
	verifier, err := c.verifier()
	if err != nil {
		return VerifyAbsent, err
	}
	return verifier.Verify(c.Secret, webhooksig.Request{Header: header, Body: body}, now, window), nil
}

// verifier builds the verification strategy from the configuration fields.
// The construction is per call: the strategies are tiny values and the parsed
// configuration is immutable.
func (c ProviderConfig) verifier() (Verifier, error) {
	if c.Verification == nil {
		if strings.TrimSpace(c.SignatureHeader) == "" {
			return nil, fmt.Errorf("%w: provider %q has no verification strategy", ErrInvalidConfig, c.Name)
		}
		return legacyVerifier{header: c.SignatureHeader}, nil
	}
	switch c.Verification.Strategy {
	case StrategyLegacyHMAC:
		header := strings.TrimSpace(c.SignatureHeader)
		if header == "" {
			return nil, fmt.Errorf("%w: provider %q: legacy_hmac requires signature_header", ErrInvalidConfig, c.Name)
		}
		return legacyVerifier{header: header}, nil
	case StrategyRawHMAC:
		header := strings.TrimSpace(c.Verification.Header)
		if header == "" {
			return nil, fmt.Errorf("%w: provider %q: raw_hmac requires header", ErrInvalidConfig, c.Name)
		}
		algorithm, err := parseAlgorithm(c.Verification.Algorithm)
		if err != nil {
			return nil, fmt.Errorf("%w: provider %q: %v", ErrInvalidConfig, c.Name, err)
		}
		encoding, err := parseEncoding(c.Verification.Encoding)
		if err != nil {
			return nil, fmt.Errorf("%w: provider %q: %v", ErrInvalidConfig, c.Name, err)
		}
		return webhooksigVerifier{strategy: webhooksig.RawHMAC{
			Header:    header,
			Algorithm: algorithm,
			Encoding:  encoding,
			Prefix:    c.Verification.Prefix,
		}}, nil
	case StrategyCanonicalJSONHMAC:
		header := strings.TrimSpace(c.Verification.Header)
		if header == "" {
			return nil, fmt.Errorf("%w: provider %q: canonical_json_hmac requires header", ErrInvalidConfig, c.Name)
		}
		algorithm, err := parseAlgorithm(c.Verification.Algorithm)
		if err != nil {
			return nil, fmt.Errorf("%w: provider %q: %v", ErrInvalidConfig, c.Name, err)
		}
		encoding, err := parseEncoding(c.Verification.Encoding)
		if err != nil {
			return nil, fmt.Errorf("%w: provider %q: %v", ErrInvalidConfig, c.Name, err)
		}
		return webhooksigVerifier{strategy: webhooksig.CanonicalJSONHMAC{
			Header:    header,
			Algorithm: algorithm,
			Encoding:  encoding,
		}}, nil
	case StrategyTokenEcho:
		return webhooksigVerifier{strategy: webhooksig.TokenEcho{Field: strings.TrimSpace(c.Verification.Field)}}, nil
	default:
		return nil, fmt.Errorf("%w: provider %q: unknown strategy %q", ErrInvalidConfig, c.Name, c.Verification.Strategy)
	}
}

// legacyVerifier verifies the platform t/v1 scheme of the legacy path.
type legacyVerifier struct {
	header string
}

// Verify implements Verifier for the legacy scheme.
func (v legacyVerifier) Verify(secret []byte, req webhooksig.Request, now time.Time, window time.Duration) VerificationState {
	value := req.Header.Get(v.header)
	if strings.TrimSpace(value) == "" {
		return VerifyAbsent
	}
	if err := hmac.Verify(secret, value, req.Body, now, window); err != nil {
		return VerifyHMACFailed
	}
	return VerifyVerified
}

// webhooksigVerifier adapts a kit verification strategy to the Verifier port.
type webhooksigVerifier struct {
	strategy webhooksig.Strategy
}

// Verify implements Verifier: kit error sentinels map to the CF-4 states.
func (v webhooksigVerifier) Verify(secret []byte, req webhooksig.Request, _ time.Time, _ time.Duration) VerificationState {
	err := v.strategy.Verify(secret, req)
	switch {
	case err == nil:
		return VerifyVerified
	case errors.Is(err, webhooksig.ErrSignatureAbsent):
		return VerifyAbsent
	case errors.Is(err, webhooksig.ErrEchoMismatch):
		return VerifyEchoMismatch
	default:
		// ErrSignatureMismatch and ErrMalformed both mean a failed check.
		return VerifyHMACFailed
	}
}

// parseAlgorithm maps the configuration algorithm to the kit value.
func parseAlgorithm(value string) (webhooksig.Algorithm, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "sha256":
		return webhooksig.SHA256, nil
	case "sha512":
		return webhooksig.SHA512, nil
	default:
		return 0, fmt.Errorf("unknown algorithm %q", value)
	}
}

// parseEncoding maps the configuration encoding to the kit value.
func parseEncoding(value string) (webhooksig.Encoding, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "hex":
		return webhooksig.Hex, nil
	case "base64":
		return webhooksig.Base64, nil
	default:
		return 0, fmt.Errorf("unknown encoding %q", value)
	}
}

// RedactToken replaces the token echo field value with an empty string before
// the raw event is recorded or published: the verification secret must never
// land in the outbox, Kafka or logs. ok=false means the body could not be
// rewritten safely (malformed, non-object or having trailing data) and must not
// be stored as is. Bodies of other strategies pass unchanged because they are
// signed over their exact bytes.
func (c ProviderConfig) RedactToken(body []byte) ([]byte, bool) {
	if c.Verification == nil || c.Verification.Strategy != StrategyTokenEcho {
		return body, true
	}
	field := strings.TrimSpace(c.Verification.Field)
	if field == "" {
		field = "token"
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var parsed map[string]json.RawMessage
	if err := decoder.Decode(&parsed); err != nil || parsed == nil {
		return nil, false
	}
	// The verification decoder was strict; the redaction must not accept a body
	// with trailing data that could smuggle a second token copy.
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, false
	}
	parsed[field] = json.RawMessage(`""`)
	redacted, err := json.Marshal(parsed)
	if err != nil {
		return nil, false
	}
	return redacted, true
}
