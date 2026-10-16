// Package transport contains the transport layer of the webhook-ingress service:
// Gin HTTP handlers assembled from usecase.
package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/cbl/cbl-kit/ginmw"
	"github.com/cbl/cbl-kit/hmac"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
	"github.com/cbl/cbl-webhook-ingress/internal/usecase"
)

// MaxBodyBytes sets the webhook body size limit (1 MB).
const MaxBodyBytes int64 = 1 << 20

// Handler describes the webhook handler for the transport layer.
type Handler interface {
	Handle(ctx context.Context, in usecase.WebhookInput) (bool, error)
}

// Server accepts provider webhooks.
type Server struct {
	providers    map[string]domain.ProviderConfig
	limiters     map[string]*domain.Limiter
	handler      Handler
	clock        func() time.Time
	window       time.Duration
	maxBody      int64
	writeTimeout time.Duration
	log          *zap.Logger
}

// Option configures a Server.
type Option func(*Server)

// WithClock sets the time source for the replay window check.
func WithClock(clock func() time.Time) Option {
	return func(s *Server) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithWindow sets the signature replay window.
func WithWindow(window time.Duration) Option {
	return func(s *Server) {
		if window > 0 {
			s.window = window
		}
	}
}

// WithMaxBody sets the webhook body size limit.
func WithMaxBody(maxBody int64) Option {
	return func(s *Server) {
		if maxBody > 0 {
			s.maxBody = maxBody
		}
	}
}

// WithWriteTimeout sets the event write budget to PostgreSQL;
// the value is clamped to the domain's allowed range.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *Server) {
		if timeout <= 0 {
			return
		}
		if timeout < domain.MinWriteTimeout {
			timeout = domain.MinWriteTimeout
		}
		if timeout > domain.MaxWriteTimeout {
			timeout = domain.MaxWriteTimeout
		}
		s.writeTimeout = timeout
	}
}

// NewServer creates a webhook intake Server.
func NewServer(providers map[string]domain.ProviderConfig, handler Handler, log *zap.Logger, opts ...Option) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	s := &Server{
		providers:    providers,
		limiters:     make(map[string]*domain.Limiter, len(providers)),
		handler:      handler,
		clock:        time.Now,
		window:       hmac.DefaultWindow,
		maxBody:      MaxBodyBytes,
		writeTimeout: domain.DefaultWriteTimeout,
		log:          log,
	}
	for name, config := range providers {
		s.limiters[name] = domain.NewLimiter(config.RateRPS, config.RateBurst)
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Register registers the webhook intake route.
func (s *Server) Register(router gin.IRouter) {
	router.POST("/hooks/:provider/:endpoint_id", s.handleWebhook)
}

// handleWebhook accepts a webhook: raw body, HMAC verification, event recording.
func (s *Server) handleWebhook(c *gin.Context) {
	provider := c.Param("provider")
	config, ok := s.providers[provider]
	if !ok {
		ginmw.AbortWithProblem(c, http.StatusNotFound, "provider_not_found", "Not Found", "провайдер не настроен")
		return
	}
	// Network checks are cheaper than reading the body and do not reveal
	// the existence of endpoint_id: a foreign address is rejected before all other checks.
	if len(config.Allowlist) > 0 {
		addr, ok := remoteIP(c.Request.RemoteAddr)
		if !ok || !config.AllowsIP(addr) {
			s.log.Warn("webhook source rejected",
				zap.String("provider", provider),
				zap.String("remote_addr", c.Request.RemoteAddr),
			)
			ginmw.AbortWithProblem(c, http.StatusForbidden, "ip_forbidden", "Forbidden", "")
			return
		}
	}
	endpointID := c.Param("endpoint_id")
	if len(config.Endpoints) > 0 && !slices.Contains(config.Endpoints, endpointID) {
		ginmw.AbortWithProblem(c, http.StatusNotFound, "endpoint_not_found", "Not Found", "эндпоинт не зарегистрирован")
		return
	}
	if limiter := s.limiters[provider]; limiter != nil && !limiter.Allow(s.clock()) {
		s.log.Warn("webhook rate limited", zap.String("provider", provider))
		c.Header("Retry-After", "1")
		ginmw.AbortWithProblem(c, http.StatusTooManyRequests, "rate_limited", "Too Many Requests", "")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, s.maxBody))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			ginmw.AbortWithProblem(c, http.StatusRequestEntityTooLarge, "body_too_large", "Payload Too Large", "")
			return
		}
		ginmw.AbortWithProblem(c, http.StatusBadRequest, "body_read_failed", "Bad Request", "")
		return
	}

	// The signature is verified before event headers are parsed: an unauthenticated
	// client must not receive distinguishable responses about the request structure.
	signature := c.GetHeader(config.SignatureHeader)
	if signature == "" {
		ginmw.AbortWithProblem(c, http.StatusUnauthorized, "invalid_signature", "Unauthorized", "")
		return
	}
	if err := hmac.Verify(config.Secret, signature, body, s.clock(), s.window); err != nil {
		s.log.Warn("webhook signature rejected", zap.String("provider", provider), zap.Error(err))
		ginmw.AbortWithProblem(c, http.StatusUnauthorized, "invalid_signature", "Unauthorized", "")
		return
	}

	vendorEventID := strings.TrimSpace(c.GetHeader(config.EventIDHeader))
	if vendorEventID == "" {
		ginmw.AbortWithProblem(c, http.StatusUnprocessableEntity, "missing_event_id", "Unprocessable Entity",
			fmt.Sprintf("заголовок %s обязателен", config.EventIDHeader))
		return
	}

	// The write budget protects the response from PostgreSQL degradation: the aborted
	// transaction is rolled back, the provider retries the delivery, and there will be no duplicates.
	writeCtx, cancel := context.WithTimeout(c.Request.Context(), s.writeTimeout)
	defer cancel()
	processed, err := s.handler.Handle(writeCtx, usecase.WebhookInput{
		Provider:      provider,
		EndpointID:    endpointID,
		VendorEventID: vendorEventID,
		Headers:       flattenHeaders(c.Request.Header, config.SignatureHeader),
		RawBody:       body,
		GatewayID:     extractGatewayID(body),
	})
	if err != nil {
		// Budget exhaustion arrives in two forms: waiting for a lock and
		// executing the query yield context.DeadlineExceeded, while the deadline
		// on connection establishment on Linux terminates dial through
		// os.ErrDeadlineExceeded ("i/o timeout", verified by profile 5.4:
		// a race with ctx.Err() does not allow distinguishing it by context state).
		// In the write path the only external I/O is PostgreSQL, so an i/o
		// timeout here means budget exhaustion; if other network calls appear
		// inside Handler, the classification will need to be refined.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
			s.log.Warn("webhook write budget exceeded",
				zap.String("provider", provider),
				zap.Duration("budget", s.writeTimeout),
				zap.Error(err),
			)
			c.Header("Retry-After", "1")
			ginmw.AbortWithProblem(c, http.StatusServiceUnavailable, "write_timeout", "Service Unavailable", "")
			return
		}
		if errors.Is(err, context.Canceled) {
			// The client canceled the request: the response does not matter, and Error
			// here would create false noise in the error rate metric (0.7).
			s.log.Info("webhook handling canceled by client", zap.String("provider", provider))
			ginmw.AbortWithProblem(c, http.StatusRequestTimeout, "request_canceled", "Request Timeout", "")
			return
		}
		s.log.Error("webhook handling failed", zap.String("provider", provider), zap.Error(err))
		ginmw.AbortWithProblem(c, http.StatusInternalServerError, "internal_error", "Internal Server Error", "")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "accepted", "duplicate": !processed})
}

// remoteIP extracts the TCP peer address of the request; proxy headers are not read
// so that a forged X-Forwarded-For cannot bypass the allowlist.
func remoteIP(remoteAddr string) (netip.Addr, bool) {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err != nil {
		host = strings.TrimSpace(remoteAddr)
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

// sensitiveHeaders sets headers that never reach the event and storages.
var sensitiveHeaders = map[string]struct{}{
	"authorization":       {},
	"cookie":              {},
	"set-cookie":          {},
	"proxy-authorization": {},
	"x-api-key":           {},
}

// flattenHeaders converts request headers to a map of first values,
// excluding potentially sensitive headers and the signature header:
// the signature header name is configurable and is set separately.
func flattenHeaders(header http.Header, signatureHeader string) map[string]string {
	headers := make(map[string]string, len(header))
	for key, values := range header {
		if len(values) == 0 {
			continue
		}
		if _, sensitive := sensitiveHeaders[strings.ToLower(key)]; sensitive {
			continue
		}
		if strings.EqualFold(key, signatureHeader) {
			continue
		}
		headers[key] = values[0]
	}
	return headers
}

// extractGatewayID extracts gateway_id from the body without strict JSON parsing.
func extractGatewayID(body []byte) string {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(body, &parsed); err != nil {
		return ""
	}
	for _, key := range []string{"gateway_id", "gatewayId", "id"} {
		raw, ok := parsed[key]
		if !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			return value
		}
	}
	return ""
}
