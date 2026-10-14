// Пакет transport содержит транспортный слой сервиса webhook-ingress:
// HTTP-хендлеры Gin, собранные из usecase.
package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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

// MaxBodyBytes задает предельный размер тела вебхука (1 МБ).
const MaxBodyBytes int64 = 1 << 20

// Handler описывает обработчик вебхука для транспортного слоя.
type Handler interface {
	Handle(ctx context.Context, in usecase.WebhookInput) (bool, error)
}

// Server принимает вебхуки провайдеров.
type Server struct {
	providers    map[string]domain.ProviderConfig
	handler      Handler
	clock        func() time.Time
	window       time.Duration
	maxBody      int64
	writeTimeout time.Duration
	log          *zap.Logger
}

// Option настраивает Server.
type Option func(*Server)

// WithClock задает источник времени для проверки окна replay.
func WithClock(clock func() time.Time) Option {
	return func(s *Server) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithWindow задает окно replay подписи.
func WithWindow(window time.Duration) Option {
	return func(s *Server) {
		if window > 0 {
			s.window = window
		}
	}
}

// WithMaxBody задает предельный размер тела вебхука.
func WithMaxBody(maxBody int64) Option {
	return func(s *Server) {
		if maxBody > 0 {
			s.maxBody = maxBody
		}
	}
}

// WithWriteTimeout задает бюджет записи события в PostgreSQL;
// значение прижимается к допустимому диапазону домена.
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

// NewServer создает Server приема вебхуков.
func NewServer(providers map[string]domain.ProviderConfig, handler Handler, log *zap.Logger, opts ...Option) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	s := &Server{
		providers:    providers,
		handler:      handler,
		clock:        time.Now,
		window:       hmac.DefaultWindow,
		maxBody:      MaxBodyBytes,
		writeTimeout: domain.DefaultWriteTimeout,
		log:          log,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Register регистрирует маршрут приема вебхуков.
func (s *Server) Register(router gin.IRouter) {
	router.POST("/hooks/:provider/:endpoint_id", s.handleWebhook)
}

// handleWebhook принимает вебхук: сырое тело, проверка HMAC, фиксация события.
func (s *Server) handleWebhook(c *gin.Context) {
	provider := c.Param("provider")
	config, ok := s.providers[provider]
	if !ok {
		ginmw.AbortWithProblem(c, http.StatusNotFound, "provider_not_found", "Not Found", "провайдер не настроен")
		return
	}
	endpointID := c.Param("endpoint_id")
	if len(config.Endpoints) > 0 && !slices.Contains(config.Endpoints, endpointID) {
		ginmw.AbortWithProblem(c, http.StatusNotFound, "endpoint_not_found", "Not Found", "эндпоинт не зарегистрирован")
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

	// Подпись проверяется до разбора заголовков события: неаутентифицированный клиент
	// не должен получать различимые ответы о структуре запроса.
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

	// Бюджет записи защищает ответ от деградации PostgreSQL: прерванная
	// транзакция откатывается, провайдер повторит доставку, дублей не будет.
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
		if errors.Is(err, context.DeadlineExceeded) {
			s.log.Warn("webhook write budget exceeded",
				zap.String("provider", provider),
				zap.Duration("budget", s.writeTimeout),
			)
			c.Header("Retry-After", "1")
			ginmw.AbortWithProblem(c, http.StatusServiceUnavailable, "write_timeout", "Service Unavailable", "")
			return
		}
		if errors.Is(err, context.Canceled) {
			// Клиент отменил запрос: ответ не важен, Error здесь создал бы
			// ложный шум в метрике доли ошибок (0.7).
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

// sensitiveHeaders задает заголовки, которые не попадают в событие и хранилища.
var sensitiveHeaders = map[string]struct{}{
	"authorization":       {},
	"cookie":              {},
	"set-cookie":          {},
	"proxy-authorization": {},
	"x-api-key":           {},
}

// flattenHeaders приводит заголовки запроса к карте первых значений,
// исключая потенциально чувствительные заголовки и заголовок подписи:
// имя заголовка подписи настраивается и задается отдельно.
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

// extractGatewayID извлекает gateway_id из тела без строгого разбора JSON.
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
