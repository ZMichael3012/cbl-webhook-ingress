// Пакет usecase содержит сценарий приема вебхука: идемпотентная фиксация
// события в inbox и outbox одной транзакцией без ожидания Kafka.
package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"
)

// TopicGatewayWebhooksRaw задает топик сырых вебхуков провайдера.
const TopicGatewayWebhooksRaw = "gateway.webhooks.raw"

// WebhookInput описывает принятый вебхук провайдера.
type WebhookInput struct {
	// Provider задает имя провайдера.
	Provider string
	// EndpointID задает идентификатор эндпоинта из пути.
	EndpointID string
	// VendorEventID задает идентификатор события провайдера из заголовка.
	VendorEventID string
	// Headers задает заголовки запроса.
	Headers map[string]string
	// RawBody задает сырое тело запроса.
	RawBody []byte
	// GatewayID задает идентификатор платежа у провайдера, если извлечен из тела.
	GatewayID string
}

// Handler фиксирует вебхуки в inbox и outbox.
type Handler struct {
	db       *gorm.DB
	guard    *inbox.Guard
	store    *outbox.Store
	producer string
	clock    func() time.Time
	log      *zap.Logger
}

// NewHandler создает Handler сервиса webhook-ingress.
func NewHandler(db *gorm.DB, producer string, log *zap.Logger) *Handler {
	if log == nil {
		log = zap.NewNop()
	}
	return &Handler{
		db:       db,
		guard:    inbox.NewGuard(),
		store:    outbox.NewStore(),
		producer: producer,
		clock:    time.Now,
		log:      log,
	}
}

// Handle сохраняет событие; повторная доставка того же события не создает дубль.
// Возвращает processed=false, если событие уже было принято ранее.
func (h *Handler) Handle(ctx context.Context, in WebhookInput) (bool, error) {
	eventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("cbl-webhook:"+in.Provider+":"+in.VendorEventID)).String()
	gatewayID := strings.TrimSpace(in.GatewayID)
	// Ключ партиции сохраняет порядок по сущности: при отсутствии gateway_id
	// используется endpoint_id, а поле конверта остается unknown (CBL-5.1 FR-4).
	partitionKey := in.Provider + ":" + gatewayID
	if gatewayID == "" {
		gatewayID = "unknown"
		partitionKey = in.Provider + ":" + in.EndpointID
	}
	raw := &eventsv1.GatewayWebhookRaw{
		Provider:          in.Provider,
		GatewayId:         gatewayID,
		ReceivedAt:        timestamppb.New(h.clock().UTC()),
		Headers:           in.Headers,
		RawBody:           in.RawBody,
		SignatureVerified: true,
	}
	payload, err := proto.Marshal(raw)
	if err != nil {
		return false, fmt.Errorf("usecase: marshal webhook: %w", err)
	}
	headers := map[string]string{
		outbox.HeaderEventID:       eventID,
		outbox.HeaderProducer:      h.producer,
		outbox.HeaderSchemaVersion: "v1",
		"provider":                 in.Provider,
		"vendor_event_id":          in.VendorEventID,
		"endpoint_id":              in.EndpointID,
	}
	envelope, err := outbox.NewEnvelope("gateway.webhook.raw", TopicGatewayWebhooksRaw,
		partitionKey, headers, payload)
	if err != nil {
		return false, fmt.Errorf("usecase: build envelope: %w", err)
	}
	processed, err := h.guard.Process(ctx, h.db, in.Provider, eventID, func(tx *gorm.DB) error {
		return h.store.Append(ctx, tx, envelope)
	})
	if err != nil {
		return false, fmt.Errorf("usecase: save webhook: %w", err)
	}
	if !processed {
		h.log.Info("duplicate webhook skipped",
			zap.String("provider", in.Provider),
			zap.String("vendor_event_id", in.VendorEventID),
		)
	}
	return processed, nil
}
