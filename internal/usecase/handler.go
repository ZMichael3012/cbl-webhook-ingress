// Package usecase contains the webhook intake use case: idempotent recording
// of an event in inbox and outbox within one transaction without waiting for Kafka.
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

// TopicGatewayWebhooksRaw sets the topic of raw provider webhooks.
const TopicGatewayWebhooksRaw = "gateway.webhooks.raw"

// WebhookInput describes an accepted provider webhook.
type WebhookInput struct {
	// Provider sets the provider name.
	Provider string
	// EndpointID sets the endpoint identifier from the path.
	EndpointID string
	// VendorEventID sets the provider event identifier from the header.
	VendorEventID string
	// Headers sets the request headers.
	Headers map[string]string
	// RawBody sets the raw request body.
	RawBody []byte
	// GatewayID sets the provider payment identifier if it was extracted from the body.
	GatewayID string
}

// Handler records webhooks in inbox and outbox.
type Handler struct {
	db       *gorm.DB
	guard    *inbox.Guard
	store    *outbox.Store
	producer string
	clock    func() time.Time
	log      *zap.Logger
}

// NewHandler creates a webhook-ingress service Handler.
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

// Handle stores the event; redelivery of the same event does not create a duplicate.
// It returns processed=false if the event was already accepted earlier.
func (h *Handler) Handle(ctx context.Context, in WebhookInput) (bool, error) {
	eventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("cbl-webhook:"+in.Provider+":"+in.VendorEventID)).String()
	gatewayID := strings.TrimSpace(in.GatewayID)
	// The partition key preserves per-entity ordering: when gateway_id is absent,
	// endpoint_id is used and the envelope field stays unknown (CBL-5.1 FR-4).
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
