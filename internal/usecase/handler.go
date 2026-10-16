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

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

// TopicGatewayWebhooksRaw sets the topic of raw provider webhooks.
const TopicGatewayWebhooksRaw = "gateway.webhooks.raw"

// WebhookInput describes an accepted provider webhook.
type WebhookInput struct {
	// Provider sets the provider name.
	Provider string
	// EndpointID sets the endpoint identifier from the path.
	EndpointID string
	// VendorEventID sets the provider event identifier; empty for unverified
	// deliveries without a resolvable key (they get a random event id).
	VendorEventID string
	// Headers sets the request headers.
	Headers map[string]string
	// RawBody sets the raw request body.
	RawBody []byte
	// GatewayID sets the provider payment identifier if it was extracted from the body.
	GatewayID string
	// SignatureState sets the CF-4 verification result of the delivery.
	SignatureState domain.VerificationState
	// FetchRequired asks the connector for a provider fetch before a canonical event.
	FetchRequired bool
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
	eventID := h.eventID(in)
	gatewayID := strings.TrimSpace(in.GatewayID)
	// The partition key preserves per-entity ordering: when gateway_id is absent,
	// endpoint_id is used and the envelope field stays unknown (CBL-5.1 FR-4).
	partitionKey := in.Provider + ":" + gatewayID
	if gatewayID == "" {
		gatewayID = "unknown"
		partitionKey = in.Provider + ":" + in.EndpointID
	}
	verified := in.SignatureState == domain.VerifyVerified
	raw := &eventsv1.GatewayWebhookRaw{
		Provider:   in.Provider,
		GatewayId:  gatewayID,
		ReceivedAt: timestamppb.New(h.clock().UTC()),
		Headers:    in.Headers,
		RawBody:    in.RawBody,
		// SignatureVerified is the deprecated CF-4 mirror; signature_state is
		// authoritative, and non-verified deliveries never yield canonical events.
		SignatureVerified: verified, //nolint:staticcheck // SA1019: the mirror stays populated until the 17.4/17.2 cutover
		SignatureState:    signatureStateProto(in.SignatureState),
		FetchRequired:     in.FetchRequired,
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

// eventID builds the envelope event id: verified deliveries use the
// deterministic UUIDv5(provider + vendor key), so provider redeliveries dedupe
// (CBL-5.1 FR-4a); unverified recorded deliveries get a random UUIDv7, so they
// cannot poison the dedup slot of a genuine delivery (CF-4 hardening).
func (h *Handler) eventID(in WebhookInput) string {
	if in.SignatureState == domain.VerifyVerified && strings.TrimSpace(in.VendorEventID) != "" {
		return uuid.NewSHA1(uuid.NameSpaceURL, []byte("cbl-webhook:"+in.Provider+":"+in.VendorEventID)).String()
	}
	random, err := uuid.NewV7()
	if err != nil {
		// NewV7 fails only when the system entropy source fails: fall back to a
		// fully random id so the record is never dropped.
		return uuid.NewString()
	}
	return random.String()
}

// signatureStateProto maps the domain verification state to the CF-4 enum.
func signatureStateProto(state domain.VerificationState) eventsv1.WebhookSignatureState {
	switch state {
	case domain.VerifyVerified:
		return eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_VERIFIED
	case domain.VerifyEchoMismatch:
		return eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_ECHO_MISMATCH
	case domain.VerifyAbsent:
		return eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_ABSENT
	default:
		return eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_HMAC_FAILED
	}
}
