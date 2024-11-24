package opentelemetry

import (
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestTrace(t *testing.T) {
	middleware := Trace(WithSpanAttributes(
		attribute.Bool("test", true),
	))

	var (
		uuid    = "52219531-0cd8-4b64-be31-ba6b4ef01472"
		payload = message.Payload("test payload for Trace")
		msg     = message.NewMessage(uuid, payload)
	)

	h := func(m *message.Message) ([]*message.Message, error) {
		if got, want := m.UUID, uuid; got != want {
			t.Fatalf("m.UUID = %q, want %q", got, want)
		}

		return nil, nil
	}

	if _, err := middleware(h)(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTraceWithTextMapPropagator(t *testing.T) {
	middleware := Trace(WithTextMapPropagator())

	var (
		traceParentKey = "traceparent"
		traceID, _     = trace.TraceIDFromHex("093615e8ce177910353c5a09782ba62a")
		spanID, _      = trace.SpanIDFromHex("98c5fa0e132dd10d")
		sc             = trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
			Remote:  true,
		})
		traceParentID = fmt.Sprintf("00-%s-%s-00", traceID.String(), spanID.String())
		uuid          = "52219531-0cd8-4b64-be31-ba6b4ef01472"
		payload       = message.Payload("test payload for Trace")
		msg           = message.NewMessage(uuid, payload)
	)

	msg.Metadata.Set(traceParentKey, traceParentID)

	h := func(m *message.Message) ([]*message.Message, error) {
		if got, want := m.UUID, uuid; got != want {
			t.Fatalf("m.UUID = %q, want %q", got, want)
		}

		msgContext := m.Context()

		if !sc.Equal(trace.SpanContextFromContext(msgContext)) {
			scJSON, _ := sc.MarshalJSON()
			extractedScJSON, _ := trace.SpanContextFromContext(msgContext).MarshalJSON()
			t.Fatalf("span context = %v, want %v", string(extractedScJSON), string(scJSON))
		}

		return nil, nil
	}

	if _, err := middleware(h)(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTraceNoPublishHandler(t *testing.T) {
	var (
		uuid    = "88b433e5-12fa-4eb7-9229-6bfd67de5c4f"
		payload = message.Payload("test payload for TraceNoPublishHandler")
		msg     = message.NewMessage(uuid, payload)
	)

	h := func(m *message.Message) error {
		if got, want := m.UUID, uuid; got != want {
			t.Fatalf("m.UUID = %q, want %q", got, want)
		}

		return nil
	}

	handlerFunc := TraceNoPublishHandler(h, WithSpanAttributes(
		attribute.Bool("test", true),
	))

	if err := handlerFunc(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
