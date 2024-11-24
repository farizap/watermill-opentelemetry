package opentelemetry

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestNewPublisherDecorator(t *testing.T) {
	var (
		uuid    = "0d5427ea-7ab4-4ef1-b80d-0a22bd54a98f"
		payload = message.Payload("test payload")

		traceParentKey = "traceparent"
		traceID, _     = trace.TraceIDFromHex("093615e8ce177910353c5a09782ba62a")
		spanID, _      = trace.SpanIDFromHex("98c5fa0e132dd10d")
		traceParentID  = fmt.Sprintf("00-%s-%s-00", traceID.String(), spanID.String())
		sc             = trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
			Remote:  true,
		})

		pub = &mockMessagePublisher{
			PublishFunc: func(topic string, messages ...*message.Message) error {
				if got, want := topic, "test.topic"; got != want {
					t.Fatalf("topic = %q, want %q", got, want)
				}

				if got, want := len(messages), 1; got != want {
					t.Fatalf("len(messages) = %d, want %d", got, want)
				}

				message := messages[0]

				if got, want := message.UUID, uuid; got != want {
					t.Fatalf("message.UUID = %q, want %q", got, want)
				}

				if got, want := message.Metadata.Get(traceParentKey), traceParentID; got != want {
					t.Fatalf("message.Metadata.Get(%q) = %q, want %q", traceParentKey, got, want)
				}

				if !bytes.Equal(payload, message.Payload) {
					t.Fatalf("unexpected payload")
				}

				return nil
			},
		}

		dec = NewPublisherDecorator(pub,
			WithSpanAttributes(
				attribute.Bool("test", true),
			),
			WithTextMapPropagator(),
		)
	)

	pd, ok := dec.(*PublisherDecorator)
	if !ok {
		t.Fatalf("expected message.Publisher to be *PublisherDecorator")
	}

	if got, want := len(pd.config.spanAttributes), 1; got != want {
		t.Fatalf("len(pd.config.spanAttributes) = %d, want %d", got, want)
	}

	msg := message.NewMessage(uuid, payload)

	msg.SetContext(trace.ContextWithSpanContext(msg.Context(), sc))

	if err := dec.Publish("test.topic", msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

var _ message.Publisher = &mockMessagePublisher{}

type mockMessagePublisher struct {
	CloseFunc   func() error
	PublishFunc func(topic string, messages ...*message.Message) error
}

func (mock *mockMessagePublisher) Close() error {
	if mock.CloseFunc == nil {
		panic("MessagePublisher.CloseFunc: method is nil but Publisher.Close was just called")
	}

	return mock.CloseFunc()
}

func (mock *mockMessagePublisher) Publish(topic string, messages ...*message.Message) error {
	if mock.PublishFunc == nil {
		panic("MessagePublisher.PublishFunc: method is nil but Publisher.Publish was just called")
	}

	return mock.PublishFunc(topic, messages...)
}
