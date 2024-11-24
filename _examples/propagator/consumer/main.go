package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	wotel "github.com/voi-oss/watermill-opentelemetry/pkg/opentelemetry"
)

var (
	natsURL = "nats://nats:4222"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := watermill.NewStdLogger(false, false)
	logger.Info("Starting the consumer", nil)

	topic := "example_topic"

	tracerProvider, err := newTraceProvider(ctx)
	if err != nil {
		panic(err)
	}
	otel.SetTracerProvider(tracerProvider)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		panic(err)
	}

	// js, err := natsJS.New(nc)
	// if err != nil {
	// 	panic(err)
	// }
	// if err != nil {
	// 	panic(err)
	// }

	pub, err := jetstream.NewPublisher(jetstream.PublisherConfig{
		URL:    natsURL,
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	r, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	retryMiddleware := middleware.Retry{
		MaxRetries:      1,
		InitialInterval: time.Millisecond * 10,
	}

	poisonQueue, err := middleware.PoisonQueue(pub, "poison_queue")
	if err != nil {
		panic(err)
	}

	r.AddMiddleware(
		// Recoverer middleware recovers panic from handlers and middlewares
		middleware.Recoverer,

		// Limit incoming messages to 10 per second
		middleware.NewThrottle(10, time.Second).Middleware,

		// If the retries limit is exceeded (see retryMiddleware below), the message is sent
		// to the poison queue (published to poison_queue topic)
		poisonQueue,

		// Retry middleware retries message processing if an error occurred in the handler
		retryMiddleware.Middleware,

		// Correlation ID middleware adds the correlation ID of the consumed message to each produced message.
		// It's useful for debugging.
		middleware.CorrelationID,

		// Simulate errors or panics from handler
		wotel.Trace(
			wotel.WithTextMapPropagator()),
	)

	// Close the router when a SIGTERM is sent
	r.AddPlugin(plugin.SignalsHandler)

	var namer jetstream.ConsumerConfigurator

	// Handler that generates "feed" from consumed posts
	//
	// This implementation just prints the posts on stdout,
	// but production ready implementation would save posts to some persistent storage.
	r.AddNoPublisherHandler(
		"feed_generator",
		topic,
		createSubscriber(nc, namer, logger),
		FeedGenerator{printFeedStorage{}}.UpdateFeed,
	)

	if err = r.Run(ctx); err != nil {
		panic(err)
	}
}

func createSubscriber(conn *nats.Conn, namer jetstream.ConsumerConfigurator, logger watermill.LoggerAdapter) message.Subscriber {
	sub, err := jetstream.NewSubscriber(jetstream.SubscriberConfig{
		Conn:           conn,
		Logger:         logger,
		AckWaitTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	return sub
}

// postAdded might look similar to the postAdded type from producer.
// It's intentionally not imported here. We avoid coupling the services at the cost of duplication.
// We don't need all of it's data either (content is not displayed on the feed).
type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`
	Author     string    `json:"author"`
	Title      string    `json:"title"`
}

type feedStorage interface {
	AddToFeed(title, author string, time time.Time) error
}

type printFeedStorage struct{}

func (printFeedStorage) AddToFeed(title, author string, time time.Time) error {
	fmt.Printf("Adding to feed: %s by %s @%s\n", title, author, time)
	return nil
}

type FeedGenerator struct {
	feedStorage feedStorage
}

func (f FeedGenerator) UpdateFeed(message *message.Message) error {
	event := postAdded{}
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return err
	}

	err := f.feedStorage.AddToFeed(event.Title, event.Author, event.OccurredOn)
	if err != nil {
		return fmt.Errorf("cannot update feed: %w", err)
	}

	message.Ack()
	return nil
}

func newTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint("jaeger:4317"), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("consumer"),
		))
	if err != nil {
		return nil, err
	}
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(exp,
			trace.WithBatchTimeout(5*time.Second),
		),
		trace.WithResource(res),
	)
	return traceProvider, nil
}
