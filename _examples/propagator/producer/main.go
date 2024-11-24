package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/nats-io/nats.go"
	natsJS "github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	wotel "github.com/voi-oss/watermill-opentelemetry/pkg/opentelemetry"
)

var (
	messagesPerMinute = 10
	numWorkers        = 1
	natsURL           = "nats://nats:4222"
	topic             = "example_topic"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// this is the default watermill will look for if no namer func passed
	consumer := fmt.Sprintf("watermill__%s", topic)

	tracerProvider, err := newTraceProvider(ctx)
	if err != nil {
		panic(err)
	}
	otel.SetTracerProvider(tracerProvider)

	logger := watermill.NewStdLogger(false, false)
	logger.Info("Starting the producer", watermill.LogFields{})

	nc, err := nats.Connect(natsURL)
	if err != nil {
		panic(err)
	}

	js, err := natsJS.New(nc)
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}

	publisher, err := jetstream.NewPublisher(jetstream.PublisherConfig{
		URL:    natsURL,
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	publisherWithTracing := wotel.NewPublisherDecorator(publisher,
		wotel.WithTextMapPropagator(),
	)

	s, se := js.CreateStream(ctx, natsJS.StreamConfig{
		Name:     topic,
		Subjects: []string{topic},
	})
	if se != nil {
		panic(se)
	}

	_, ce := s.CreateOrUpdateConsumer(ctx, natsJS.ConsumerConfig{
		Name:      consumer,
		AckPolicy: natsJS.AckExplicitPolicy,
	})
	if ce != nil {
		panic(ce)
	}

	defer publisher.Close()
	defer func() {
		if err := js.DeleteConsumer(ctx, topic, consumer); err != nil {
			panic(err)
		}
		if err := js.DeleteStream(ctx, topic); err != nil {
			panic(err)
		}
		return
	}()

	closeCh := make(chan struct{})
	workersGroup := &sync.WaitGroup{}
	workersGroup.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go worker(publisherWithTracing, workersGroup, closeCh)
	}

	// wait for SIGINT
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// signal for the workers to stop publishing
	close(closeCh)

	// Waiting for all messages to be published
	workersGroup.Wait()

	logger.Info("All messages published", nil)
}

// worker publishes messages until closeCh is closed.
func worker(publisher message.Publisher, wg *sync.WaitGroup, closeCh chan struct{}) {
	ticker := time.NewTicker(time.Duration(int(time.Minute) / messagesPerMinute))

	for {
		select {
		case <-closeCh:
			ticker.Stop()
			wg.Done()
			return

		case <-ticker.C:
		}

		msgPayload := postAdded{
			OccurredOn: time.Now(),
			Author:     gofakeit.Username(),
			Title:      gofakeit.Sentence(rand.Intn(5) + 1),
			Content:    gofakeit.Sentence(rand.Intn(10) + 5),
		}

		payload, err := json.Marshal(msgPayload)
		if err != nil {
			panic(err)
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)

		// Use a middleware to set the correlation ID, it's useful for debugging
		middleware.SetCorrelationID(watermill.NewShortUUID(), msg)
		err = publisher.Publish(topic, msg)
		if err != nil {
			fmt.Println("cannot publish message:", err)
			continue
		}
	}
}

type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Content string `json:"content"`
}

func newTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint("jaeger:4317"), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("producer"),
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
