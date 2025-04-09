package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	kafkaBroker     = os.Getenv("KAFKA_BROKER")
	topic           = "data-pipeline"
	consumerGroupID = "consumer-group"
	consumerPort    = ":8081"
	storageURL      = os.Getenv("STORAGE_URL")
	serviceName     = "consumer-service"
	tracer          = otel.Tracer("consumer-service")
)

func main() {
	tracerProvider, err := initTracer()
	if err != nil {
		log.Fatalf("Could not initialize Jaeger tracer: %v", err)
	}
	defer func() {
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	go consumeFromKafka(context.Background())

	log.Printf("Consumer service started on %s\n", consumerPort)
	log.Fatal(http.ListenAndServe(consumerPort, nil))
}

func initTracer() (*tracesdk.TracerProvider, error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://jaeger:14268/api/traces")))
	if err != nil {
		return nil, fmt.Errorf("failed to create Jaeger exporter: %w", err)
	}

	tracerProvider := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			attribute.String("environment", "production"),
		)),
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tracerProvider, nil
}

func consumeFromKafka(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
		GroupID: consumerGroupID,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Extract tracing context from Kafka headers
		carrier := make(propagation.MapCarrier)
		for _, header := range msg.Headers {
			carrier[header.Key] = string(header.Value)
		}

		// Extract the context from the carrier
		ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

		// Start a new span as a child of the extracted context
		ctx, span := tracer.Start(ctx, "process_kafka_message",
			trace.WithAttributes(
				attribute.String("kafka.topic", topic),
				attribute.String("kafka.group_id", consumerGroupID),
			),
		)

		// Log message details
		span.AddEvent("message_received",
			trace.WithAttributes(
				attribute.String("message", string(msg.Value)),
			),
		)

		log.Printf("Received message: %s\n", string(msg.Value))

		if err := forwardToStorage(ctx, msg.Value); err != nil {
			log.Printf("Failed to forward message: %v", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}

		span.End()
	}
}

func forwardToStorage(ctx context.Context, data []byte) error {
	ctx, span := tracer.Start(ctx, "forwardToStorage HTTP_POST /store",
		trace.WithAttributes(
			attribute.String("http.method", "POST"),
			attribute.String("http.url", storageURL),
		))
	defer span.End()

	req, err := http.NewRequest("POST", storageURL, bytes.NewBuffer(data))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Inject tracing context into HTTP headers
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	req = req.WithContext(ctx)

	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: true,
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		err := fmt.Errorf("storage service returned status: %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
