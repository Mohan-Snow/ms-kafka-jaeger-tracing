package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/segmentio/kafka-go"
	jaegerPropogator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	topic       = "data-pipeline"
	serviceName = "producer-service"
	tracer      = otel.Tracer("producer-service")
)

// KafkaHeaderCarrier adapts kafka.Message headers for OpenTelemetry propagation
type KafkaHeaderCarrier struct {
	Headers *[]kafka.Header
}

// Ensure KafkaHeaderCarrier implements the TextMapCarrier interface
var _ propagation.TextMapCarrier = (*KafkaHeaderCarrier)(nil)

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

	http.HandleFunc("/data", handleRequest)

	log.Println("Producer service started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func initTracer() (*tracesdk.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://jaeger:14268/api/traces")))
	if err != nil {
		return nil, err
	}

	otel.SetTextMapPropagator(jaegerPropogator.Jaeger{})

	tracerProvider := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
	)

	otel.SetTracerProvider(tracerProvider)

	return tracerProvider, nil
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := tracer.Start(ctx, "handle_data_from_request",
		trace.WithAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.url", r.URL.Path),
		))
	defer span.End()

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return
	}

	if err := produceToKafka(ctx, jsonData); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Data accepted and sent to Kafka"))
}

func produceToKafka(ctx context.Context, data []byte) error {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			ClientID: "producer-service",
		},
	}
	defer writer.Close()

	ctx, span := tracer.Start(ctx, "produce_to_kafka")
	defer span.End()

	headers := make([]kafka.Header, 0)
	carrier := KafkaHeaderCarrier{&headers}

	// Inject tracing context into Kafka headers
	otel.GetTextMapPropagator().Inject(ctx, &carrier)

	err := writer.WriteMessages(ctx, kafka.Message{
		Headers: headers,
		Value:   data,
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return err
	}

	return nil
}

// Get returns the value associated with the passed key.
func (c *KafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set stores the key-value pair.
func (c *KafkaHeaderCarrier) Set(key, value string) {
	// Check if key exists and replace
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(value)
			return
		}
	}
	// Otherwise, append new
	*c.Headers = append(*c.Headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys lists the keys stored in this carrier.
func (c *KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.Headers))
	for i, h := range *c.Headers {
		keys[i] = h.Key
	}
	return keys
}
