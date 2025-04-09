package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	_ "time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/segmentio/kafka-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

var (
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	topic       = "data-pipeline"
	serviceName = "producer-service"
)

type kafkaHeadersWriter struct {
	headers *[]kafka.Header
}

func main() {
	_, closer, err := initTracer()
	defer closer.Close()
	if err != nil {
		log.Fatalf("Could not initialize Jaeger tracer: %v", err)
	}

	http.HandleFunc("/data", handleRequest)

	log.Println("Producer service started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Функция для настройки Jaeger клиента
func initTracer() (opentracing.Tracer, io.Closer, error) {
	cfg := jaegercfg.Configuration{
		ServiceName: serviceName,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans:           true,
			LocalAgentHostPort: "jaeger:6831",
		},
	}

	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		return nil, nil, fmt.Errorf("could not initialize Jaeger tracer: %w", err)
	}

	opentracing.SetGlobalTracer(tracer)
	return tracer, closer, nil
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	spanCtx, _ := opentracing.GlobalTracer().Extract(
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(r.Header),
	)

	span := opentracing.StartSpan("handle_data_from_request", ext.RPCServerOption(spanCtx))
	defer span.Finish()

	// Create child spans
	ctx := opentracing.ContextWithSpan(r.Context(), span)

	// Add tags
	span.SetTag("http.method", "POST")
	span.SetTag("http.url", "/data")

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		ext.Error.Set(span, true)
		return
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		ext.Error.Set(span, true)
		return
	}

	if err := produceToKafka(ctx, jsonData); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		ext.Error.Set(span, true)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Data accepted and sent to Kafka"))
}

// Функция для отправки данных в Kafka
func produceToKafka(ctx context.Context, data []byte) error {
	// Kafka writer with tracing instrumentation
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			ClientID: "producer-service",
		},
	}
	defer writer.Close()

	span, _ := opentracing.StartSpanFromContext(ctx, "produce_to_kafka")
	defer span.Finish()

	headers := make([]kafka.Header, 0)

	// Inject tracing context into Kafka headers
	err := opentracing.GlobalTracer().Inject(
		span.Context(),
		opentracing.TextMap,
		kafkaHeadersWriter{&headers},
	)
	if err != nil {
		return err
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Headers: headers,
		Value:   data,
	})

	if err != nil {
		ext.Error.Set(span, true)
		return err
	}

	return nil
}

func (w kafkaHeadersWriter) Set(key, val string) {
	*w.headers = append(*w.headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}
