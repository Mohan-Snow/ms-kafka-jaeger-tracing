package main

import (
	"bytes"
	"context"
	_ "encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	jaegerlog "github.com/opentracing/opentracing-go/log"
	"github.com/segmentio/kafka-go"
	"github.com/uber/jaeger-client-go"

	jaegercfg "github.com/uber/jaeger-client-go/config"
)

var (
	kafkaBroker     = os.Getenv("KAFKA_BROKER")
	topic           = "data-pipeline"
	consumerGroupID = "consumer-group"
	consumerPort    = ":8081"
	storageURL      = os.Getenv("STORAGE_URL")
	serviceName     = "consumer-service"
)

func main() {
	_, closer, err := initTracer()
	defer closer.Close()
	if err != nil {
		log.Fatalf("Could not initialize Jaeger tracer: %v", err)
	}

	go consumeFromKafka(context.Background())

	log.Printf("Consumer service started on %s\n", consumerPort)
	log.Fatal(http.ListenAndServe(consumerPort, nil))
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
			jaegerlog.Error(err)
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Extract tracing context from Kafka headers
		headers := make(map[string]string)
		for _, header := range msg.Headers {
			headers[header.Key] = string(header.Value)
		}

		// Извлечение контекста трассировки из Kafka сообщения
		// Создаем Carrier для извлечения контекста из сообщения
		carrier := opentracing.TextMapCarrier(headers)
		if err != nil {
			jaegerlog.Error(err)
			log.Printf("Failed to extract span context: %v", err)
			continue
		}

		// Извлечение контекста из carrier
		spanContext, err := opentracing.GlobalTracer().Extract(
			opentracing.TextMap,
			carrier,
		)
		if err != nil {
			log.Printf("Failed to extract span context: %v", err)
			continue
		}

		// Восстановление контекста и создание нового спана
		span := opentracing.GlobalTracer().StartSpan("process_kafka_message", opentracing.ChildOf(spanContext))
		defer span.Finish()

		// Добавляем лог для демонстрации
		span.LogFields(
			jaegerlog.String("event", "message_received"),
			jaegerlog.String("message", string(msg.Value)),
		)

		// Обработка сообщения
		log.Printf("Received message: %s\n", string(msg.Value))

		ctx := opentracing.ContextWithSpan(context.Background(), span)

		if err := forwardToStorage(ctx, msg.Value); err != nil {
			log.Printf("Failed to forward message: %v", err)
			ext.Error.Set(span, true)
		}

		span.Finish()
	}
}

func forwardToStorage(ctx context.Context, data []byte) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "forwardToStorage  HTTP_POST /store")
	defer span.Finish()

	// Add tags
	span.SetTag("http.method", "POST")
	span.SetTag("http.url", "/store")

	req, err := http.NewRequest("POST", storageURL, bytes.NewBuffer(data))
	if err != nil {
		ext.Error.Set(span, true)
		return err
	}

	// Inject tracing context into HTTP headers
	err = opentracing.GlobalTracer().Inject(
		span.Context(),
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(req.Header),
	)
	if err != nil {
		ext.Error.Set(span, true)
		return err
	}

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
		ext.Error.Set(span, true)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		ext.Error.Set(span, true)
		return fmt.Errorf("storage service returned status: %d", resp.StatusCode)
	}

	return nil
}
