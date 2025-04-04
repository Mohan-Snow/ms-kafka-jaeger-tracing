package main

import (
	"bytes"
	"context"
	_ "encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
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
)

func main() {
	_, closer := initTracer("consumer-service")
	defer closer()

	go consumeFromKafka(context.Background())

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("Consumer service started on %s\n", consumerPort)
	log.Fatal(http.ListenAndServe(consumerPort, nil))
}

func initTracer(serviceName string) (opentracing.Tracer, func()) {
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
		panic(err)
	}

	opentracing.SetGlobalTracer(tracer)
	return tracer, func() { closer.Close() }
}

func consumeFromKafka(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
		GroupID: consumerGroupID,
	})

	// Create local random generator (thread-safe)
	newRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		// Generate random sleep duration (5-10 seconds)
		sleepDuration := time.Duration(newRand.Intn(3)+1) * time.Second

		log.Printf("Sleeping during reading msg from Kafka for %v\n", sleepDuration)
		time.Sleep(sleepDuration)

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Extract tracing context from Kafka headers
		headers := make(map[string]string)
		for _, header := range msg.Headers {
			headers[header.Key] = string(header.Value)
		}

		spanCtx, _ := opentracing.GlobalTracer().Extract(
			opentracing.TextMap,
			opentracing.TextMapCarrier(headers),
		)
		span := opentracing.StartSpan("processMessageFromKafka", ext.RPCServerOption(spanCtx))
		ctx := opentracing.ContextWithSpan(context.Background(), span)

		log.Printf("Received message: %s\n", string(msg.Value))

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

	// Create local random generator (thread-safe)
	newRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Generate random sleep duration (5-10 seconds)
	sleepDuration := time.Duration(newRand.Intn(2)+1) * time.Second

	span.LogKV("event", "delay_start", "seconds", sleepDuration)

	log.Printf("Sleeping during request to storage-service for %v\n", sleepDuration)
	time.Sleep(sleepDuration)

	span.LogKV("event", "delay_complete")

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
