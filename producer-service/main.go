package main

import (
	"context"
	"encoding/json"
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
)

func initTracer(serviceName string) (opentracing.Tracer, func()) {
	cfg := jaegercfg.Configuration{
		ServiceName: serviceName,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans:           true,
			LocalAgentHostPort: "localhost:6831",
		},
	}

	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		panic(err)
	}

	opentracing.SetGlobalTracer(tracer)
	return tracer, func() { closer.Close() }
}

func produceToKafka(ctx context.Context, data []byte) error {
	span, _ := opentracing.StartSpanFromContext(ctx, "produceToKafka")
	defer span.Finish()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

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

type kafkaHeadersWriter struct {
	headers *[]kafka.Header
}

func (w kafkaHeadersWriter) Set(key, val string) {
	*w.headers = append(*w.headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	spanCtx, _ := opentracing.GlobalTracer().Extract(
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(r.Header),
	)
	span := opentracing.StartSpan("handleRequest", ext.RPCServerOption(spanCtx))
	defer span.Finish()

	ctx := opentracing.ContextWithSpan(r.Context(), span)

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

func main() {
	_, closer := initTracer("producer-service")
	defer closer()

	http.HandleFunc("/data", handleRequest)

	log.Println("Producer service started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
