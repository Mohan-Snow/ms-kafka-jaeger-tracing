package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
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
	dbHost      = os.Getenv("DB_HOST")
	dbPort      = 5432
	dbUser      = os.Getenv("DB_USER")
	dbPassword  = os.Getenv("DB_PASSWORD")
	dbName      = os.Getenv("DB_NAME")
	storagePort = ":8082"
	serviceName = "storage-service"
	tracer      = otel.Tracer("storage-service")
)

type Data struct {
	ID        int       `json:"id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

var db *sql.DB

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

	db = initDB()
	defer db.Close()

	http.HandleFunc("/store", handleRequest)

	log.Printf("Storage service started on %s\n", storagePort)
	log.Fatal(http.ListenAndServe(storagePort, nil))
}

func initDB() *sql.DB {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS data (
			id SERIAL PRIMARY KEY,
			content JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	return db
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

func handleRequest(w http.ResponseWriter, r *http.Request) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := tracer.Start(ctx, "handle_data_from_request",
		trace.WithAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.url", r.URL.String()),
		))
	defer span.End()

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	err = storeData(ctx, string(jsonData))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Data stored successfully"))
}

func storeData(ctx context.Context, data string) error {
	ctx, span := tracer.Start(ctx, "save_data_to_database",
		trace.WithAttributes(
			attribute.String("component", "db"),
		))
	defer span.End()

	span.AddEvent("Executing SQL query",
		trace.WithAttributes(
			attribute.String("query", "INSERT INTO data (content) VALUES ($1)"),
			attribute.String("data", data),
		))

	_, err := db.ExecContext(ctx, "INSERT INTO data (content) VALUES ($1)", data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
