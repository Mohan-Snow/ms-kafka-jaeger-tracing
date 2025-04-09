package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	jaegerlog "github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

var (
	dbHost      = os.Getenv("DB_HOST")
	dbPort      = 5432
	dbUser      = os.Getenv("DB_USER")
	dbPassword  = os.Getenv("DB_PASSWORD")
	dbName      = os.Getenv("DB_NAME")
	storagePort = ":8082"
	serviceName = "storage-service"
)

type Data struct {
	ID        int       `json:"id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

var db *sql.DB

func main() {
	_, closer, err := initTracer()
	defer closer.Close()
	if err != nil {
		log.Fatalf("Could not initialize Jaeger tracer: %v", err)
	}

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

	// Создаем спан для отслеживания HTTP запроса
	//span := opentracing.GlobalTracer().StartSpan("handle_data_from_request")
	//defer span.Finish()

	// Извлекаем контекст из запроса и привязываем его к текущему спану
	span.SetTag("http.method", r.Method)
	span.SetTag("http.url", r.URL.String())

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

	err = storeData(span.Context(), string(jsonData))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		ext.Error.Set(span, true)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Data stored successfully"))
}

func storeData(parentCtx opentracing.SpanContext, data string) error {
	// Создаем спан для операции сохранения в базе данных
	span := opentracing.GlobalTracer().StartSpan("save_data_to_database", opentracing.ChildOf(parentCtx))
	defer span.Finish()

	// Создаем новый span как дочерний для parentCtx
	//span := opentracing.StartSpan(
	//	"store_data",
	//	opentracing.ChildOf(parentCtx),
	//	opentracing.Tag{Key: string(ext.Component), Value: "db"},
	//)
	//defer span.Finish()

	// Получаем контекст с информацией о span
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	// Выполняем SQL запрос с трассировкой
	span.LogFields(
		jaegerlog.String("query", "INSERT INTO data"),
		jaegerlog.String("data", data),
	)

	_, err := db.ExecContext(ctx, "INSERT INTO data (content) VALUES ($1)", data)
	if err != nil {
		span.LogFields(jaegerlog.Error(err))
		ext.Error.Set(span, true)
		return err
	}

	return nil
}
