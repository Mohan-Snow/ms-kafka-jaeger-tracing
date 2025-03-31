package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
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
)

type Data struct {
	ID        int       `json:"id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
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

func storeDataHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		spanCtx, _ := opentracing.GlobalTracer().Extract(
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(r.Header),
		)
		span := opentracing.StartSpan("storeData", ext.RPCServerOption(spanCtx))
		defer span.Finish()

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

		_, err = db.ExecContext(
			opentracing.ContextWithSpan(r.Context(), span),
			"INSERT INTO data (content) VALUES ($1)",
			jsonData,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			ext.Error.Set(span, true)
			return
		}

		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("Data stored successfully"))
	}
}

func main() {
	_, closer := initTracer("storage-service")
	defer closer()

	db := initDB()
	defer db.Close()

	http.HandleFunc("/store", storeDataHandler(db))
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("Storage service started on %s\n", storagePort)
	log.Fatal(http.ListenAndServe(storagePort, nil))
}
