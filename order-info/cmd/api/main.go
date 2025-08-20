package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	h "github.com/MaksProg/order-service/internal/http"
	"github.com/MaksProg/order-service/internal/cache"
	"github.com/MaksProg/order-service/internal/db"
	kc "github.com/MaksProg/order-service/internal/kafka"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" { return v }
	return def
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// конфиг
	pgHost := getenv("PG_HOST", "localhost")
	pgPort, _ := strconv.Atoi(getenv("PG_PORT", "5432"))
	pgUser := getenv("PG_USER", "order_user")
	pgPass := getenv("PG_PASS", "order_pass")
	pgDB   := getenv("PG_DB",   "order_db")

	kafkaBrokers := []string{getenv("KAFKA_BROKER", "localhost:9092")}
	kafkaTopic   := getenv("KAFKA_TOPIC", "orders")
	kafkaGroup   := getenv("KAFKA_GROUP", "order-service")

	// БД
	store, err := db.New(ctx, pgHost, pgPort, pgUser, pgPass, pgDB)
	if err != nil { log.Fatal(err) }
	defer store.Close()

	// кэш
	c := cache.New()

	// восстановление кэша на старте (берём последние N заказов; для простоты — все)
	// в учебной задаче можно обойтись выборкой по orders и подгрузить детали
	// (оставлю простую версию: без лимита)
	rows, err := store.Pool.Query(ctx, `SELECT order_uid FROM orders`)
	if err != nil { log.Printf("cache warmup skipped: %v", err) }
	for rows != nil && rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			if o, err := store.GetOrder(ctx, id); err == nil {
				c.Set(o)
			}
		}
	}
	if rows != nil { rows.Close() }

	// Kafka consumer
	consumer := kc.NewConsumer(kafkaBrokers, kafkaTopic, kafkaGroup, store, c)
	go func() {
		if err := consumer.Run(ctx); err != nil {
			log.Printf("kafka consumer stopped: %v", err)
			cancel()
		}
	}()
	defer consumer.Close()

	// HTTP
	staticDir := "internal/web/static"
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      h.Router(store, c, staticDir),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Println("HTTP listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down...")
	shCtx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	_ = srv.Shutdown(shCtx)
}
