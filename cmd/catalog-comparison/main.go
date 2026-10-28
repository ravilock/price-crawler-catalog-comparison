package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"price-crawler/services/catalog-comparison/internal/config"
	"price-crawler/services/catalog-comparison/internal/observability"

	amqp "github.com/rabbitmq/amqp091-go"
	_ "github.com/lib/pq"
)

func verifyMigration(ctx context.Context, db *sql.DB) error {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return errors.New("cannot inspect catalog-comparison migration state")
	}
	if !exists { return errors.New("catalog-comparison database has no migration version; run the packaged migrations") }
	var count, version int
	var dirty bool
	if err := db.QueryRowContext(ctx, `SELECT count(*), COALESCE(max(version),0), COALESCE(bool_or(dirty),false) FROM public.schema_migrations`).Scan(&count, &version, &dirty); err != nil {
		return errors.New("cannot read catalog-comparison migration state")
	}
	if count != 1 || dirty || version != 1 { return fmt.Errorf("catalog-comparison migration state is not clean (version=%d, dirty=%t, records=%d)", version, dirty, count) }
	return nil
}

func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil { return errors.New("invalid catalog-comparison database configuration") }
	defer db.Close()
	db.SetMaxOpenConns(cfg.WorkerConcurrency + 2)
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(startup); err != nil { return errors.New("catalog-comparison database is unavailable or credentials were rejected") }
	if err := verifyMigration(startup, db); err != nil { return err }
	broker, err := amqp.DialConfig(cfg.RabbitMQURL, amqp.Config{Dial: func(network, address string) (net.Conn, error) { return net.DialTimeout(network, address, 10*time.Second) }})
	if err != nil { return errors.New("catalog-comparison RabbitMQ connection failed") }
	defer func() { if closeErr := broker.Close(); closeErr != nil { logger.Warn("RabbitMQ close failed", "error", closeErr) } }()

	pool := observability.NewWorkerPool(cfg.WorkerConcurrency)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Header().Set("content-type", "application/json"); w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte(`{"status":"alive"}`)) })
	bounded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Run(r.Context(), func(context.Context) error { mux.ServeHTTP(w, r); return nil }); err != nil { http.Error(w, "service is shutting down", http.StatusServiceUnavailable) }
	})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: bounded, ReadHeaderTimeout: 5*time.Second}
	serverErr := make(chan error, 1)
	go func() { logger.Info("catalog-comparison process started", "addr", cfg.HTTPAddr, "worker_concurrency", cfg.WorkerConcurrency); if serveErr := server.ListenAndServe(); !errors.Is(serveErr, http.ErrServerClosed) { serverErr <- serveErr; return }; serverErr <- nil }()
	select { case err := <-serverErr: return err; case <-ctx.Done(): }
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := observability.ShutdownHTTP(shutdown, server); err != nil { return fmt.Errorf("catalog-comparison HTTP shutdown failed: %w", err) }
	return <-serverErr
}

func main() {
	logger := observability.Logger()
	cfg, err := config.Load()
	if err != nil { logger.Error("catalog-comparison configuration failed", "error", err); os.Exit(1) }
	ctx, stop := observability.ShutdownContext(context.Background())
	defer stop()
	if err := run(ctx, cfg, logger); err != nil { logger.Error("catalog-comparison stopped", "error", err); os.Exit(1) }
}
