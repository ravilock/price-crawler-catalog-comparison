package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"price-crawler/services/catalog-comparison/internal/config"
	"price-crawler/services/catalog-comparison/internal/observability"

	_ "github.com/lib/pq"
)

func verifyMigration(ctx context.Context, db *sql.DB) error {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return errors.New("cannot inspect catalog-comparison migration state")
	}
	if !exists {
		return errors.New("catalog-comparison database has no migration version; run the packaged migrations")
	}
	var count, version int
	var dirty bool
	if err := db.QueryRowContext(ctx, `SELECT count(*), COALESCE(max(version),0), COALESCE(bool_or(dirty),false) FROM public.schema_migrations`).Scan(&count, &version, &dirty); err != nil {
		return errors.New("cannot read catalog-comparison migration state")
	}
	if count != 1 || dirty || version != 1 {
		return fmt.Errorf("catalog-comparison migration state is not clean (version=%d, dirty=%t, records=%d)", version, dirty, count)
	}
	return nil
}

func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return errors.New("invalid catalog-comparison database configuration")
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			logger.Warn("database close failed", "error", closeErr)
		}
	}()
	db.SetMaxOpenConns(cfg.WorkerConcurrency + 2)
	broker := &observability.Broker{URL: cfg.RabbitMQURL, InitialDelay: cfg.ReconnectInitial, MaxDelay: cfg.ReconnectMax}
	database := &observability.Dependency{InitialDelay: cfg.ReconnectInitial, MaxDelay: cfg.ReconnectMax, Check: func(probe context.Context) error {
		var one int
		if err := db.QueryRowContext(probe, "SELECT 1").Scan(&one); err != nil || one != 1 {
			return errors.New("catalog-comparison database unavailable")
		}
		return verifyMigration(probe, db)
	}}
	monitorCtx, stopMonitors := context.WithCancel(ctx)
	brokerReady, databaseReady := make(chan struct{}), make(chan struct{})
	brokerDone, databaseDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(brokerDone); broker.Run(monitorCtx, brokerReady) }()
	go func() { defer close(databaseDone); database.Run(monitorCtx, databaseReady) }()
	defer func() { stopMonitors(); <-brokerDone; <-databaseDone }()
	startup, cancel := context.WithTimeout(ctx, cfg.ReadinessTimeout)
	defer cancel()
	if err := broker.WaitReady(startup, brokerReady); err != nil {
		return err
	}
	select {
	case <-databaseReady:
	case <-startup.Done():
		return fmt.Errorf("catalog-comparison database or migration readiness timeout: %v", database.LastError())
	}

	pool := observability.NewWorkerPool(cfg.WorkerConcurrency)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !broker.Ready() || !database.Ready() {
			http.Error(w, "dependency unavailable or migration not clean", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"alive"}`))
	})
	bounded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" && (!broker.Ready() || !database.Ready()) {
			http.Error(w, "dependency unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := pool.Run(r.Context(), func(context.Context) error { mux.ServeHTTP(w, r); return nil }); err != nil {
			http.Error(w, "service is shutting down", http.StatusServiceUnavailable)
		}
	})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: bounded, ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("catalog-comparison process started", "addr", cfg.HTTPAddr, "worker_concurrency", cfg.WorkerConcurrency)
		if serveErr := server.ListenAndServe(); !errors.Is(serveErr, http.ErrServerClosed) {
			serverErr <- serveErr
			return
		}
		serverErr <- nil
	}()
	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := observability.ShutdownHTTP(shutdown, server); err != nil {
		return fmt.Errorf("catalog-comparison HTTP shutdown failed: %w", err)
	}
	return <-serverErr
}

func main() {
	logger := observability.Logger()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("catalog-comparison configuration failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := observability.ShutdownContext(context.Background())
	defer stop()
	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("catalog-comparison stopped", "error", err)
		os.Exit(1)
	}
}
