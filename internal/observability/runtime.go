package observability

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

// Logger emits structured JSON and never logs environment contents or credentials.
func Logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// ShutdownContext is canceled for SIGINT/SIGTERM so servers and workers can drain.
func ShutdownContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
func ShutdownHTTP(ctx context.Context, server *http.Server) error {
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

// WorkerPool bounds active work and waits for the caller's context while acquiring capacity.
type WorkerPool struct{ slots chan struct{} }

func NewWorkerPool(concurrency int) *WorkerPool {
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > 64 {
		concurrency = 64
	}
	return &WorkerPool{slots: make(chan struct{}, concurrency)}
}
func (p *WorkerPool) Run(ctx context.Context, work func(context.Context) error) error {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.slots }()
	return work(ctx)
}
