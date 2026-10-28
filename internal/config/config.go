package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr, DatabaseURL, RabbitMQURL, OIDCIssuer   string
	WorkerConcurrency                                int
	ReadinessTimeout, ReconnectInitial, ReconnectMax time.Duration
}

func Load() (Config, error) {
	c := Config{HTTPAddr: env("HTTP_ADDR", ":8080"), DatabaseURL: os.Getenv("DATABASE_URL"), RabbitMQURL: os.Getenv("RABBITMQ_URL"), OIDCIssuer: os.Getenv("OIDC_ISSUER"), WorkerConcurrency: 4, ReadinessTimeout: 120 * time.Second, ReconnectInitial: time.Second, ReconnectMax: 30 * time.Second}
	for name, value := range map[string]*time.Duration{"READINESS_TIMEOUT_SECONDS": &c.ReadinessTimeout, "RECONNECT_INITIAL_DELAY_SECONDS": &c.ReconnectInitial, "RECONNECT_MAX_DELAY_SECONDS": &c.ReconnectMax} {
		if raw := os.Getenv(name); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > 3600 {
				return Config{}, fmt.Errorf("%s must be between 1 and 3600", name)
			}
			*value = time.Duration(n) * time.Second
		}
	}
	if c.ReconnectInitial > c.ReconnectMax {
		return Config{}, errors.New("initial reconnect delay must not exceed maximum")
	}
	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 64 {
			return Config{}, errors.New("WORKER_CONCURRENCY must be between 1 and 64")
		}
		c.WorkerConcurrency = n
	}
	for name, value := range map[string]string{"RABBITMQ_URL": c.RabbitMQURL, "DATABASE_URL": c.DatabaseURL} {
		if value == "" {
			return Config{}, fmt.Errorf("%s is required", name)
		}
	}
	return c, nil
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
