package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr, DatabaseURL, RabbitMQURL, OIDCIssuer string
	WorkerConcurrency                              int
}

func Load() (Config, error) {
	c := Config{HTTPAddr: env("HTTP_ADDR", ":8080"), DatabaseURL: os.Getenv("DATABASE_URL"), RabbitMQURL: os.Getenv("RABBITMQ_URL"), OIDCIssuer: os.Getenv("OIDC_ISSUER"), WorkerConcurrency: 4}
	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 64 {
			return Config{}, errors.New("WORKER_CONCURRENCY must be between 1 and 64")
		}
		c.WorkerConcurrency = n
	}
	for name, value := range map[string]string{"RABBITMQ_URL": c.RabbitMQURL} {
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
