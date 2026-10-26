package database

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// PostgresConnection initializes a high-performance PostgreSQL connection pool (pgxpool)
// with automated schema migrations and OpenTelemetry tracing instrumentation.
func PostgresConnection() (*pgxpool.Pool, error) {
	_ = godotenv.Load()

	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := os.Getenv("DB_NAME")

	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)

	// Автоматический запуск миграций (если присутствует папка migrations)
	if _, err := os.Stat("migrations"); err == nil {
		m, err := migrate.New("file://migrations", connStr)
		if err == nil {
			defer m.Close()
			if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
				return nil, fmt.Errorf("failed to apply migrations: %w", err)
			}
		}
	}

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse pgxpool config: %w", err)
	}

	maxConns := int32(25)
	if mcStr := os.Getenv("DB_MAX_CONNS"); mcStr != "" {
		if mc, err := strconv.Atoi(mcStr); err == nil && mc > 0 {
			maxConns = int32(mc)
		}
	}

	config.MinConns = 2
	config.MaxConns = maxConns
	config.MaxConnLifetime = time.Hour

	// Инструментация OpenTelemetry для pgxpool
	config.ConnConfig.Tracer = otelpgx.NewTracer()

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database pool: %w", err)
	}

	return pool, nil
}
