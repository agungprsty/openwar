package config

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	AppEnv      string
	ListenAddr  string
	RedisAddr   string
	NATSURL     string
	DatabaseURL string
	BackendURL  string
	JWTSecret   string

	AdmissionRate     int
	AdmissionTTL      time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration
	IdempotencyWin    time.Duration
	InventoryShards   int
	PaymentWindow     time.Duration
	ReservationTTL    time.Duration
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func Load() Config {
	return Config{
		AppEnv:            getenv("APP_ENV", "development"),
		ListenAddr:        getenv("LISTEN_ADDR", ":8080"),
		RedisAddr:         getenv("REDIS_ADDR", "localhost:6379"),
		NATSURL:           getenv("NATS_URL", "nats://localhost:4222"),
		DatabaseURL:       getenv("DATABASE_URL", "postgres://openwar:openwar@localhost:5432/openwar"),
		BackendURL:        getenv("BACKEND_URL", "http://backend-demo:9001"),
		JWTSecret:         getenv("JWT_SECRET", "dev-secret-change-me"),
		AdmissionRate:     getint("ADMISSION_RATE", 100),
		AdmissionTTL:      getdur("ADMISSION_TTL", 5*time.Minute),
		HeartbeatInterval: getdur("HEARTBEAT_INTERVAL", 15*time.Second),
		HeartbeatTTL:      getdur("HEARTBEAT_TTL", 45*time.Second),
		IdempotencyWin:    getdur("IDEMPOTENCY_WINDOW", 24*time.Hour),
		InventoryShards:   getint("INVENTORY_SHARDS", 32),
		PaymentWindow:     getdur("PAYMENT_WINDOW", 15*time.Minute),
		ReservationTTL:    getdur("RESERVATION_TTL", 25*time.Minute),
	}
}

// Validate checks configuration for security risks and missing fields.
func (c Config) Validate() error {
	if (c.AppEnv == "production" || c.AppEnv == "prod") && (c.JWTSecret == "" || c.JWTSecret == "dev-secret-change-me") {
		return errors.New("JWT_SECRET must be configured with a secure secret in production")
	}
	return nil
}

// SanitizeDSN masks passwords in DSN or database URLs for safe logging.
func SanitizeDSN(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "[redacted-invalid-url]"
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), "redacted")
		}
	}
	return u.String()
}
