package config

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppEnv      string
	ListenAddr  string
	RedisAddr   string
	RedisPass   string
	NATSURL     string
	DatabaseURL string
	BackendURL     string
	JWTSecret      string
	AllowedOrigins []string

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

func getslice(key string, def []string) []string {
	if v := os.Getenv(key); v != "" {
		parts := strings.Split(v, ",")
		var res []string
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				res = append(res, trimmed)
			}
		}
		if len(res) > 0 {
			return res
		}
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
		RedisPass:         getenv("REDIS_PASS", ""),
		NATSURL:           getenv("NATS_URL", "nats://localhost:4222"),
		DatabaseURL:       getenv("DATABASE_URL", "postgres://openwar:openwar@localhost:5432/openwar"),
		BackendURL:        getenv("BACKEND_URL", "http://backend-demo:9001"),
		JWTSecret:         getenv("JWT_SECRET", "dev-secret-change-me"),
		AllowedOrigins:    getslice("ALLOWED_ORIGINS", []string{"*"}),
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
	if len(c.JWTSecret) < 32 {
		return errors.New("JWT_SECRET must be at least 32 characters long for secure HMAC-SHA256 signing")
	}
	if c.JWTSecret == "dev-secret-change-me" {
		return errors.New("JWT_SECRET cannot be the default value")
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
