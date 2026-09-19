package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/openwar/openwar/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear relevant env vars to test defaults
	envKeys := []string{
		"APP_ENV", "LISTEN_ADDR", "REDIS_ADDR", "NATS_URL", "DATABASE_URL",
		"BACKEND_URL", "JWT_SECRET", "ADMISSION_RATE", "ADMISSION_TTL",
		"HEARTBEAT_INTERVAL", "HEARTBEAT_TTL", "IDEMPOTENCY_WINDOW",
		"INVENTORY_SHARDS", "PAYMENT_WINDOW", "RESERVATION_TTL",
	}
	for _, k := range envKeys {
		os.Unsetenv(k)
	}

	cfg := config.Load()
	if cfg.AppEnv != "development" {
		t.Errorf("expected default AppEnv 'development', got %q", cfg.AppEnv)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("expected default ListenAddr ':8080', got %q", cfg.ListenAddr)
	}
	if cfg.AdmissionRate != 100 {
		t.Errorf("expected default AdmissionRate 100, got %d", cfg.AdmissionRate)
	}
	if cfg.AdmissionTTL != 5*time.Minute {
		t.Errorf("expected default AdmissionTTL 5m, got %v", cfg.AdmissionTTL)
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	os.Setenv("APP_ENV", "production")
	os.Setenv("ADMISSION_RATE", "500")
	os.Setenv("HEARTBEAT_INTERVAL", "5s")
	os.Setenv("JWT_SECRET", "super-secret-key-12345")
	os.Setenv("ALLOWED_ORIGINS", "https://app.openwar.io, https://admin.openwar.io")
	defer func() {
		os.Unsetenv("APP_ENV")
		os.Unsetenv("ADMISSION_RATE")
		os.Unsetenv("HEARTBEAT_INTERVAL")
		os.Unsetenv("JWT_SECRET")
		os.Unsetenv("ALLOWED_ORIGINS")
	}()

	cfg := config.Load()
	if cfg.AppEnv != "production" {
		t.Errorf("expected AppEnv 'production', got %q", cfg.AppEnv)
	}
	if cfg.AdmissionRate != 500 {
		t.Errorf("expected AdmissionRate 500, got %d", cfg.AdmissionRate)
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Errorf("expected HeartbeatInterval 5s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.JWTSecret != "super-secret-key-12345" {
		t.Errorf("expected JWTSecret 'super-secret-key-12345', got %q", cfg.JWTSecret)
	}
	if len(cfg.AllowedOrigins) != 2 || cfg.AllowedOrigins[0] != "https://app.openwar.io" || cfg.AllowedOrigins[1] != "https://admin.openwar.io" {
		t.Errorf("expected AllowedOrigins parsed, got %v", cfg.AllowedOrigins)
	}
}

func TestValidate_JWTSecret(t *testing.T) {
	defaultCfg := config.Config{
		AppEnv:    "development",
		JWTSecret: "dev-secret-change-me",
	}
	if err := defaultCfg.Validate(); err == nil {
		t.Error("expected error when validating config with default JWT secret, got nil")
	}

	emptyCfg := config.Config{
		AppEnv:    "production",
		JWTSecret: "",
	}
	if err := emptyCfg.Validate(); err == nil {
		t.Error("expected error when validating config with empty JWT secret, got nil")
	}

	shortCfg := config.Config{
		AppEnv:    "production",
		JWTSecret: "prod-secure-random-secret", // 25 chars
	}
	if err := shortCfg.Validate(); err == nil {
		t.Error("expected error when validating config with short JWT secret, got nil")
	}

	validCfg := config.Config{
		AppEnv:    "production",
		JWTSecret: "this-is-a-very-secure-secret-that-is-at-least-32-bytes",
	}
	if err := validCfg.Validate(); err != nil {
		t.Errorf("expected no error for valid config, got %v", err)
	}
}

func TestSanitizeDSN(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "postgres://user:password123@localhost:5432/openwar?sslmode=disable",
			expected: "postgres://user:redacted@localhost:5432/openwar?sslmode=disable",
		},
		{
			input:    "postgres://localhost:5432/openwar",
			expected: "postgres://localhost:5432/openwar",
		},
		{
			input:    "invalid url with spaces : //",
			expected: "[redacted-invalid-url]",
		},
	}

	for _, tt := range tests {
		got := config.SanitizeDSN(tt.input)
		if got != tt.expected {
			t.Errorf("SanitizeDSN(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
