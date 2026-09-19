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
	defer func() {
		os.Unsetenv("APP_ENV")
		os.Unsetenv("ADMISSION_RATE")
		os.Unsetenv("HEARTBEAT_INTERVAL")
		os.Unsetenv("JWT_SECRET")
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
}

func TestValidate_ProductionSecret(t *testing.T) {
	prodDefaultCfg := config.Config{
		AppEnv:    "production",
		JWTSecret: "dev-secret-change-me",
	}
	if err := prodDefaultCfg.Validate(); err == nil {
		t.Error("expected error when validating production config with default JWT secret, got nil")
	}

	prodEmptyCfg := config.Config{
		AppEnv:    "production",
		JWTSecret: "",
	}
	if err := prodEmptyCfg.Validate(); err == nil {
		t.Error("expected error when validating production config with empty JWT secret, got nil")
	}

	prodValidCfg := config.Config{
		AppEnv:    "production",
		JWTSecret: "prod-secure-random-secret",
	}
	if err := prodValidCfg.Validate(); err != nil {
		t.Errorf("expected no error for valid production config, got %v", err)
	}

	devDefaultCfg := config.Config{
		AppEnv:    "development",
		JWTSecret: "dev-secret-change-me",
	}
	if err := devDefaultCfg.Validate(); err != nil {
		t.Errorf("expected no error for development config with default secret, got %v", err)
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
