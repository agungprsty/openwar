package resilience

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateHalfOpen:
		return "HALF_OPEN"
	case StateOpen:
		return "OPEN"
	default:
		return "UNKNOWN"
	}
}

type Config struct {
	MaxConsecutiveFailures int
	Cooldown               time.Duration
	HalfOpenMaxSuccess     int
}

func DefaultConfig() Config {
	return Config{
		MaxConsecutiveFailures: 5,
		Cooldown:               5 * time.Second,
		HalfOpenMaxSuccess:     2,
	}
}

type CircuitBreaker struct {
	mu           sync.Mutex
	cfg          Config
	state        State
	failures     int
	successes    int
	lastFailedAt time.Time
}

func NewCircuitBreaker(cfg Config) *CircuitBreaker {
	if cfg.MaxConsecutiveFailures <= 0 {
		cfg.MaxConsecutiveFailures = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 5 * time.Second
	}
	if cfg.HalfOpenMaxSuccess <= 0 {
		cfg.HalfOpenMaxSuccess = 2
	}
	return &CircuitBreaker{
		cfg:   cfg,
		state: StateClosed,
	}
}

func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.checkStateLocked()
	return cb.state
}

func (cb *CircuitBreaker) checkStateLocked() {
	if cb.state == StateOpen && time.Since(cb.lastFailedAt) >= cb.cfg.Cooldown {
		cb.state = StateHalfOpen
		cb.successes = 0
		cb.failures = 0
	}
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	cb.mu.Lock()
	cb.checkStateLocked()

	if cb.state == StateOpen {
		cb.mu.Unlock()
		return ErrCircuitOpen
	}
	cb.mu.Unlock()

	// Execute operation
	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.onFailureLocked()
		return err
	}

	cb.onSuccessLocked()
	return nil
}

func (cb *CircuitBreaker) onFailureLocked() {
	cb.lastFailedAt = time.Now()
	switch cb.state {
	case StateClosed:
		cb.failures++
		if cb.failures >= cb.cfg.MaxConsecutiveFailures {
			cb.state = StateOpen
		}
	case StateHalfOpen:
		cb.state = StateOpen
		cb.failures = cb.cfg.MaxConsecutiveFailures
		cb.successes = 0
	}
}

func (cb *CircuitBreaker) onSuccessLocked() {
	switch cb.state {
	case StateClosed:
		cb.failures = 0
	case StateHalfOpen:
		cb.successes++
		if cb.successes >= cb.cfg.HalfOpenMaxSuccess {
			cb.state = StateClosed
			cb.failures = 0
			cb.successes = 0
		}
	}
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
	cb.lastFailedAt = time.Time{}
}
