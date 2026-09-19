package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openwar/openwar/internal/resilience"
)

func TestCircuitBreaker_ClosedStateSuccess(t *testing.T) {
	cb := resilience.NewCircuitBreaker(resilience.Config{
		MaxConsecutiveFailures: 3,
		Cooldown:               100 * time.Millisecond,
		HalfOpenMaxSuccess:     2,
	})

	if cb.State() != resilience.StateClosed {
		t.Errorf("expected StateClosed, got %v", cb.State())
	}

	err := cb.Execute(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Errorf("expected nil error on success, got %v", err)
	}
	if cb.State() != resilience.StateClosed {
		t.Errorf("expected StateClosed after success, got %v", cb.State())
	}
}

func TestCircuitBreaker_TrippingToOpen(t *testing.T) {
	cb := resilience.NewCircuitBreaker(resilience.Config{
		MaxConsecutiveFailures: 3,
		Cooldown:               100 * time.Millisecond,
		HalfOpenMaxSuccess:     2,
	})

	testErr := errors.New("upstream failed")

	// Fail 1 and 2 -> still closed
	for i := 0; i < 2; i++ {
		err := cb.Execute(context.Background(), func() error {
			return testErr
		})
		if !errors.Is(err, testErr) {
			t.Errorf("expected %v, got %v", testErr, err)
		}
		if cb.State() != resilience.StateClosed {
			t.Errorf("expected StateClosed at failure %d, got %v", i+1, cb.State())
		}
	}

	// Fail 3 -> trips to OPEN
	err := cb.Execute(context.Background(), func() error {
		return testErr
	})
	if !errors.Is(err, testErr) {
		t.Errorf("expected %v, got %v", testErr, err)
	}
	if cb.State() != resilience.StateOpen {
		t.Errorf("expected StateOpen after 3 consecutive failures, got %v", cb.State())
	}

	// 4th call immediately rejected by open circuit
	err = cb.Execute(context.Background(), func() error {
		t.Fatal("handler should not have been called when circuit is open")
		return nil
	})
	if !errors.Is(err, resilience.ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_HalfOpenRecovery(t *testing.T) {
	cb := resilience.NewCircuitBreaker(resilience.Config{
		MaxConsecutiveFailures: 2,
		Cooldown:               50 * time.Millisecond,
		HalfOpenMaxSuccess:     2,
	})

	// Trip to open
	cb.Execute(context.Background(), func() error { return errors.New("fail") })
	cb.Execute(context.Background(), func() error { return errors.New("fail") })

	if cb.State() != resilience.StateOpen {
		t.Fatalf("expected StateOpen, got %v", cb.State())
	}

	// Wait for cooldown
	time.Sleep(70 * time.Millisecond)

	if cb.State() != resilience.StateHalfOpen {
		t.Errorf("expected StateHalfOpen after cooldown, got %v", cb.State())
	}

	// 1st success in half-open -> remains half-open
	err := cb.Execute(context.Background(), func() error { return nil })
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if cb.State() != resilience.StateHalfOpen {
		t.Errorf("expected StateHalfOpen after 1 success, got %v", cb.State())
	}

	// 2nd success in half-open -> closes circuit
	err = cb.Execute(context.Background(), func() error { return nil })
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if cb.State() != resilience.StateClosed {
		t.Errorf("expected StateClosed after reaching HalfOpenMaxSuccess, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailureReTrips(t *testing.T) {
	cb := resilience.NewCircuitBreaker(resilience.Config{
		MaxConsecutiveFailures: 2,
		Cooldown:               50 * time.Millisecond,
		HalfOpenMaxSuccess:     2,
	})

	// Trip to open
	cb.Execute(context.Background(), func() error { return errors.New("fail") })
	cb.Execute(context.Background(), func() error { return errors.New("fail") })

	// Wait for cooldown -> HalfOpen
	time.Sleep(70 * time.Millisecond)
	if cb.State() != resilience.StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}

	// Fail in HalfOpen -> immediately re-trips to Open
	testErr := errors.New("temporary error")
	err := cb.Execute(context.Background(), func() error { return testErr })
	if !errors.Is(err, testErr) {
		t.Errorf("expected %v, got %v", testErr, err)
	}
	if cb.State() != resilience.StateOpen {
		t.Errorf("expected StateOpen after failure in half-open, got %v", cb.State())
	}
}
