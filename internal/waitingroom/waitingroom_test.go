package waitingroom_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

func setupTestRoom(t *testing.T) (*waitingroom.Room, *miniredis.Miniredis, *redis.Client) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	room := waitingroom.New(rdb, 15*time.Second, 5*time.Minute)
	return room, s, rdb
}

func TestRoom_JoinAndRank(t *testing.T) {
	ctx := context.Background()
	room, _, _ := setupTestRoom(t)

	// User 1 joins
	pos1, err := room.Join(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if pos1 != 0 {
		t.Errorf("expected pos 0 for first user, got %d", pos1)
	}

	// User 2 joins
	pos2, err := room.Join(ctx, "event-1", "session-2")
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if pos2 != 1 {
		t.Errorf("expected pos 1 for second user, got %d", pos2)
	}

	// User 1 joins again (duplicate join should return rank 0)
	pos1Rejoin, err := room.Join(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Rejoin failed: %v", err)
	}
	if pos1Rejoin != 0 {
		t.Errorf("expected pos 0 on rejoin, got %d", pos1Rejoin)
	}
}

func TestRoom_Status(t *testing.T) {
	ctx := context.Background()
	room, _, _ := setupTestRoom(t)

	// Non-existent session
	pos, admitted, err := room.Status(ctx, "event-1", "non-existent")
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if pos != -1 || admitted {
		t.Errorf("expected pos -1 and admitted false, got pos %d, admitted %v", pos, admitted)
	}

	// Join session
	room.Join(ctx, "event-1", "session-1")
	pos, admitted, err = room.Status(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if pos != 0 || admitted {
		t.Errorf("expected pos 0 and admitted false, got pos %d, admitted %v", pos, admitted)
	}
}

func TestRoom_HeartbeatAndAdmit(t *testing.T) {
	ctx := context.Background()
	room, _, _ := setupTestRoom(t)

	// Try to admit a session that hasn't sent a heartbeat -> should return false
	admitted, err := room.Admit(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Admit error: %v", err)
	}
	if admitted {
		t.Error("expected Admit to return false for session without heartbeat")
	}

	// Send heartbeat
	pos, isAdm, err := room.Heartbeat(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if isAdm {
		t.Error("expected admitted false on initial heartbeat")
	}
	_ = pos

	// Admit after heartbeat -> should succeed
	admitted, err = room.Admit(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Admit failed: %v", err)
	}
	if !admitted {
		t.Error("expected Admit to succeed after heartbeat")
	}

	// Check HasAdmission
	hasAdm, err := room.HasAdmission(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("HasAdmission failed: %v", err)
	}
	if !hasAdm {
		t.Error("expected HasAdmission to return true")
	}

	// Check Heartbeat after admission
	_, isAdmNow, err := room.Heartbeat(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	if !isAdmNow {
		t.Error("expected heartbeat to report admitted = true")
	}
}

func TestRoom_ConsumeAdmissionSingleUse(t *testing.T) {
	ctx := context.Background()
	room, _, _ := setupTestRoom(t)

	// Send heartbeat and admit
	room.Heartbeat(ctx, "event-1", "session-1")
	room.Admit(ctx, "event-1", "session-1")

	// First consumption -> should succeed (returns true)
	consumed1, err := room.ConsumeAdmission(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("ConsumeAdmission failed: %v", err)
	}
	if !consumed1 {
		t.Error("expected first ConsumeAdmission to return true")
	}

	// Second consumption -> should fail (single-use token consumed)
	consumed2, err := room.ConsumeAdmission(ctx, "event-1", "session-1")
	if err != nil {
		t.Fatalf("Second ConsumeAdmission failed: %v", err)
	}
	if consumed2 {
		t.Error("expected second ConsumeAdmission to return false (single-use token)")
	}
}
