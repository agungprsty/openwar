package order

import (
	"testing"
)

func TestShardForUser(t *testing.T) {
	tests := []struct {
		userID string
		shards int
	}{
		{"user-1", 4},
		{"user-2", 32},
		{"user-42", 16},
	}

	for _, tt := range tests {
		shard := shardForUser(tt.userID, tt.shards)
		if shard < 0 || shard >= tt.shards {
			t.Errorf("shardForUser(%q, %d) = %d; out of range [0, %d)", tt.userID, tt.shards, shard, tt.shards)
		}
	}
}
