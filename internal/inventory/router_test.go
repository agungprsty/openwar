package inventory

import "testing"

func TestParseSKUFromInvKey(t *testing.T) {
	tests := []struct {
		invKey string
		want   string
	}{
		{"inventory:flash-sale-001:ticket:shard:0", "flash-sale-001:ticket"},
		{"inventory:sku-x:shard:15", "sku-x"},
		{"invalid_key", ""},
		{"inventory:no_shard", ""},
	}

	for _, tt := range tests {
		got := parseSKUFromInvKey(tt.invKey)
		if got != tt.want {
			t.Errorf("parseSKUFromInvKey(%q) = %q, want %q", tt.invKey, got, tt.want)
		}
	}
}
