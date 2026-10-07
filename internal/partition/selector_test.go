package partition

import (
	"fmt"
	"testing"
)

func TestKeyStabilityAndRoundRobin(t *testing.T) {
	var next uint64
	for i := 0; i < 12; i++ {
		if got := Select("", 3, &next); got != i%3 {
			t.Fatalf("round robin: %d", got)
		}
	}
	before := next
	p := Select("customer-42", 3, &next)
	for i := 0; i < 10; i++ {
		if got := Select("customer-42", 3, &next); got != p {
			t.Fatal("key moved partitions")
		}
	}
	if next != before {
		t.Fatal("keyed publishes changed round robin cursor")
	}
	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("customer-%d", i)
		seen[Select(key, 3, &next)] = true
	}
	if len(seen) != 3 {
		t.Fatal("keys did not use all partitions")
	}
	if got := Select("hello", 7, &next); got != 2 {
		t.Fatalf("FNV-1a golden value: got %d, want 2", got)
	}
}
