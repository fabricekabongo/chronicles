package hashroute

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"chronicles/internal/domain"
)

func TestPartitionForStreamKeyDeterministic(t *testing.T) {
	keys := []string{"order-45", "  Order-45 ", "550e8400-e29b-41d4-a716-446655440000", "1234567890"}
	for _, key := range keys {
		p1 := PartitionForStreamKey(key)
		p2 := PartitionForStreamKey(key)
		if p1 != p2 {
			t.Fatalf("partition should be deterministic for %q", key)
		}
		if p1 < 0 || p1 >= PartitionCount {
			t.Fatalf("partition out of range for %q: %d", key, p1)
		}
	}
}

func TestCanonicalizeStreamKeyEdgeCases(t *testing.T) {
	cases := map[string]string{
		"  ABC  ":    "abc",
		"":           "",
		"  üñîçødê ": "üñîçødê",
		"MiXeD Case": "mixed case",
	}
	for in, want := range cases {
		if got := CanonicalizeStreamKey(in); got != want {
			t.Fatalf("canonicalize(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestPartitionRangeProperty(t *testing.T) {
	cfg := &quick.Config{Rand: rand.New(rand.NewSource(time.Now().UnixNano()))}
	if err := quick.Check(func(s string) bool {
		p := PartitionForStreamKey(s)
		return p >= 0 && p < PartitionCount
	}, cfg); err != nil {
		t.Fatalf("partition property failed: %v", err)
	}
}

func TestEnsureRoutePinsCreationDayUTC(t *testing.T) {
	r := NewRouter()
	stream := domain.StreamRef{TenantID: "t1", SubjectType: "order", StreamKey: "Order-45"}

	first := time.Date(2026, 1, 2, 0, 30, 0, 0, time.FixedZone("UTC+2", 2*3600))
	second := first.Add(24 * time.Hour)

	a := r.EnsureRoute(stream, first)
	b := r.EnsureRoute(stream, second)

	if a.CreationDayUTC != "2026-01-01" {
		t.Fatalf("unexpected creation day: %s", a.CreationDayUTC)
	}
	if a.CreationDayUTC != b.CreationDayUTC {
		t.Fatalf("creation day changed from %s to %s", a.CreationDayUTC, b.CreationDayUTC)
	}
	if a.PartitionID != b.PartitionID {
		t.Fatalf("partition changed from %d to %d", a.PartitionID, b.PartitionID)
	}
}
