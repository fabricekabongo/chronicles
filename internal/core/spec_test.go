package core

import (
	"testing"
	"time"
)

func TestPartitionForStreamKeyDeterministic(t *testing.T) {
	key := "order-45"
	p1 := PartitionForStreamKey(key)
	p2 := PartitionForStreamKey(key)
	if p1 != p2 {
		t.Fatalf("expected deterministic partition, got %d and %d", p1, p2)
	}
	if p1 < 0 || p1 >= PartitionCount {
		t.Fatalf("partition out of range: %d", p1)
	}
}

func TestEnsureRoutePinsCreationDayAndPartition(t *testing.T) {
	r := NewRouter()
	first := time.Date(2026, 1, 1, 23, 59, 0, 0, time.FixedZone("UTC+2", 2*3600))
	second := time.Date(2026, 1, 3, 1, 0, 0, 0, time.UTC)

	a := r.EnsureRoute("order", "45", first)
	b := r.EnsureRoute("order", "45", second)

	if a.CreationDayUTC != "2026-01-01" {
		t.Fatalf("expected UTC creation day pinning, got %s", a.CreationDayUTC)
	}
	if a.PartitionID != b.PartitionID {
		t.Fatalf("partition should be pinned, got %d and %d", a.PartitionID, b.PartitionID)
	}
	if a.CreationDayUTC != b.CreationDayUTC {
		t.Fatalf("creation day should be pinned, got %s and %s", a.CreationDayUTC, b.CreationDayUTC)
	}
}

func TestQuorumSize(t *testing.T) {
	cases := []struct {
		replicas int
		want     int
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
	}
	for _, c := range cases {
		got := QuorumSize(c.replicas)
		if got != c.want {
			t.Fatalf("quorum(%d) = %d, want %d", c.replicas, got, c.want)
		}
	}
}
