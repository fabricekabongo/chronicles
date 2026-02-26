package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"chronicles/internal/domain"
	"chronicles/internal/hashroute"
	"chronicles/internal/storage"
)

func TestSchemaInitializationCreatesExpectedTables(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.catalogDB(0); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	events, err := s.eventsDB("2026-01-01", 0)
	if err != nil {
		t.Fatalf("events init: %v", err)
	}

	var cnt int
	if err := events.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='entries'`).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatalf("entries table missing")
	}
}

func TestEntriesAreAppendOnlyViaTriggers(t *testing.T) {
	ctx := context.Background()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stream := domain.StreamRef{TenantID: "t1", SubjectType: "order", StreamKey: "45"}
	route, err := s.EnsureRoute(ctx, stream, domain.PartitionID(hashroute.PartitionForStreamKey(stream.StreamKey)), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	entry := storage.AppendEntry{LSN: 1, EventID: "e1", EventType: "created", EventTimeUTCNs: 1, ReceivedAtUTCNs: 2, PayloadJSON: "{}", Source: "socket", SourceRef: "a", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey}
	if err := s.AppendUncommittedBatch(ctx, route, 1, []storage.AppendEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCommitted(ctx, route.PartitionID, route.CreationDayUTC, 1, 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	db, err := s.eventsDB(route.CreationDayUTC, route.PartitionID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`UPDATE entries SET event_type='x' WHERE lsn=1`)
	if err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("expected append-only update error, got %v", err)
	}
	_, err = db.Exec(`DELETE FROM entries WHERE lsn=1`)
	if err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("expected append-only delete error, got %v", err)
	}
}

func TestAppendBatchTransactionAndDedup(t *testing.T) {
	ctx := context.Background()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stream := domain.StreamRef{TenantID: "t1", SubjectType: "order", StreamKey: "45"}
	route, err := s.EnsureRoute(ctx, stream, domain.PartitionID(hashroute.PartitionForStreamKey(stream.StreamKey)), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	entries := []storage.AppendEntry{
		{LSN: 1, EventID: "e1", EventType: "created", EventTimeUTCNs: 10, ReceivedAtUTCNs: 20, PayloadJSON: "{}", Source: "socket", SourceRef: "1", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
		{LSN: 2, EventID: "e2", EventType: "paid", EventTimeUTCNs: 11, ReceivedAtUTCNs: 21, PayloadJSON: "{}", Source: "socket", SourceRef: "2", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
	}
	if err := s.AppendUncommittedBatch(ctx, route, 3, entries); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendUncommittedBatch(ctx, route, 3, []storage.AppendEntry{entries[0]}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCommitted(ctx, route.PartitionID, route.CreationDayUTC, 1, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	db, _ := s.eventsDB(route.CreationDayUTC, route.PartitionID)
	var cnt int
	if err := db.QueryRow(`SELECT count(*) FROM entries`).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("expected 2 unique entries, got %d", cnt)
	}
}

func TestRecoveryReopenWALDatabases(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	stream := domain.StreamRef{TenantID: "t1", SubjectType: "order", StreamKey: "45"}
	var route domain.ChronicleRoute

	{
		s, err := NewStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		route, err = s.EnsureRoute(ctx, stream, domain.PartitionID(hashroute.PartitionForStreamKey(stream.StreamKey)), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		entry := storage.AppendEntry{LSN: 1, EventID: "recover", EventType: "x", EventTimeUTCNs: 1, ReceivedAtUTCNs: 2, PayloadJSON: "{}", Source: "socket", SourceRef: "src", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey}
		if err := s.AppendUncommittedBatch(ctx, route, 1, []storage.AppendEntry{entry}); err != nil {
			t.Fatal(err)
		}
		if err := s.MarkCommitted(ctx, route.PartitionID, route.CreationDayUTC, 1, 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
	}

	s2, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	items, err := s2.GetChronicleByStream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].EventID != "recover" {
		t.Fatalf("unexpected recovered data: %+v, route=%+v", items, route)
	}
}

func TestReadPathsCommitAndVisualOrder(t *testing.T) {
	ctx := context.Background()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stream := domain.StreamRef{TenantID: "t1", SubjectType: "order", StreamKey: "45"}
	route, err := s.EnsureRoute(ctx, stream, domain.PartitionID(hashroute.PartitionForStreamKey(stream.StreamKey)), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	entries := []storage.AppendEntry{
		{LSN: 1, EventID: "e1", EventType: "later-time", EventTimeUTCNs: 200, ReceivedAtUTCNs: 1, PayloadJSON: "{}", Source: "socket", SourceRef: "1", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
		{LSN: 2, EventID: "e2", EventType: "earlier-time", EventTimeUTCNs: 100, ReceivedAtUTCNs: 2, PayloadJSON: "{}", Source: "socket", SourceRef: "2", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
	}
	if err := s.AppendUncommittedBatch(ctx, route, 1, entries); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCommitted(ctx, route.PartitionID, route.CreationDayUTC, 1, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	commitOrder, err := s.GetChronicleByStream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if commitOrder[0].EventID != "e1" || commitOrder[1].EventID != "e2" {
		t.Fatalf("unexpected commit order: %+v", commitOrder)
	}

	visualOrder, err := s.GetChronicleByStreamVisualOrder(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if visualOrder[0].EventID != "e2" || visualOrder[1].EventID != "e1" {
		t.Fatalf("unexpected visual order: %+v", visualOrder)
	}
}

func TestSQLiteWALModeEnabled(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	db, err := s.eventsDB("2026-01-01", 1)
	if err != nil {
		t.Fatal(err)
	}
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode;`).Scan(&mode); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if strings.ToLower(mode) != "wal" {
		t.Fatalf("journal mode must be WAL, got %q", mode)
	}
}

func TestEnsureRouteRejectsMismatchedPartition(t *testing.T) {
	ctx := context.Background()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stream := domain.StreamRef{TenantID: "t1", SubjectType: "order", StreamKey: "Order-45"}
	wrongPartition := domain.PartitionID(0)
	if right := domain.PartitionID(hashroute.PartitionForStreamKey(stream.StreamKey)); right == wrongPartition {
		wrongPartition = 1
	}

	if _, err := s.EnsureRoute(ctx, stream, wrongPartition, time.Now().UTC()); err == nil {
		t.Fatalf("expected partition mismatch error")
	}
}

func TestGetRouteUsesCanonicalizedStreamKey(t *testing.T) {
	ctx := context.Background()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stream := domain.StreamRef{TenantID: "t1", SubjectType: "order", StreamKey: "  Order-45  "}
	partition := domain.PartitionID(hashroute.PartitionForStreamKey(stream.StreamKey))
	route, err := s.EnsureRoute(ctx, stream, partition, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	got, ok, err := s.GetRoute(ctx, domain.StreamRef{TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: "order-45"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected route to exist")
	}
	if got.PartitionID != route.PartitionID || got.CreationDayUTC != route.CreationDayUTC {
		t.Fatalf("unexpected route lookup result: %+v", got)
	}
}

func TestSnapshotRoundTripPartitionMeta(t *testing.T) {
	ctx := context.Background()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.ApplySnapshot(ctx, 3, 42, "2026-01-15"); err != nil {
		t.Fatal(err)
	}
	meta, err := s.CreateSnapshot(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if meta["snapshot_applied_index"] != "42" {
		t.Fatalf("unexpected applied index: %+v", meta)
	}
	if meta["snapshot_watermark_day_utc"] != "2026-01-15" {
		t.Fatalf("unexpected watermark: %+v", meta)
	}
}
