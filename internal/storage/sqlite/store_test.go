package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"chronicles/internal/domain"
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
	route, err := s.EnsureRoute(ctx, stream, 0, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	entry := storage.AppendEntry{LSN: 1, EventID: "e1", EventType: "created", EventTimeUTCNs: 1, ReceivedAtUTCNs: 2, PayloadJSON: "{}", Source: "socket", SourceRef: "a", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey}
	if err := s.AppendCommittedBatch(ctx, route, 1, []storage.AppendEntry{entry}, time.Now().UTC()); err != nil {
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
	route, err := s.EnsureRoute(ctx, stream, 2, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	entries := []storage.AppendEntry{
		{LSN: 1, EventID: "e1", EventType: "created", EventTimeUTCNs: 10, ReceivedAtUTCNs: 20, PayloadJSON: "{}", Source: "socket", SourceRef: "1", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
		{LSN: 2, EventID: "e2", EventType: "paid", EventTimeUTCNs: 11, ReceivedAtUTCNs: 21, PayloadJSON: "{}", Source: "socket", SourceRef: "2", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
	}
	if err := s.AppendCommittedBatch(ctx, route, 3, entries, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendCommittedBatch(ctx, route, 3, []storage.AppendEntry{entries[0]}, time.Now().UTC()); err != nil {
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
		route, err = s.EnsureRoute(ctx, stream, 1, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		entry := storage.AppendEntry{LSN: 1, EventID: "recover", EventType: "x", EventTimeUTCNs: 1, ReceivedAtUTCNs: 2, PayloadJSON: "{}", Source: "socket", SourceRef: "src", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey}
		if err := s.AppendCommittedBatch(ctx, route, 1, []storage.AppendEntry{entry}, time.Now().UTC()); err != nil {
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
	route, err := s.EnsureRoute(ctx, stream, 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	entries := []storage.AppendEntry{
		{LSN: 1, EventID: "e1", EventType: "later-time", EventTimeUTCNs: 200, ReceivedAtUTCNs: 1, PayloadJSON: "{}", Source: "socket", SourceRef: "1", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
		{LSN: 2, EventID: "e2", EventType: "earlier-time", EventTimeUTCNs: 100, ReceivedAtUTCNs: 2, PayloadJSON: "{}", Source: "socket", SourceRef: "2", TenantID: stream.TenantID, SubjectType: stream.SubjectType, StreamKey: stream.StreamKey},
	}
	if err := s.AppendCommittedBatch(ctx, route, 1, entries, time.Now().UTC()); err != nil {
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
