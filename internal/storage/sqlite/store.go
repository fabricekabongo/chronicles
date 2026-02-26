package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"chronicles/internal/domain"
	"chronicles/internal/storage"

	_ "modernc.org/sqlite"
)

const (
	catalogSchema = `
CREATE TABLE IF NOT EXISTS chronicle_route_index (
	tenant_id TEXT,
	subject_type TEXT NOT NULL,
	stream_key TEXT NOT NULL,
	partition_id INTEGER NOT NULL,
	creation_day_utc TEXT NOT NULL,
	first_received_at_utc_ns INTEGER NOT NULL,
	last_received_at_utc_ns INTEGER NOT NULL,
	event_count INTEGER NOT NULL DEFAULT 0,
	is_closed INTEGER NOT NULL DEFAULT 0,
	updated_at_utc_ns INTEGER NOT NULL,
	PRIMARY KEY (tenant_id, subject_type, stream_key)
);

CREATE TABLE IF NOT EXISTS partition_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`
	eventsSchema = `
CREATE TABLE IF NOT EXISTS entries (
	partition_id INTEGER NOT NULL,
	creation_day_utc TEXT NOT NULL,
	lsn INTEGER NOT NULL,
	term INTEGER NOT NULL,
	tenant_id TEXT,
	subject_type TEXT NOT NULL,
	stream_key TEXT NOT NULL,
	event_no INTEGER,
	event_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	event_time_utc_ns INTEGER NOT NULL,
	received_at_utc_ns INTEGER NOT NULL,
	committed_at_utc_ns INTEGER,
	payload_json TEXT NOT NULL,
	payload_encoding TEXT NOT NULL DEFAULT 'json',
	source TEXT NOT NULL,
	source_ref TEXT NOT NULL,
	record_hash BLOB,
	prev_stream_hash BLOB,
	committed INTEGER NOT NULL DEFAULT 0,
	UNIQUE(partition_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_entries_stream_lsn ON entries(tenant_id, subject_type, stream_key, lsn);
CREATE INDEX IF NOT EXISTS idx_entries_stream_event_time_lsn ON entries(tenant_id, subject_type, stream_key, event_time_utc_ns, lsn);
CREATE INDEX IF NOT EXISTS idx_entries_partition_commit_lsn ON entries(partition_id, committed, lsn);

CREATE TRIGGER IF NOT EXISTS trg_entries_no_update
BEFORE UPDATE ON entries
BEGIN
	SELECT RAISE(ABORT, 'entries are append-only: UPDATE forbidden');
END;

CREATE TRIGGER IF NOT EXISTS trg_entries_no_delete
BEFORE DELETE ON entries
BEGIN
	SELECT RAISE(ABORT, 'entries are append-only: DELETE forbidden');
END;
`
)

type Store struct {
	baseDir string

	mu       sync.Mutex
	catalogs map[domain.PartitionID]*sql.DB
	events   map[string]*sql.DB
}

func NewStore(baseDir string) (*Store, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir base dir: %w", err)
	}
	return &Store{baseDir: baseDir, catalogs: make(map[domain.PartitionID]*sql.DB), events: make(map[string]*sql.DB)}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for _, db := range s.catalogs {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, db := range s.events {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Store) EnsureRoute(ctx context.Context, stream domain.StreamRef, partitionID domain.PartitionID, receivedAt time.Time) (domain.ChronicleRoute, error) {
	db, err := s.catalogDB(partitionID)
	if err != nil {
		return domain.ChronicleRoute{}, err
	}
	now := receivedAt.UTC().UnixNano()
	creationDay := receivedAt.UTC().Format(time.DateOnly)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ChronicleRoute{}, err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
INSERT INTO chronicle_route_index(
	tenant_id, subject_type, stream_key, partition_id, creation_day_utc,
	first_received_at_utc_ns, last_received_at_utc_ns, updated_at_utc_ns
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(tenant_id, subject_type, stream_key)
DO UPDATE SET last_received_at_utc_ns=excluded.last_received_at_utc_ns, updated_at_utc_ns=excluded.updated_at_utc_ns`,
		stream.TenantID, stream.SubjectType, stream.StreamKey, int(partitionID), creationDay, now, now, now)
	if err != nil {
		return domain.ChronicleRoute{}, err
	}

	route, ok, err := getRouteTx(ctx, tx, stream)
	if err != nil {
		return domain.ChronicleRoute{}, err
	}
	if !ok {
		return domain.ChronicleRoute{}, fmt.Errorf("route missing after upsert")
	}
	if err := tx.Commit(); err != nil {
		return domain.ChronicleRoute{}, err
	}
	return route, nil
}

func (s *Store) GetRoute(ctx context.Context, stream domain.StreamRef) (domain.ChronicleRoute, bool, error) {
	// deterministic partition lookup by searching all catalogs in v1 skeleton
	for p := 0; p < 25; p++ {
		db, err := s.catalogDB(domain.PartitionID(p))
		if err != nil {
			return domain.ChronicleRoute{}, false, err
		}
		route, ok, err := getRouteDB(ctx, db, stream)
		if err != nil {
			return domain.ChronicleRoute{}, false, err
		}
		if ok {
			return route, true, nil
		}
	}
	return domain.ChronicleRoute{}, false, nil
}

func (s *Store) AppendCommittedBatch(ctx context.Context, route domain.ChronicleRoute, term uint64, entries []storage.AppendEntry, committedAt time.Time) error {
	db, err := s.eventsDB(route.CreationDayUTC, route.PartitionID)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, e := range entries {
		_, err := tx.ExecContext(ctx, `
INSERT INTO entries(
	partition_id, creation_day_utc, lsn, term,
	tenant_id, subject_type, stream_key, event_no,
	event_id, event_type, event_time_utc_ns,
	received_at_utc_ns, committed_at_utc_ns,
	payload_json, payload_encoding, source, source_ref,
	record_hash, prev_stream_hash, committed
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(partition_id, event_id) DO NOTHING`,
			int(route.PartitionID), route.CreationDayUTC, int64(e.LSN), int64(term),
			e.TenantID, e.SubjectType, e.StreamKey, nullableUint64(e.EventNo),
			e.EventID, e.EventType, e.EventTimeUTCNs,
			e.ReceivedAtUTCNs, committedAt.UTC().UnixNano(),
			e.PayloadJSON, emptyToDefault(e.PayloadEncoding, "json"), e.Source, e.SourceRef,
			e.RecordHash, e.PrevStreamHash)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	catalog, err := s.catalogDB(route.PartitionID)
	if err != nil {
		return err
	}
	_, err = catalog.ExecContext(ctx, `
UPDATE chronicle_route_index
SET event_count = event_count + ?, last_received_at_utc_ns=?, updated_at_utc_ns=?
WHERE tenant_id IS ? AND subject_type=? AND stream_key=?`,
		len(entries), committedAt.UTC().UnixNano(), committedAt.UTC().UnixNano(), route.TenantID, route.SubjectType, route.StreamKey)
	return err
}

func (s *Store) MarkCommitted(ctx context.Context, partitionID domain.PartitionID, creationDayUTC string, lsnFrom, lsnTo uint64, committedAt time.Time) error {
	db, err := s.eventsDB(creationDayUTC, partitionID)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `UPDATE entries SET committed=1, committed_at_utc_ns=? WHERE partition_id=? AND lsn BETWEEN ? AND ?`,
		committedAt.UTC().UnixNano(), int(partitionID), int64(lsnFrom), int64(lsnTo))
	return err
}

func (s *Store) GetChronicleByStream(ctx context.Context, stream domain.StreamRef) ([]storage.AppendEntry, error) {
	return s.getChronicle(ctx, stream, storage.SortCommitOrder)
}

func (s *Store) GetChronicleByStreamVisualOrder(ctx context.Context, stream domain.StreamRef) ([]storage.AppendEntry, error) {
	return s.getChronicle(ctx, stream, storage.SortVisualOrder)
}

func (s *Store) getChronicle(ctx context.Context, stream domain.StreamRef, sort storage.QuerySort) ([]storage.AppendEntry, error) {
	route, ok, err := s.GetRoute(ctx, stream)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	db, err := s.eventsDB(route.CreationDayUTC, route.PartitionID)
	if err != nil {
		return nil, err
	}
	orderBy := "lsn ASC"
	if sort == storage.SortVisualOrder {
		orderBy = "event_time_utc_ns ASC, lsn ASC"
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT lsn, term, tenant_id, subject_type, stream_key, event_no, event_id, event_type,
	event_time_utc_ns, received_at_utc_ns, payload_json, payload_encoding, source, source_ref,
	record_hash, prev_stream_hash
FROM entries
WHERE tenant_id IS ? AND subject_type=? AND stream_key=?
ORDER BY %s`, orderBy), stream.TenantID, stream.SubjectType, stream.StreamKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []storage.AppendEntry
	for rows.Next() {
		var item storage.AppendEntry
		var eventNo sql.NullInt64
		if err := rows.Scan(
			&item.LSN, &item.Term, &item.TenantID, &item.SubjectType, &item.StreamKey, &eventNo, &item.EventID, &item.EventType,
			&item.EventTimeUTCNs, &item.ReceivedAtUTCNs, &item.PayloadJSON, &item.PayloadEncoding, &item.Source, &item.SourceRef,
			&item.RecordHash, &item.PrevStreamHash,
		); err != nil {
			return nil, err
		}
		if eventNo.Valid {
			v := uint64(eventNo.Int64)
			item.EventNo = &v
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) catalogDB(partitionID domain.PartitionID) (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if db, ok := s.catalogs[partitionID]; ok {
		return db, nil
	}
	path := filepath.Join(s.baseDir, fmt.Sprintf("catalog-p%02d.db", partitionID))
	db, err := openSQLite(path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(catalogSchema); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.catalogs[partitionID] = db
	return db, nil
}

func (s *Store) eventsDB(day string, partitionID domain.PartitionID) (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := fmt.Sprintf("%s-p%02d", day, partitionID)
	if db, ok := s.events[k]; ok {
		return db, nil
	}
	path := filepath.Join(s.baseDir, fmt.Sprintf("events-%s-p%02d.db", day, partitionID))
	db, err := openSQLite(path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(eventsSchema); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.events[k] = db
	return db, nil
}

func openSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=FULL;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func getRouteDB(ctx context.Context, db *sql.DB, stream domain.StreamRef) (domain.ChronicleRoute, bool, error) {
	row := db.QueryRowContext(ctx, `
SELECT tenant_id, subject_type, stream_key, partition_id, creation_day_utc,
	first_received_at_utc_ns, last_received_at_utc_ns
FROM chronicle_route_index
WHERE tenant_id IS ? AND subject_type=? AND stream_key=?`,
		stream.TenantID, stream.SubjectType, stream.StreamKey)
	var route domain.ChronicleRoute
	var p int
	err := row.Scan(&route.TenantID, &route.SubjectType, &route.StreamKey, &p, &route.CreationDayUTC, &route.FirstReceivedAtUTCNs, &route.LastReceivedAtUTCNs)
	if err == sql.ErrNoRows {
		return domain.ChronicleRoute{}, false, nil
	}
	if err != nil {
		return domain.ChronicleRoute{}, false, err
	}
	route.PartitionID = domain.PartitionID(p)
	return route, true, nil
}

func getRouteTx(ctx context.Context, tx *sql.Tx, stream domain.StreamRef) (domain.ChronicleRoute, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT tenant_id, subject_type, stream_key, partition_id, creation_day_utc,
	first_received_at_utc_ns, last_received_at_utc_ns
FROM chronicle_route_index
WHERE tenant_id IS ? AND subject_type=? AND stream_key=?`,
		stream.TenantID, stream.SubjectType, stream.StreamKey)
	var route domain.ChronicleRoute
	var p int
	err := row.Scan(&route.TenantID, &route.SubjectType, &route.StreamKey, &p, &route.CreationDayUTC, &route.FirstReceivedAtUTCNs, &route.LastReceivedAtUTCNs)
	if err == sql.ErrNoRows {
		return domain.ChronicleRoute{}, false, nil
	}
	if err != nil {
		return domain.ChronicleRoute{}, false, err
	}
	route.PartitionID = domain.PartitionID(p)
	return route, true, nil
}

func nullableUint64(v *uint64) any {
	if v == nil {
		return nil
	}
	return int64(*v)
}

func emptyToDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
