package registry

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Upload session states.
const (
	StateOpen       = "open"
	StateCommitting = "committing"
	StateCommitted  = "committed"
)

// ErrNotImplemented is retained for compatibility with the starting skeleton.
var ErrNotImplemented = errors.New("registry operation not implemented")

const schema = `
CREATE TABLE IF NOT EXISTS uploads (
	id TEXT PRIMARY KEY,
	tenant TEXT NOT NULL,
	object_name TEXT NOT NULL,
	size INTEGER NOT NULL CHECK (size >= 0),
	sha256 TEXT NOT NULL,
	state TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
	upload_id TEXT NOT NULL,
	start_off INTEGER NOT NULL,
	end_off INTEGER NOT NULL,
	length INTEGER NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY (upload_id, start_off, end_off)
);
CREATE TABLE IF NOT EXISTS blobs (
	sha256 TEXT PRIMARY KEY,
	size INTEGER NOT NULL CHECK (size >= 0),
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
	tenant TEXT NOT NULL,
	name TEXT NOT NULL,
	sha256 TEXT NOT NULL,
	size INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (tenant, name)
);
`

// Store owns the SQLite metadata database and content-addressed blob directory.
type Store struct {
	root string
	db   *sql.DB

	// Per-upload locks serialize the open -> committing -> committed state
	// machine for concurrent requests targeting the same upload.
	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

// Open prepares (or reopens) the data directory, metadata database and blob
// store. Reopening an existing directory fully recovers durable state.
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("empty data directory")
	}
	clean, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(clean, "blobs"), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(clean, "tmp"), 0o700); err != nil {
		return nil, err
	}

	dsn := "file:" + filepath.Join(clean, "registry.db") +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=foreign_keys(ON)" +
		"&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection serializes writers; paired with the in-process per-upload
	// locks this keeps state transitions linear.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	s := &Store{root: clean, db: db, locks: make(map[string]*sync.Mutex)}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	// A crash between moving to 'committing' and 'committed' never published an
	// object (the rename is the publish point), so resume such uploads.
	if _, err := db.Exec(`UPDATE uploads SET state = ?, updated_at = ? WHERE state = ?`,
		StateOpen, time.Now().UnixNano(), StateCommitting); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Root reports the absolute data directory.
func (s *Store) Root() string { return s.root }

// Close releases the metadata database.
func (s *Store) Close() error { return s.db.Close() }

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Upload describes one resumable upload session.
type Upload struct {
	ID         string `json:"id"`
	Tenant     string `json:"tenant"`
	ObjectName string `json:"object_name"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	State      string `json:"state"`
	CreatedAt  int64  `json:"created_at,omitempty"`
	UpdatedAt  int64  `json:"updated_at,omitempty"`
}

// CreateUpload validates the request and opens a new resumable upload.
func (s *Store) CreateUpload(ctx context.Context, tenant, objectName, digest string, size int64) (u Upload, err error) {
	if err = ValidateTenant(tenant); err != nil {
		return
	}
	if err = ValidateObjectName(objectName); err != nil {
		return
	}
	if size < 0 {
		err = newError(CodeInvalidArgument, "size must be non-negative")
		return
	}
	if err = ValidateDigest(digest); err != nil {
		return
	}

	var raw [16]byte
	if _, err = rand.Read(raw[:]); err != nil {
		err = wrapError(CodeInternal, "failed to generate upload id", err)
		return
	}
	id := hex.EncodeToString(raw[:])
	now := time.Now().UnixNano()
	u = Upload{
		ID: id, Tenant: tenant, ObjectName: objectName, Size: size,
		SHA256: digest, State: StateOpen, CreatedAt: now, UpdatedAt: now,
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO uploads (id, tenant, object_name, size, sha256, state, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, tenant, objectName, size, digest, StateOpen, now, now)
	if err != nil {
		err = wrapError(CodeInternal, "failed to persist upload", err)
	}
	return
}

func (s *Store) uploadLock(id string) *sync.Mutex {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	m, ok := s.locks[id]
	if !ok {
		m = &sync.Mutex{}
		s.locks[id] = m
	}
	return m
}

type sqlQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func queryUpload(q sqlQueryer, id string) (Upload, error) {
	var u Upload
	err := q.QueryRow(`SELECT id, tenant, object_name, size, sha256, state, created_at, updated_at
	                    FROM uploads WHERE id = ?`, id).
		Scan(&u.ID, &u.Tenant, &u.ObjectName, &u.Size, &u.SHA256, &u.State, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Upload{}, newError(CodeNotFound, "upload not found")
	}
	if err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to read upload", err)
	}
	return u, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// PutChunk stores one raw-binary chunk covering the inclusive [start, end]
// byte range. Identical range + bytes is idempotent; any overlap with
// different bounds or bytes conflicts. Bounds are validated against the
// declared total size before storage.
func (s *Store) PutChunk(ctx context.Context, id string, start, end, total int64, data []byte) error {
	if len(data) == 0 {
		return newError(CodeInvalidArgument, "chunk body must not be empty")
	}
	if int64(len(data)) > MaxChunkSize {
		return newError(CodePayloadTooLarge, "chunk must not exceed 1 MiB")
	}
	if start < 0 || end < start {
		return newError(CodeInvalidArgument, "invalid content range")
	}
	if int64(len(data)) != end-start+1 {
		return newError(CodeInvalidArgument, "content range length does not match body size")
	}

	mu := s.uploadLock(id)
	mu.Lock()
	defer mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapError(CodeInternal, "failed to begin transaction", err)
	}
	defer tx.Rollback()

	u, err := queryUpload(tx, id)
	if err != nil {
		return err
	}
	if u.State != StateOpen {
		return newError(CodeUploadNotOpen, fmt.Sprintf("upload is %s; chunks are no longer accepted", u.State))
	}
	if total != u.Size {
		return newError(CodeInvalidArgument,
			fmt.Sprintf("content range total %d does not match declared size %d", total, u.Size))
	}
	if end >= u.Size {
		return newError(CodeRangeInvalid, "chunk range extends beyond declared size")
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT start_off, end_off, data FROM chunks WHERE upload_id = ? ORDER BY start_off`, id)
	if err != nil {
		return wrapError(CodeInternal, "failed to read chunks", err)
	}
	conflict := false
	for rows.Next() {
		var cs, ce int64
		var cd []byte
		if err := rows.Scan(&cs, &ce, &cd); err != nil {
			rows.Close()
			return wrapError(CodeInternal, "failed to scan chunk", err)
		}
		if cs <= end && ce >= start {
			if cs == start && ce == end && bytesEqual(cd, data) {
				rows.Close()
				return nil // identical retransmission
			}
			conflict = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return wrapError(CodeInternal, "failed to iterate chunks", err)
	}
	if conflict {
		return newError(CodeChunkConflict, "overlapping range with different bytes or bounds")
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO chunks (upload_id, start_off, end_off, length, data) VALUES (?, ?, ?, ?, ?)`,
		id, start, end, len(data), data); err != nil {
		return wrapError(CodeInternal, "failed to store chunk", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE uploads SET updated_at = ? WHERE id = ?`, time.Now().UnixNano(), id); err != nil {
		return wrapError(CodeInternal, "failed to update upload", err)
	}
	if err := tx.Commit(); err != nil {
		return wrapError(CodeInternal, "failed to commit chunk", err)
	}
	return nil
}

// blobPath returns the sharded on-disk path of a digest's physical blob.
func blobPath(root, digest string) string {
	return filepath.Join(root, "blobs", digest[0:2], digest[2:4], digest)
}

// CompleteUpload verifies exact gapless coverage, streams the assembled
// content through SHA-256, publishes via a temp file plus atomic rename and
// commits metadata. The call is retriable and never exposes a half-built
// object: objects become visible only after the atomic rename.
func (s *Store) CompleteUpload(ctx context.Context, id string) (Upload, error) {
	mu := s.uploadLock(id)
	mu.Lock()
	defer mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to begin transaction", err)
	}
	defer tx.Rollback()

	u, err := queryUpload(tx, id)
	if err != nil {
		return Upload{}, err
	}
	switch u.State {
	case StateCommitted:
		// Retried completion of an already published upload.
		return u, nil
	case StateCommitting:
		// A previous attempt published the physical file but failed before the
		// final metadata commit; resume because every later step is idempotent.
	case StateOpen:
	default:
		return Upload{}, newError(CodeUploadNotOpen, fmt.Sprintf("upload is %s", u.State))
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT start_off, end_off, length, data FROM chunks WHERE upload_id = ? ORDER BY start_off`, id)
	if err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to read chunks", err)
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "publish-*")
	if err != nil {
		rows.Close()
		return Upload{}, wrapError(CodeInternal, "failed to create temp file", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(tmpName)
		}
	}()

	var next int64
	var hasher = newSHA256()
	for rows.Next() {
		var cs, ce, length int64
		var data []byte
		if err := rows.Scan(&cs, &ce, &length, &data); err != nil {
			rows.Close()
			tmp.Close()
			return Upload{}, wrapError(CodeInternal, "failed to scan chunk", err)
		}
		if cs != next || ce < cs || length != ce-cs+1 {
			rows.Close()
			tmp.Close()
			return Upload{}, newError(CodeIncomplete, "chunks do not exactly and contiguously cover the declared size")
		}
		if _, err := tmp.Write(data); err != nil {
			rows.Close()
			tmp.Close()
			return Upload{}, wrapError(CodeInternal, "failed to assemble object", err)
		}
		hasher.write(data)
		next = ce + 1
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		tmp.Close()
		return Upload{}, wrapError(CodeInternal, "failed to iterate chunks", err)
	}
	if next != u.Size {
		tmp.Close()
		return Upload{}, newError(CodeIncomplete, "chunks do not exactly and contiguously cover the declared size")
	}

	gotDigest := hasher.hex()
	if gotDigest != u.SHA256 {
		tmp.Close()
		return Upload{}, newError(CodeDigestMismatch, "streamed content does not match declared sha256")
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return Upload{}, wrapError(CodeInternal, "failed to flush object", err)
	}
	if err := tmp.Close(); err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to finish object", err)
	}

	// Move upload to committing inside the same metadata transaction so a
	// concurrent completion cannot double-publish.
	now := time.Now().UnixNano()
	// open and committing (resumed after a failed final commit) are both
	// valid predecessors; anything else lost the state race.
	res, err := tx.ExecContext(ctx,
		`UPDATE uploads SET state = ?, updated_at = ? WHERE id = ? AND state IN (?, ?)`,
		StateCommitting, now, id, StateOpen, StateCommitting)
	if err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to advance upload state", err)
	}
	affected, _ := res.RowsAffected()
	if affected != 1 {
		return Upload{}, newError(CodeUploadNotOpen, "upload is no longer open")
	}
	if err := tx.Commit(); err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to commit uploading state", err)
	}

	// Publish the physical blob with an atomic rename. Content is shared: a
	// digest already on disk keeps its single physical copy.
	dest := blobPath(s.root, gotDigest)
	destExists := false
	if fi, statErr := os.Stat(dest); statErr == nil {
		if fi.Size() != u.Size {
			// Existing path with wrong size is corrupt; fail closed.
			os.Remove(tmpName)
			return Upload{}, newError(CodeDigestMismatch, "existing blob has inconsistent size")
		}
		destExists = true
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return Upload{}, wrapError(CodeInternal, "failed to inspect blob", statErr)
	}
	if !destExists {
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return Upload{}, wrapError(CodeInternal, "failed to create blob directory", err)
		}
		if err := os.Link(tmpName, dest); err != nil {
			if renameErr := os.Rename(tmpName, dest); renameErr != nil {
				return Upload{}, wrapError(CodeInternal, "failed to publish blob", renameErr)
			}
		}
	}
	cleanup = false
	os.Remove(tmpName)

	// Final metadata transaction: register blob/object and mark committed.
	tx2, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to begin transaction", err)
	}
	defer tx2.Rollback()
	now = time.Now().UnixNano()
	if _, err := tx2.ExecContext(ctx,
		`INSERT INTO blobs (sha256, size, created_at) VALUES (?, ?, ?)
		 ON CONFLICT(sha256) DO NOTHING`, gotDigest, u.Size, now); err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to register blob", err)
	}
	if _, err := tx2.ExecContext(ctx,
		`INSERT INTO objects (tenant, name, sha256, size, created_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(tenant, name) DO UPDATE SET sha256 = excluded.sha256, size = excluded.size, created_at = excluded.created_at`,
		u.Tenant, u.ObjectName, gotDigest, u.Size, now); err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to publish object", err)
	}
	res, err = tx2.ExecContext(ctx,
		`UPDATE uploads SET state = ?, updated_at = ? WHERE id = ?`, StateCommitted, now, id)
	if err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to finalize upload", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return Upload{}, newError(CodeNotFound, "upload vanished during completion")
	}
	if err := tx2.Commit(); err != nil {
		return Upload{}, wrapError(CodeInternal, "failed to commit completion", err)
	}
	u.State = StateCommitted
	u.SHA256 = gotDigest
	u.UpdatedAt = now
	return u, nil
}
