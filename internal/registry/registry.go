package registry

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	StateOpen       = "open"
	StateCommitting = "committing"
	StateCommitted  = "committed"
	StateConflict   = "conflict"

	MaxChunkSize    = int64(1 << 20)
	MaxTenantLength = 128
	MaxNameLength   = 1024
)

var (
	ErrNotFound           = errors.New("not found")
	ErrConflict           = errors.New("conflict")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrDigestMismatch     = errors.New("digest mismatch")
	ErrRangeUnsatisfiable = errors.New("range not satisfiable")
)

var (
	tenantRe  = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
	nameSegRe = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]*$")
	shaRe     = regexp.MustCompile("^[0-9a-f]{64}$")
)

// Store owns the SQLite metadata database and content-addressed blob directory.
type Store struct {
	root string
	db   *sql.DB

	mu    sync.Mutex
	locks map[string]*uploadLock

	refsWG sync.WaitGroup
	closed chan struct{}
}

type uploadLock struct {
	mu   sync.Mutex
	refs int
}

// Open opens (or creates) a registry backed by dataDir. The directory contains
// registry.db plus the blobs/ and staging/ subdirectories.
func Open(dataDir string) (_ *Store, err error) {
	if dataDir == "" {
		return nil, errors.New("empty data directory")
	}
	root := filepath.Clean(dataDir)
	if err := os.MkdirAll(filepath.Join(root, "blobs"), 0o750); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "staging"), 0o700); err != nil {
		return nil, err
	}

	dsn := "file:" + filepath.Join(root, "registry.db") +
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	s := &Store{
		root:   root,
		db:     db,
		locks:  make(map[string]*uploadLock),
		closed: make(chan struct{}),
	}
	if err := s.init(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.recoverStaging(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close stops accepting coordinated work, waits for in-flight upload
// operations, and closes the database.
func (s *Store) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	s.refsWG.Wait()
	return s.db.Close()
}

// Ping verifies database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

const schemaUploads = "CREATE TABLE IF NOT EXISTS uploads (" +
	"id TEXT PRIMARY KEY," +
	"tenant TEXT NOT NULL," +
	"object_name TEXT NOT NULL," +
	"size INTEGER NOT NULL CHECK (size >= 0)," +
	"sha256 TEXT NOT NULL," +
	"state TEXT NOT NULL," +
	"created_at INTEGER NOT NULL," +
	"updated_at INTEGER NOT NULL)"

const schemaChunks = "CREATE TABLE IF NOT EXISTS chunks (" +
	"upload_id TEXT NOT NULL REFERENCES uploads(id)," +
	"start_off INTEGER NOT NULL," +
	"end_off INTEGER NOT NULL," +
	"PRIMARY KEY (upload_id, start_off))"

const schemaObjects = "CREATE TABLE IF NOT EXISTS objects (" +
	"tenant TEXT NOT NULL," +
	"name TEXT NOT NULL," +
	"digest TEXT NOT NULL," +
	"size INTEGER NOT NULL," +
	"updated_at INTEGER NOT NULL," +
	"PRIMARY KEY (tenant, name))"

func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		schemaUploads,
		schemaChunks,
		schemaObjects,
		"CREATE INDEX IF NOT EXISTS idx_chunks_upload ON chunks(upload_id, start_off, end_off)",
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// recoverStaging resets uploads interrupted while committing and removes staged
// chunk files whose chunk row never committed.
func (s *Store) recoverStaging(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx,
		"UPDATE uploads SET state = ?, updated_at = ? WHERE state = ?",
		StateOpen, time.Now().UnixNano(), StateCommitting); err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "staging"))
	if err != nil {
		return err
	}
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasSuffix(name, ".part") {
			continue
		}
		base := strings.TrimSuffix(name, ".part")
		dash := strings.IndexByte(base, '-')
		if dash <= 0 {
			os.Remove(filepath.Join(s.root, "staging", name))
			continue
		}
		id := base[:dash]
		var start int64
		if _, err := fmt.Sscanf(base[dash+1:], "%d", &start); err != nil {
			os.Remove(filepath.Join(s.root, "staging", name))
			continue
		}
		var n int
		if err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM chunks WHERE upload_id = ? AND start_off = ?",
			id, start).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			os.Remove(filepath.Join(s.root, "staging", name))
		}
	}
	return nil
}

// ValidateTenant reports whether tenant is a safe per-tenant identifier.
func ValidateTenant(tenant string) bool {
	return len(tenant) <= MaxTenantLength && tenantRe.MatchString(tenant)
}

// ValidateObjectName reports whether name is a clean, non-traversing path.
func ValidateObjectName(name string) bool {
	if name == "" || len(name) > MaxNameLength {
		return false
	}
	if filepath.IsAbs(name) || strings.Contains(name, "\\") {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(name))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
		return false
	}
	for _, seg := range strings.Split(clean, "/") {
		if seg == "" || seg == "." || seg == ".." || !nameSegRe.MatchString(seg) {
			return false
		}
	}
	return true
}

// ValidateDigest reports whether digest is a 64-char lowercase hex SHA-256.
func ValidateDigest(digest string) bool {
	return shaRe.MatchString(digest)
}

// lockUpload serializes operations on a single upload across goroutines.
func (s *Store) lockUpload(id string) func() {
	s.mu.Lock()
	l, ok := s.locks[id]
	if !ok {
		l = &uploadLock{}
		s.locks[id] = l
	}
	l.refs++
	s.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		s.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(s.locks, id)
		}
		s.mu.Unlock()
	}
}

func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Upload describes one resumable upload session.
type Upload struct {
	ID         string "json:\"id\""
	Tenant     string "json:\"tenant\""
	ObjectName string "json:\"object_name\""
	Size       int64  "json:\"size\""
	SHA256     string "json:\"sha256\""
	State      string "json:\"state\""
}

const uploadCols = "id, tenant, object_name, size, sha256, state, created_at, updated_at"

type rowScanner interface {
	Scan(...any) error
}

func scanUpload(row rowScanner) (Upload, error) {
	var u Upload
	var created, updated int64
	if err := row.Scan(&u.ID, &u.Tenant, &u.ObjectName, &u.Size, &u.SHA256,
		&u.State, &created, &updated); err != nil {
		return Upload{}, err
	}
	return u, nil
}

func (s *Store) getUpload(ctx context.Context, tx *sql.Tx, id string) (Upload, error) {
	u, err := scanUpload(tx.QueryRowContext(ctx,
		"SELECT "+uploadCols+" FROM uploads WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return Upload{}, ErrNotFound
	}
	return u, err
}

// CreateUpload starts a new resumable upload session.
func (s *Store) CreateUpload(ctx context.Context, tenant, objectName, digest string, size int64) (Upload, error) {
	if !ValidateTenant(tenant) {
		return Upload{}, fmt.Errorf("%w: invalid tenant", ErrInvalidArgument)
	}
	if !ValidateObjectName(objectName) {
		return Upload{}, fmt.Errorf("%w: invalid object_name", ErrInvalidArgument)
	}
	if !ValidateDigest(digest) {
		return Upload{}, fmt.Errorf("%w: invalid sha256", ErrInvalidArgument)
	}
	if size < 0 {
		return Upload{}, fmt.Errorf("%w: negative size", ErrInvalidArgument)
	}
	id, err := newUploadID()
	if err != nil {
		return Upload{}, err
	}
	now := time.Now().UnixNano()
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO uploads (id, tenant, object_name, size, sha256, state, created_at, updated_at)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		id, tenant, objectName, size, digest, StateOpen, now, now); err != nil {
		return Upload{}, err
	}
	return Upload{
		ID:         id,
		Tenant:     tenant,
		ObjectName: objectName,
		Size:       size,
		SHA256:     digest,
		State:      StateOpen,
	}, nil
}

func (s *Store) stagingPath(id string, start int64) string {
	return filepath.Join(s.root, "staging", fmt.Sprintf("%s-%d.part", id, start))
}

// PutChunk stores one uploaded byte range. start and end are inclusive byte
// offsets of an object of length total. Re-sending the same range with the
// same bytes is idempotent; overlapping ranges or different bytes fail with
// ErrConflict.
func (s *Store) PutChunk(ctx context.Context, id string, start, end, total int64, data []byte) error {
	if start < 0 || end < start || total < 0 || end >= total {
		return fmt.Errorf("%w: range out of bounds", ErrInvalidArgument)
	}
	length := end - start + 1
	if length != int64(len(data)) {
		return fmt.Errorf("%w: content length does not match range", ErrInvalidArgument)
	}
	if length > MaxChunkSize {
		return fmt.Errorf("%w: chunk exceeds 1MiB", ErrInvalidArgument)
	}

	s.refsWG.Add(1)
	defer s.refsWG.Done()
	if err := ctx.Err(); err != nil {
		return err
	}

	unlock := s.lockUpload(id)
	defer unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	u, err := s.getUpload(ctx, tx, id)
	if err != nil {
		return err
	}
	if total != u.Size {
		return fmt.Errorf("%w: total size does not match upload metadata", ErrInvalidArgument)
	}
	if u.State != StateOpen {
		return fmt.Errorf("%w: upload is %s", ErrConflict, u.State)
	}

	finalPath := s.stagingPath(id, start)

	var existingEnd int64
	err = tx.QueryRowContext(ctx,
		"SELECT end_off FROM chunks WHERE upload_id = ? AND start_off = ?",
		id, start).Scan(&existingEnd)
	switch {
	case err == nil:
		// Idempotent retry: the range and bytes must be identical.
		if existingEnd != end {
			return fmt.Errorf("%w: conflicting chunk range", ErrConflict)
		}
		old, readErr := os.ReadFile(finalPath)
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(old, data) {
			return fmt.Errorf("%w: chunk content differs on retry", ErrConflict)
		}
		return nil
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}

	var overlaps int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM chunks WHERE upload_id = ? AND start_off <= ? AND end_off >= ?",
		id, end, start).Scan(&overlaps); err != nil {
		return err
	}
	if overlaps > 0 {
		return fmt.Errorf("%w: overlapping chunk", ErrConflict)
	}

	// Spool to a temp file, fsync, then atomically rename into place. The
	// chunk row is committed only after the bytes are durably on disk.
	tmp, err := os.CreateTemp(s.root, "chunk-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		"INSERT INTO chunks (upload_id, start_off, end_off) VALUES (?, ?, ?)",
		id, start, end); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// chunk is one stored inclusive byte range.
type chunk struct {
	start int64
	end   int64
}

// tilesExactly reports whether chunks are a sorted exact cover of [0,size).
func tilesExactly(chunks []chunk, size int64) bool {
	if size == 0 {
		return len(chunks) == 0
	}
	if len(chunks) == 0 || chunks[0].start != 0 || chunks[len(chunks)-1].end != size-1 {
		return false
	}
	for i, c := range chunks {
		if c.end < c.start {
			return false
		}
		if i > 0 && c.start != chunks[i-1].end+1 {
			return false
		}
	}
	return true
}

func (s *Store) listChunks(ctx context.Context, tx *sql.Tx, id string) ([]chunk, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT start_off, end_off FROM chunks WHERE upload_id = ? ORDER BY start_off", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []chunk
	for rows.Next() {
		var c chunk
		if err := rows.Scan(&c.start, &c.end); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// verifyAssembly streams the staged chunks in order, hashes the content and
// writes it to a temp file in blobs. It returns the temp path and digest.
func (s *Store) verifyAssembly(ctx context.Context, u Upload, chunks []chunk) (string, string, error) {
	tmp, err := os.CreateTemp(filepath.Join(s.root, "blobs"), "assemble-*.tmp")
	if err != nil {
		return "", "", err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()

	h := sha256.New()
	w := io.MultiWriter(tmp, h)
	buf := make([]byte, 64*1024)
	for _, c := range chunks {
		f, err := os.Open(s.stagingPath(u.ID, c.start))
		if err != nil {
			return "", "", err
		}
		want := c.end - c.start + 1
		n, err := io.CopyBuffer(w, io.LimitReader(f, want), buf)
		if err != nil {
			f.Close()
			return "", "", err
		}
		if n != want {
			f.Close()
			return "", "", fmt.Errorf("%w: staged chunk is short", ErrConflict)
		}
		extra, copyErr := io.Copy(io.Discard, f)
		f.Close()
		if copyErr != nil {
			return "", "", copyErr
		}
		if extra > 0 {
			return "", "", fmt.Errorf("%w: staged chunk is larger than metadata", ErrConflict)
		}
	}
	if err := tmp.Sync(); err != nil {
		return "", "", err
	}
	if err := tmp.Close(); err != nil {
		return "", "", err
	}

	digest := hex.EncodeToString(h.Sum(nil))
	if digest != u.SHA256 {
		return "", "", fmt.Errorf("%w: expected %s got %s", ErrDigestMismatch, u.SHA256, digest)
	}
	ok = true
	return tmpName, digest, nil
}

func (s *Store) blobPath(digest string) string {
	return filepath.Join(s.root, "blobs", digest[:2], digest)
}

// CompleteUpload finalizes an upload: chunks must tile [0,size) exactly, the
// streamed SHA-256 must match the declared digest, and the object is then
// published atomically. It is safe to retry: a completed upload returns the
// same result, and concurrent completion runs are serialized.
func (s *Store) CompleteUpload(ctx context.Context, id string) (Upload, error) {
	s.refsWG.Add(1)
	defer s.refsWG.Done()
	if err := ctx.Err(); err != nil {
		return Upload{}, err
	}

	unlock := s.lockUpload(id)
	defer unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Upload{}, err
	}
	defer tx.Rollback()

	u, err := s.getUpload(ctx, tx, id)
	if err != nil {
		return Upload{}, err
	}
	if u.State == StateCommitted {
		return u, nil
	}
	if u.State != StateOpen {
		return Upload{}, fmt.Errorf("%w: upload is %s", ErrConflict, u.State)
	}

	chunks, err := s.listChunks(ctx, tx, id)
	if err != nil {
		return Upload{}, err
	}
	if !tilesExactly(chunks, u.Size) {
		return Upload{}, fmt.Errorf("%w: chunks do not exactly cover object size", ErrConflict)
	}

	// Claim the committing state first so concurrent completions wait and a
	// crash leaves a recoverable (not half-published) upload.
	if _, err := tx.ExecContext(ctx,
		"UPDATE uploads SET state = ?, updated_at = ? WHERE id = ? AND state = ?",
		StateCommitting, time.Now().UnixNano(), id, StateOpen); err != nil {
		return Upload{}, err
	}
	u.State = StateCommitting
	if err := tx.Commit(); err != nil {
		return Upload{}, err
	}

	tmpName, digest, verifyErr := s.verifyAssembly(ctx, u, chunks)
	if verifyErr != nil {
		s.markConflict(context.Background(), id)
		return Upload{}, verifyErr
	}

	finalPath := s.blobPath(digest)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
		os.Remove(tmpName)
		s.markConflict(context.Background(), id)
		return Upload{}, err
	}
	// Content-addressed storage: a rename that fails because the blob
	// already exists is fine; the identical content is already present.
	if err := os.Rename(tmpName, finalPath); err != nil {
		if !os.IsExist(err) {
			if _, statErr := os.Stat(finalPath); statErr != nil {
				os.Remove(tmpName)
				s.markConflict(context.Background(), id)
				return Upload{}, err
			}
		}
		os.Remove(tmpName)
	}

	pubTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.markConflict(context.Background(), id)
		return Upload{}, err
	}
	defer pubTx.Rollback()

	now := time.Now().UnixNano()
	var state string
	pubErr := pubTx.QueryRowContext(ctx, "SELECT state FROM uploads WHERE id = ?", id).Scan(&state)
	if pubErr != nil {
		s.markConflict(context.Background(), id)
		return Upload{}, pubErr
	}
	if state == StateCommitted {
		pubTx.Rollback()
		return s.completedUpload(ctx, id)
	}

	if _, err := pubTx.ExecContext(ctx,
		"INSERT INTO objects (tenant, name, digest, size, updated_at) VALUES (?, ?, ?, ?, ?)"+
			" ON CONFLICT(tenant, name) DO UPDATE SET digest = excluded.digest,"+
			" size = excluded.size, updated_at = excluded.updated_at",
		u.Tenant, u.ObjectName, digest, u.Size, now); err != nil {
		s.markConflict(context.Background(), id)
		return Upload{}, err
	}
	if _, err := pubTx.ExecContext(ctx,
		"UPDATE uploads SET state = ?, updated_at = ? WHERE id = ?",
		StateCommitted, now, id); err != nil {
		s.markConflict(context.Background(), id)
		return Upload{}, err
	}
	if err := pubTx.Commit(); err != nil {
		s.markConflict(context.Background(), id)
		return Upload{}, err
	}

	// Staged bytes are no longer needed; removal failures are harmless.
	for _, c := range chunks {
		os.Remove(s.stagingPath(id, c.start))
	}
	u.State = StateCommitted
	return u, nil
}

// markConflict moves a failed finalization to the terminal conflict state. It
// never regresses a committed upload.
func (s *Store) markConflict(ctx context.Context, id string) {
	s.db.ExecContext(ctx,
		"UPDATE uploads SET state = ?, updated_at = ? WHERE id = ? AND state != ?",
		StateConflict, time.Now().UnixNano(), id, StateCommitted)
}

func (s *Store) completedUpload(ctx context.Context, id string) (Upload, error) {
	u, err := scanUpload(s.db.QueryRowContext(ctx,
		"SELECT "+uploadCols+" FROM uploads WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return Upload{}, ErrNotFound
	}
	return u, err
}

// Object is a stored object's current metadata.
type Object struct {
	Tenant string
	Name   string
	Digest string
	Size   int64
}

// LookupObject returns the current object metadata for a tenant/name pair.
func (s *Store) LookupObject(ctx context.Context, tenant, name string) (Object, error) {
	if !ValidateTenant(tenant) || !ValidateObjectName(name) {
		return Object{}, fmt.Errorf("%w: invalid object reference", ErrInvalidArgument)
	}
	var o Object
	err := s.db.QueryRowContext(ctx,
		"SELECT tenant, name, digest, size FROM objects WHERE tenant = ? AND name = ?",
		tenant, name).Scan(&o.Tenant, &o.Name, &o.Digest, &o.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	return o, err
}

// BlobReader opens a blob by digest. The caller must close the returned file.
func (s *Store) BlobReader(ctx context.Context, digest string) (*os.File, int64, error) {
	if !ValidateDigest(digest) {
		return nil, 0, fmt.Errorf("%w: invalid digest", ErrInvalidArgument)
	}
	path := s.blobPath(digest)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// ReadBlob returns the requested inclusive byte range of a blob. A nil start
// reads the whole blob. It exists for programmatic consumers; the HTTP layer
// streams via BlobReader instead.
func (s *Store) ReadBlob(ctx context.Context, digest string, start, end *int64) ([]byte, string, error) {
	f, size, err := s.BlobReader(ctx, digest)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()

	var lo, hi int64
	if start == nil && end == nil {
		lo, hi = 0, size-1
	} else {
		if start == nil || end == nil || *start < 0 || *end < *start || *end >= size {
			return nil, "", fmt.Errorf("%w: invalid range", ErrRangeUnsatisfiable)
		}
		lo, hi = *start, *end
	}
	if size == 0 {
		return []byte{}, "application/octet-stream", nil
	}
	out := make([]byte, hi-lo+1)
	if _, err := f.ReadAt(out, lo); err != nil && !errors.Is(err, io.EOF) {
		return nil, "", err
	}
	return out, "application/octet-stream", nil
}
