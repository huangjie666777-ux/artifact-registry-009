package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// ObjectHandle is a readable, committed object.
type ObjectHandle struct {
	Tenant    string
	Name      string
	Digest    string
	Size      int64
	ModTime   time.Time
	Path      string
	CloseFunc func() error
}

// Close releases resources associated with the handle.
func (o *ObjectHandle) Close() error {
	if o.CloseFunc != nil {
		return o.CloseFunc()
	}
	return nil
}

// OpenObject resolves a committed object under a tenant and opens its
// single shared physical blob for reading.
func (s *Store) OpenObject(ctx context.Context, tenant, name string) (*ObjectHandle, error) {
	if err := ValidateTenant(tenant); err != nil {
		return nil, err
	}
	if err := ValidateObjectName(name); err != nil {
		return nil, err
	}
	var digest string
	var size, createdAt int64
	err := s.db.QueryRowContext(ctx,
		`SELECT sha256, size, created_at FROM objects WHERE tenant = ? AND name = ?`, tenant, name).
		Scan(&digest, &size, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, newError(CodeNotFound, "object not found")
	}
	if err != nil {
		return nil, wrapError(CodeInternal, "failed to read object", err)
	}
	if err := ValidateDigest(digest); err != nil {
		return nil, err
	}
	path := blobPath(s.root, digest)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, newError(CodeNotFound, "object content is unavailable")
		}
		return nil, wrapError(CodeInternal, "failed to open content", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, wrapError(CodeInternal, "failed to stat content", err)
	}
	if fi.Size() != size {
		f.Close()
		return nil, newError(CodeDigestMismatch, "stored content size is inconsistent")
	}
	return &ObjectHandle{
		Tenant: tenant, Name: name, Digest: digest, Size: size,
		ModTime: time.Unix(0, createdAt).UTC(), Path: path, CloseFunc: f.Close,
	}, nil
}

// ReadBlob reads an inclusive [start, end] byte range (or the whole blob when
// both are nil) of a content-addressed blob and verifies the digest exists.
func (s *Store) ReadBlob(ctx context.Context, digest string, start, end *int64) ([]byte, string, error) {
	if err := ValidateDigest(digest); err != nil {
		return nil, "", err
	}
	var size int64
	if err := s.db.QueryRowContext(ctx, `SELECT size FROM blobs WHERE sha256 = ?`, digest).Scan(&size); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", newError(CodeNotFound, "blob not found")
		}
		return nil, "", wrapError(CodeInternal, "failed to read blob metadata", err)
	}
	path := filepath.Join(s.root, "blobs", digest[0:2], digest[2:4], digest)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", newError(CodeNotFound, "blob content is unavailable")
		}
		return nil, "", wrapError(CodeInternal, "failed to open blob", err)
	}
	defer f.Close()

	var off, limit int64
	switch {
	case start == nil && end == nil:
		off, limit = 0, size
	case start != nil && end != nil:
		if *start < 0 || *end < *start || *end >= size {
			return nil, "", newError(CodeRangeNotSatisf, "range is not satisfiable")
		}
		off, limit = *start, *end-*start+1
	default:
		return nil, "", newError(CodeInvalidArgument, "range requires both start and end")
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, "", wrapError(CodeInternal, "failed to seek blob", err)
	}
	buf := make([]byte, limit)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, "", wrapError(CodeInternal, "failed to read blob", err)
	}
	return buf, fmt.Sprintf("%d", size), nil
}

// BlobSize reports the recorded size of a content-addressed blob.
func (s *Store) BlobSize(ctx context.Context, digest string, size *int64) error {
	return s.db.QueryRowContext(ctx, `SELECT size FROM blobs WHERE sha256 = ?`, digest).Scan(size)
}

// BlobFileInfo resolves the physical path and modification time of a blob.
func (s *Store) BlobFileInfo(ctx context.Context, digest string) (string, time.Time, error) {
	var createdAt int64
	if err := s.db.QueryRowContext(ctx, `SELECT created_at FROM blobs WHERE sha256 = ?`, digest).Scan(&createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", time.Time{}, newError(CodeNotFound, "blob not found")
		}
		return "", time.Time{}, wrapError(CodeInternal, "failed to read blob", err)
	}
	path := blobPath(s.root, digest)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", time.Time{}, newError(CodeNotFound, "blob content is unavailable")
		}
		return "", time.Time{}, wrapError(CodeInternal, "failed to stat blob", err)
	}
	return path, time.Unix(0, createdAt).UTC(), nil
}
