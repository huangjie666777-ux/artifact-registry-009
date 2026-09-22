package registry

import (
    "context"
    "errors"
    "path/filepath"
    "sync"
)

var ErrNotImplemented = errors.New("registry operation not implemented")

// Store owns the SQLite metadata database and content-addressed blob directory.
// The concrete schema and transaction behavior are part of the task.
type Store struct {
    mu   sync.RWMutex
    root string
}

func Open(root string) (*Store, error) {
    if root == "" {
        return nil, errors.New("empty data directory")
    }
    return &Store{root: filepath.Clean(root)}, nil
}

func (s *Store) Close() error { return nil }

func (s *Store) Ping(ctx context.Context) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    return nil
}

// Upload describes one resumable upload session.
type Upload struct {
    ID         string `json:"id"`
    Tenant     string `json:"tenant"`
    ObjectName string `json:"object_name"`
    Size       int64  `json:"size"`
    SHA256     string `json:"sha256"`
    State      string `json:"state"`
}

func (s *Store) CreateUpload(ctx context.Context, tenant, objectName, digest string, size int64) (Upload, error) {
    return Upload{}, ErrNotImplemented
}

func (s *Store) PutChunk(ctx context.Context, id string, start, end, total int64, data []byte) error {
    return ErrNotImplemented
}

func (s *Store) CompleteUpload(ctx context.Context, id string) (Upload, error) {
    return Upload{}, ErrNotImplemented
}

func (s *Store) ReadBlob(ctx context.Context, digest string, start, end *int64) ([]byte, string, error) {
    return nil, "", ErrNotImplemented
}
