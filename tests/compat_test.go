package tests

import (
    "context"
    "testing"

    "artifact-registry/internal/registry"
)

func TestStartingStoreCanOpen(t *testing.T) {
    store, err := registry.Open(t.TempDir())
    if err != nil {
        t.Fatal(err)
    }
    defer store.Close()
    if err := store.Ping(context.Background()); err != nil {
        t.Fatal(err)
    }
}
