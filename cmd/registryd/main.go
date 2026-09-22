package main

import (
    "flag"
    "log"
    "net/http"

    "github.com/labstack/echo/v4"
    "artifact-registry/internal/httpapi"
    "artifact-registry/internal/registry"
)

func main() {
    addr := flag.String("addr", "127.0.0.1:8080", "listen address")
    dataDir := flag.String("data", "./var", "durable data directory")
    flag.Parse()

    store, err := registry.Open(*dataDir)
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    e := echo.New()
    httpapi.Register(e, store)
    log.Printf("artifact registry listening on %s", *addr)
    if err := e.Start(*addr); err != nil && err != http.ErrServerClosed {
        log.Fatal(err)
    }
}
