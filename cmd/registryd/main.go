package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"artifact-registry/internal/httpapi"
	"artifact-registry/internal/registry"

	"github.com/labstack/echo/v4"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	dataDir := flag.String("data", "", "durable data directory (or REGISTRY_DATA)")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
	flag.Parse()

	dir := *dataDir
	if dir == "" {
		dir = os.Getenv("REGISTRY_DATA")
	}
	if dir == "" {
		log.Fatal("data directory must be provided with -data or REGISTRY_DATA")
	}

	store, err := registry.Open(dir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	httpapi.Register(e, store)

	server := &http.Server{
		Addr:              *addr,
		Handler:           e,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       0, // chunk uploads may be slow; bodies are capped at 2 MiB
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("artifact registry listening on %s (data %s)", *addr, store.Root())
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-serverErr:
		log.Printf("server error: %v", err)
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
		_ = server.Close()
	}
	if err := store.Close(); err != nil {
		log.Printf("store close: %v", err)
	}
}
