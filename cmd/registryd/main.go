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

	"github.com/labstack/echo/v4"

	"artifact-registry/internal/httpapi"
	"artifact-registry/internal/registry"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	dataDir := flag.String("data", "", "durable data directory (or set REGISTRY_DATA_DIR)")
	shutdownTimeout := flag.Duration("shutdown-timeout", 15*time.Second,
		"maximum time to wait for in-flight requests on shutdown")
	flag.Parse()

	// The data directory may only come from an explicit flag or an explicit
	// environment variable; never fall back to a guessed relative path.
	if *dataDir == "" {
		*dataDir = os.Getenv("REGISTRY_DATA_DIR")
	}
	if *dataDir == "" {
		log.Fatal("missing data directory: pass -data or set REGISTRY_DATA_DIR")
	}

	store, err := registry.Open(*dataDir)
	if err != nil {
		log.Fatalf("open registry: %v", err)
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	httpapi.Register(e, store)

	server := &http.Server{
		Addr:              *addr,
		Handler:           e,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("artifact registry listening on %s (data %s)", *addr, *dataDir)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		log.Printf("server error: %v", err)
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
		_ = server.Close()
	}
	if err := store.Close(); err != nil {
		log.Printf("closing store: %v", err)
	}
	log.Printf("stopped")
}
