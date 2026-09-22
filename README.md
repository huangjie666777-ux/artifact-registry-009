# Content addressed artifact registry

This repository is the starting skeleton for a local HTTP artifact registry. The implementation must keep the public package names in `internal/registry` stable while adding the durable upload state machine and HTTP API described in the task prompt.

The service is intentionally incomplete in the starting commit. `go run ./cmd/registryd -addr 127.0.0.1:0 -data ./var` is the expected local entry point after implementation.
