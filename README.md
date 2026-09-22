# Content addressed artifact registry

A local, content-addressed artifact repository exposed over HTTP. Metadata is
stored in SQLite (pure-Go `modernc.org/sqlite`), object bytes live under
`<data-dir>/blobs` and identical SHA-256 digests share a single physical copy.
Uploads are resumable chunked sessions with an `open -> committing -> committed`
state machine; objects are published via a temp file and atomic rename, so no
partially assembled object is ever visible.

## Run

The data directory must be supplied explicitly by flag or environment variable:

```sh
go run ./cmd/registryd -addr 127.0.0.1:8080 -data ./var
# or
REGISTRY_DATA=/srv/registry go run ./cmd/registryd -addr 127.0.0.1:8080
```

Flags:

- `-addr` listen address (default `127.0.0.1:8080`, loopback-only by default)
- `-data` durable data directory (required, or `REGISTRY_DATA`)
- `-shutdown-timeout` graceful shutdown budget (default `10s`)

SIGINT/SIGTERM trigger graceful shutdown: in-flight requests are drained, the
HTTP server is stopped and SQLite is closed. Reopening an existing data
directory recovers all metadata; uploads interrupted mid-completion resume from
`open`.

Layout:

```
<data-dir>/registry.db      # SQLite metadata (uploads, chunks, blobs, objects)
<data-dir>/blobs/xx/yy/<64-hex-sha256>   # sharded, content-addressed bytes
<data-dir>/tmp/             # staging files, removed on failure
```

## Core API

All errors use the stable JSON shape `{"error":{"code":...,"message":...}}`.

### Health

```sh
curl -s http://127.0.0.1:8080/healthz
# {"status":"ok"}
```

### 1. Create an upload

`tenant` (`[A-Za-z0-9._-]`, max 64), `object_name` (slash-separated, no `.`/
`..` or empty segments, max 1024), non-negative `size`, and a 64-char lowercase
SHA-256 hex digest are validated up front.

```sh
SIZE=$(stat -c %s artifact.bin)
SHA=$(sha256sum artifact.bin | cut -d' ' -f1)
UP=$(curl -s -X POST http://127.0.0.1:8080/v1/uploads \
  -H 'Content-Type: application/json' \
  -d "{\"tenant\":\"team-a\",\"object_name\":\"releases/artifact.bin\",\"size\":$SIZE,\"sha256\":\"$SHA\"}")
ID=$(echo "$UP" | sed 's/.*"id":"\([^"]*\)".*/\1/')
```

### 2. Upload chunks

Raw binary body plus `Content-Range: bytes <start>-<end>/<total>` (inclusive
bounds). A chunk is at most 1 MiB. Overlapping ranges with identical bytes are
idempotent; overlapping ranges with different bytes or bounds return
`409 chunk_conflict`. Out-of-bounds ranges or a total that disagrees with the
upload metadata return `400`/`416`.

```sh
curl -s -X POST http://127.0.0.1:8080/v1/uploads/$ID/chunks \
  -H 'Content-Range: bytes 0-1048575/'$SIZE \
  --data-binary @chunk-0.bin
```

### 3. Complete

```sh
curl -s -X POST http://127.0.0.1:8080/v1/uploads/$ID/complete
```

The server checks the chunks contiguously cover exactly `[0, size)`, streams the
assembled bytes through SHA-256 and verifies the digest. Gaps return
`409 upload_incomplete`, a digest mismatch returns `409 digest_mismatch`. The
request is safe to retry: a committed upload returns the same result and the
object is published exactly once.

### 4. Read an object

```sh
curl -s http://127.0.0.1:8080/v1/objects/team-a/releases/artifact.bin -o out.bin

# single range -> 206 with Content-Range, Accept-Ranges, Content-Length
curl -s -H 'Range: bytes=100-199' \
  http://127.0.0.1:8080/v1/objects/team-a/releases/artifact.bin
# suffix ranges (bytes=-100) and open-ended ranges (bytes=100-) are supported
# malformed or multiple ranges -> 400; unsatisfiable ranges -> 416
```

### 5. Read a blob by digest

```sh
curl -s http://127.0.0.1:8080/v1/blobs/$SHA -o blob.bin
```

## Safety defaults

- Listens on loopback; request bodies capped at 2 MiB, headers at 64 KiB.
- Tenant and object names are validated, so object paths cannot escape the
  tenant namespace or the blob directory.
- SQLite runs with WAL, foreign keys and a busy timeout; a single DB connection
  plus per-upload locks serialize concurrent chunk/complete requests without
  losing data, double-committing or moving state backwards.
- Staging files are created with `0600` in a `0700` data directory and are
  removed when completion fails.

## Verify

```sh
go test ./...
go vet ./...
go build ./cmd/registryd
```
