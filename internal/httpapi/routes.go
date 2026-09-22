// Package httpapi exposes the artifact registry over HTTP.
package httpapi

import (
	"database/sql"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"artifact-registry/internal/registry"

	"github.com/labstack/echo/v4"
)

const maxBodyBytes = 2 << 20 // metadata JSON plus one <=1 MiB chunk

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(c echo.Context, status int, code, message string) error {
	b := errorBody{}
	b.Error.Code = code
	b.Error.Message = message
	return c.JSON(status, b)
}

func mapError(c echo.Context, err error) error {
	if re, ok := registry.AsError(err); ok {
		status := http.StatusInternalServerError
		switch re.Code {
		case registry.CodeInvalidArgument, registry.CodeRangeInvalid:
			status = http.StatusBadRequest
		case registry.CodePayloadTooLarge:
			status = http.StatusRequestEntityTooLarge
		case registry.CodeNotFound:
			status = http.StatusNotFound
		case registry.CodeChunkConflict, registry.CodeUploadNotOpen, registry.CodeIncomplete, registry.CodeDigestMismatch:
			status = http.StatusConflict
		case registry.CodeRangeNotSatisf:
			status = http.StatusRequestedRangeNotSatisfiable
		}
		return writeError(c, status, re.Code, re.Message)
	}
	if errors.Is(err, io.EOF) {
		return writeError(c, http.StatusBadRequest, registry.CodeInvalidArgument, "request body is required")
	}
	return writeError(c, http.StatusInternalServerError, registry.CodeInternal, "internal error")
}

// Register wires the registry's public HTTP API onto e.
func Register(e *echo.Echo, store *registry.Store) {
	e.HideBanner = true
	e.HidePort = true
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		he, ok := err.(*echo.HTTPError)
		if ok {
			code := registry.CodeInternal
			switch he.Code {
			case http.StatusRequestEntityTooLarge:
				code = registry.CodePayloadTooLarge
			case http.StatusNotFound, http.StatusMethodNotAllowed:
				code = registry.CodeNotFound
			default:
				if he.Code >= 400 && he.Code < 500 {
					code = registry.CodeInvalidArgument
				}
			}
			msg := http.StatusText(he.Code)
			if s, ok := he.Message.(string); ok && s != "" {
				msg = s
			}
			_ = writeError(c, he.Code, code, msg)
			return
		}
		_ = mapError(c, err)
	}
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			r := c.Request()
			if r.Body != nil {
				r.Body = http.MaxBytesReader(c.Response(), r.Body, maxBodyBytes)
			}
			return next(c)
		}
	})

	e.GET("/healthz", healthz(store))
	e.POST("/v1/uploads", createUpload(store))
	e.POST("/v1/uploads/:id/chunks", putChunk(store))
	e.POST("/v1/uploads/:id/complete", completeUpload(store))
	e.GET("/v1/objects/:tenant/*", getObject(store))
	e.GET("/v1/blobs/:digest", getBlob(store))
}

func healthz(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, cancel := contextWithTimeout(c, 2*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			return writeError(c, http.StatusServiceUnavailable, registry.CodeInternal, "store unavailable")
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	}
}

type createUploadRequest struct {
	Tenant     string `json:"tenant"`
	ObjectName string `json:"object_name"`
	Size       *int64 `json:"size"`
	SHA256     string `json:"sha256"`
}

func createUpload(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req createUploadRequest
		if err := c.Bind(&req); err != nil {
			return writeError(c, http.StatusBadRequest, registry.CodeInvalidArgument, "invalid JSON body")
		}
		if req.Size == nil {
			return writeError(c, http.StatusBadRequest, registry.CodeInvalidArgument, "size is required")
		}
		ctx, cancel := contextWithTimeout(c, 10*time.Second)
		defer cancel()
		u, err := store.CreateUpload(ctx, req.Tenant, req.ObjectName, req.SHA256, *req.Size)
		if err != nil {
			return mapError(c, err)
		}
		return c.JSON(http.StatusCreated, u)
	}
}

// parseContentRange parses a strict "bytes start-end/total" header.
func parseContentRange(v string) (start, end, total int64, err error) {
	const prefix = "bytes "
	if !strings.HasPrefix(v, prefix) {
		return 0, 0, 0, errors.New("malformed Content-Range")
	}
	rest := strings.TrimPrefix(v, prefix)
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return 0, 0, 0, errors.New("malformed Content-Range")
	}
	rangePart, totalPart := rest[:slash], rest[slash+1:]
	dash := strings.IndexByte(rangePart, '-')
	if dash < 0 || totalPart == "" || totalPart == "*" {
		return 0, 0, 0, errors.New("malformed Content-Range")
	}
	start, err = strconv.ParseInt(rangePart[:dash], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	end, err = strconv.ParseInt(rangePart[dash+1:], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	total, err = strconv.ParseInt(totalPart, 10, 64)
	return start, end, total, err
}

func putChunk(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		id := c.Param("id")
		cr := c.Request().Header.Get("Content-Range")
		if cr == "" {
			return writeError(c, http.StatusBadRequest, registry.CodeInvalidArgument,
				"Content-Range: bytes start-end/total is required")
		}
		start, end, total, err := parseContentRange(cr)
		if err != nil || start < 0 || end < start || total < 0 {
			return writeError(c, http.StatusBadRequest, registry.CodeInvalidArgument, "malformed Content-Range")
		}
		length := end - start + 1
		if length > registry.MaxChunkSize {
			return writeError(c, http.StatusRequestEntityTooLarge, registry.CodePayloadTooLarge,
				"chunk must not exceed 1 MiB")
		}
		body := c.Request().Body
		// LimitReader caps memory even if declared length is bogus.
		data, err := io.ReadAll(io.LimitReader(body, registry.MaxChunkSize+1))
		if err != nil {
			var bme *http.MaxBytesError
			if errors.As(err, &bme) {
				return writeError(c, http.StatusRequestEntityTooLarge, registry.CodePayloadTooLarge,
					"chunk must not exceed 1 MiB")
			}
			return writeError(c, http.StatusBadRequest, registry.CodeInvalidArgument, "failed to read chunk body")
		}
		if int64(len(data)) > registry.MaxChunkSize {
			return writeError(c, http.StatusRequestEntityTooLarge, registry.CodePayloadTooLarge,
				"chunk must not exceed 1 MiB")
		}
		ctx, cancel := contextWithTimeout(c, 30*time.Second)
		defer cancel()
		if err := store.PutChunk(ctx, id, start, end, total, data); err != nil {
			return mapError(c, err)
		}
		return c.JSON(http.StatusOK, map[string]any{
			"id":    id,
			"start": start,
			"end":   end,
			"saved": len(data),
		})
	}
}

func completeUpload(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		id := c.Param("id")
		ctx, cancel := contextWithTimeout(c, 60*time.Second)
		defer cancel()
		u, err := store.CompleteUpload(ctx, id)
		if err != nil {
			return mapError(c, err)
		}
		return c.JSON(http.StatusOK, u)
	}
}

// parseRequestRange accepts exactly one byte range with explicit bounds or a
// open-ended suffix range. Multiple ranges and malformed values are rejected.
func parseRequestRange(v string, size int64) (start, end int64, badRequest bool, ok bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(v, prefix) {
		return 0, 0, true, false
	}
	spec := strings.TrimPrefix(v, prefix)
	if strings.ContainsRune(spec, ',') {
		return 0, 0, true, false // multiple ranges unsupported
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, true, false
	}
	first, last := spec[:dash], spec[dash+1:]
	if first == "" {
		if last == "" {
			return 0, 0, true, false
		}
		suffix, err := strconv.ParseInt(last, 10, 64)
		if err != nil || suffix < 0 {
			return 0, 0, true, false
		}
		if size == 0 || suffix == 0 {
			return 0, 0, false, false
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, false, true
	}
	s, err := strconv.ParseInt(first, 10, 64)
	if err != nil || s < 0 {
		return 0, 0, true, false
	}
	if s >= size {
		return 0, 0, false, false // unsatisfiable
	}
	e := size - 1
	if last != "" {
		l, err := strconv.ParseInt(last, 10, 64)
		if err != nil || l < s {
			return 0, 0, true, false
		}
		e = l
		if e >= size {
			e = size - 1
		}
	}
	return s, e, false, true
}

func serveRange(c echo.Context, path, digest string, size int64, modTime time.Time) error {
	rangeHeader := c.Request().Header.Get("Range")
	if rangeHeader != "" {
		_, _, badRequest, satisfiable := parseRequestRange(rangeHeader, size)
		if badRequest {
			return writeError(c, http.StatusBadRequest, registry.CodeInvalidArgument, "malformed or unsupported Range header")
		}
		if !satisfiable {
			c.Response().Header().Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
			return writeError(c, http.StatusRequestedRangeNotSatisfiable, registry.CodeRangeNotSatisf,
				"range is not satisfiable")
		}
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return writeError(c, http.StatusNotFound, registry.CodeNotFound, "content is unavailable")
		}
		return writeError(c, http.StatusInternalServerError, registry.CodeInternal, "failed to open content")
	}
	defer f.Close()
	h := c.Response().Header()
	h.Set("Accept-Ranges", "bytes")
	h.Set("ETag", digestETag(digest))
	h.Set("Content-Type", echo.MIMEOctetStream)
	// http.ServeContent handles GET 200/206, Content-Length and If-* headers.
	http.ServeContent(c.Response(), c.Request(), "", modTime, f)
	return nil
}

func getObject(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		tenant := c.Param("tenant")
		name := strings.TrimPrefix(c.Param("*"), "/")
		ctx, cancel := contextWithTimeout(c, 10*time.Second)
		defer cancel()
		h, err := store.OpenObject(ctx, tenant, name)
		if err != nil {
			return mapError(c, err)
		}
		defer h.Close()
		return serveRange(c, h.Path, h.Digest, h.Size, h.ModTime)
	}
}

func getBlob(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		digest := c.Param("digest")
		if err := registry.ValidateDigest(digest); err != nil {
			return mapError(c, err)
		}
		ctx, cancel := contextWithTimeout(c, 10*time.Second)
		defer cancel()
		var size int64
		if err := store.BlobSize(ctx, digest, &size); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return writeError(c, http.StatusNotFound, registry.CodeNotFound, "blob not found")
			}
			return mapError(c, err)
		}
		path, modTime, err := store.BlobFileInfo(ctx, digest)
		if err != nil {
			return mapError(c, err)
		}
		return serveRange(c, path, digest, size, modTime)
	}
}
