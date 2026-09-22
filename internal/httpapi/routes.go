// Package httpapi exposes the registry over HTTP.
package httpapi

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"artifact-registry/internal/registry"
)

// APIError is the stable JSON error envelope for every error response.
type APIError struct {
	Code    string "json:\"code\""
	Message string "json:\"message\""
}

func apiError(c echo.Context, status int, code, msg string) error {
	return c.JSON(status, APIError{Code: code, Message: msg})
}

func storeError(c echo.Context, err error) error {
	switch {
	case errors.Is(err, registry.ErrNotFound):
		return apiError(c, http.StatusNotFound, "not_found", "upload or object not found")
	case errors.Is(err, registry.ErrInvalidArgument):
		return apiError(c, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, registry.ErrDigestMismatch):
		return apiError(c, http.StatusUnprocessableEntity, "digest_mismatch", err.Error())
	case errors.Is(err, registry.ErrConflict):
		return apiError(c, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, registry.ErrRangeUnsatisfiable):
		return apiError(c, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable", err.Error())
	default:
		return apiError(c, http.StatusInternalServerError, "internal_error", "internal error")
	}
}

// Register mounts the registry HTTP API on e.
func Register(e *echo.Echo, store *registry.Store) {
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		var he *echo.HTTPError
		if errors.As(err, &he) {
			code := "http_error"
			if he.Code == http.StatusRequestEntityTooLarge {
				code = "request_too_large"
			}
			msg := http.StatusText(he.Code)
			if s, ok := he.Message.(string); ok {
				msg = s
			}
			_ = apiError(c, he.Code, code, msg)
			return
		}
		_ = apiError(c, http.StatusInternalServerError, "internal_error", "internal error")
	}

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Response().Header().Set("X-Content-Type-Options", "nosniff")
			return next(c)
		}
	})

	e.GET("/healthz", healthz(store))

	api := e.Group("/v1")
	api.POST("/uploads", createUpload(store))
	api.POST("/uploads/:id/chunks", putChunk(store))
	api.POST("/uploads/:id/complete", completeUpload(store))
	api.GET("/objects/:tenant/*", getObject(store))
	api.GET("/blobs/:digest", getBlob(store))
}

func healthz(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := store.Ping(c.Request().Context()); err != nil {
			return apiError(c, http.StatusServiceUnavailable, "unavailable", "database unavailable")
		}
		return c.String(http.StatusOK, "ok\n")
	}
}

type createUploadRequest struct {
	Tenant     string "json:\"tenant\""
	ObjectName string "json:\"object_name\""
	Size       *int64 "json:\"size\""
	SHA256     string "json:\"sha256\""
}

func createUpload(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req createUploadRequest
		if err := c.Bind(&req); err != nil {
			return apiError(c, http.StatusBadRequest, "invalid_argument", "malformed JSON body")
		}
		if req.Size == nil {
			return apiError(c, http.StatusBadRequest, "invalid_argument", "size is required")
		}
		u, err := store.CreateUpload(c.Request().Context(),
			strings.TrimSpace(req.Tenant), strings.TrimSpace(req.ObjectName),
			strings.TrimSpace(req.SHA256), *req.Size)
		if err != nil {
			return storeError(c, err)
		}
		c.Response().Header().Set("Location", "/v1/uploads/"+u.ID)
		return c.JSON(http.StatusCreated, u)
	}
}

// parseContentRange parses "bytes start-end/total"; "*" for total is rejected.
func parseContentRange(v string) (start, end, total int64, ok bool) {
	if v == "" || !strings.HasPrefix(v, "bytes ") {
		return 0, 0, 0, false
	}
	spec := strings.TrimPrefix(v, "bytes ")
	parts := strings.Split(spec, "/")
	if len(parts) != 2 {
		return 0, 0, 0, false
	}
	bounds := strings.Split(parts[0], "-")
	if len(bounds) != 2 || bounds[0] == "" || bounds[1] == "" {
		return 0, 0, 0, false
	}
	var err error
	if start, err = strconv.ParseInt(bounds[0], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if end, err = strconv.ParseInt(bounds[1], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if total, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if start < 0 || end < start || total <= 0 || end >= total {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

func putChunk(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		id := c.Param("id")
		start, end, total, ok := parseContentRange(c.Request().Header.Get("Content-Range"))
		if !ok {
			return apiError(c, http.StatusBadRequest, "invalid_argument",
				"Content-Range must be bytes start-end/total")
		}

		// Read one byte beyond the advertised range to detect oversized bodies.
		length := end - start + 1
		body := http.MaxBytesReader(c.Response().Writer, c.Request().Body, length+1)
		data, err := io.ReadAll(body)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) || err.Error() == "http: request body too large" {
				return apiError(c, http.StatusRequestEntityTooLarge, "request_too_large",
					"chunk body exceeds Content-Range length")
			}
			return apiError(c, http.StatusBadRequest, "invalid_argument", "cannot read chunk body")
		}
		if int64(len(data)) != length {
			return apiError(c, http.StatusBadRequest, "invalid_argument",
				"body length does not match Content-Range")
		}

		if err := store.PutChunk(c.Request().Context(), id, start, end, total, data); err != nil {
			return storeError(c, err)
		}
		return c.NoContent(http.StatusNoContent)
	}
}

func completeUpload(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		id := c.Param("id")
		u, err := store.CompleteUpload(c.Request().Context(), id)
		if err != nil {
			return storeError(c, err)
		}
		return c.JSON(http.StatusOK, u)
	}
}

// parseRange parses a single "bytes=start-end" range. Multiple ranges and
// suffix ranges are rejected with an HTTP status.
func parseRange(v string, size int64) (start, end int64, status int, code string) {
	if v == "" {
		return 0, 0, 0, ""
	}
	if !strings.HasPrefix(v, "bytes=") {
		return 0, 0, http.StatusBadRequest, "invalid_range"
	}
	spec := strings.TrimPrefix(v, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, http.StatusBadRequest, "multiple_ranges_unsupported"
	}
	bounds := strings.SplitN(spec, "-", 2)
	if len(bounds) != 2 {
		return 0, 0, http.StatusBadRequest, "invalid_range"
	}
	if bounds[0] == "" {
		// Suffix ranges (bytes=-N) and empty specs are not supported.
		return 0, 0, http.StatusBadRequest, "suffix_range_unsupported"
	}
	s, err := strconv.ParseInt(bounds[0], 10, 64)
	if err != nil || s < 0 {
		return 0, 0, http.StatusBadRequest, "invalid_range"
	}
	end = size - 1
	if bounds[1] != "" {
		e, err := strconv.ParseInt(bounds[1], 10, 64)
		if err != nil || e < s {
			return 0, 0, http.StatusBadRequest, "invalid_range"
		}
		end = e
	}
	if s >= size || end >= size {
		return 0, 0, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable"
	}
	return s, end, http.StatusOK, ""
}

func serveBlob(c echo.Context, digest string, size int64, open func() (*sectionFile, error)) error {
	h := c.Response().Header()
	h.Set("Accept-Ranges", "bytes")
	h.Set(echo.HeaderContentType, "application/octet-stream")
	h.Set("X-Content-Sha256", digest)

	rangeHdr := c.Request().Header.Get("Range")
	if rangeHdr != "" {
		start, end, status, code := parseRange(rangeHdr, size)
		if status != http.StatusOK {
			if status == http.StatusRequestedRangeNotSatisfiable {
				h.Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
			}
			return apiError(c, status, code, "unsatisfiable or unsupported range")
		}
		sf, err := open()
		if err != nil {
			return storeError(c, err)
		}
		defer sf.Close()
		h.Set("Content-Range",
			"bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+
				"/"+strconv.FormatInt(size, 10))
		h.Set(echo.HeaderContentLength, strconv.FormatInt(end-start+1, 10))
		c.Response().WriteHeader(http.StatusPartialContent)
		_, err = sf.writeRange(c.Response(), start, end-start+1)
		return err
	}

	h.Set(echo.HeaderContentLength, strconv.FormatInt(size, 10))
	if size == 0 {
		c.Response().WriteHeader(http.StatusOK)
		return nil
	}
	sf, err := open()
	if err != nil {
		return storeError(c, err)
	}
	defer sf.Close()
	c.Response().WriteHeader(http.StatusOK)
	return sf.writeAll(c.Response(), size)
}

func getObject(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		tenant := c.Param("tenant")
		name := strings.TrimPrefix(c.Param("*"), "/")
		obj, err := store.LookupObject(c.Request().Context(), tenant, name)
		if err != nil {
			return storeError(c, err)
		}
		return serveBlob(c, obj.Digest, obj.Size, func() (*sectionFile, error) {
			return openSection(store, c, obj.Digest)
		})
	}
}

func getBlob(store *registry.Store) echo.HandlerFunc {
	return func(c echo.Context) error {
		digest := c.Param("digest")
		if !registry.ValidateDigest(digest) {
			return apiError(c, http.StatusBadRequest, "invalid_argument", "invalid digest")
		}
		// Probe existence up front for a stable 404, then let serveBlob open
		// the file lazily so error paths do not leak descriptors.
		probe, size, err := store.BlobReader(c.Request().Context(), digest)
		if err != nil {
			return storeError(c, err)
		}
		probe.Close()
		return serveBlob(c, digest, size, func() (*sectionFile, error) {
			return openSection(store, c, digest)
		})
	}
}
