package httpapi

import (
    "net/http"

    "github.com/labstack/echo/v4"
    "artifact-registry/internal/registry"
)

func Register(e *echo.Echo, store *registry.Store) {
    e.GET("/healthz", func(c echo.Context) error {
        return c.String(http.StatusOK, "ok\n")
    })
    // Upload and blob routes are deliberately left for the implementation task.
    e.Any("/v1/*", func(c echo.Context) error {
        return c.NoContent(http.StatusNotImplemented)
    })
}
