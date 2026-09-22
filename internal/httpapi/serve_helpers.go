package httpapi

import (
	"context"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
)

func contextWithTimeout(c echo.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Request().Context(), d)
}

func digestETag(digest string) string {
	return strconv.Quote("sha256-" + digest)
}
