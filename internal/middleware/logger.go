package middleware

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
)

// LoggerMiddleware logs incoming HTTP requests using standard structured logging (log/slog).
// It categorizes requests by response status codes, tracks latency, and collects query parameters.
func LoggerMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		end := time.Now()
		latency := end.Sub(start)

		status := c.Writer.Status()
		method := c.Request.Method
		clientIP := c.ClientIP()

		attributes := []slog.Attr{
			slog.Int("status", status),
			slog.String("method", method),
			slog.String("path", path),
			slog.String("client_ip", clientIP),
			slog.Duration("latency", latency),
		}

		if query != "" {
			attributes = append(attributes, slog.String("query", query))
		}

		if len(c.Errors) > 0 {
			for _, e := range c.Errors.Errors() {
				logger.LogAttrs(c.Request.Context(), slog.LevelError, e, attributes...)
			}
		} else if status >= 500 {
			logger.LogAttrs(c.Request.Context(), slog.LevelError, "HTTP Server Error", attributes...)
		} else if status >= 400 {
			logger.LogAttrs(c.Request.Context(), slog.LevelWarn, "HTTP Client Error", attributes...)
		} else {
			logger.LogAttrs(c.Request.Context(), slog.LevelInfo, "HTTP Request", attributes...)
		}
	}
}
