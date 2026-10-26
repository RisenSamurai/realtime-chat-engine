package middleware

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// RateLimiter enforces request rate limiting using Redis sliding/fixed time window.
// It uses Redis Pipelining for atomic INCR and EXPIRE operations to prevent key leakage.
func RateLimiter(rdb *redis.Client, limit int64, window time.Duration) gin.HandlerFunc {
	windowSec := int64(window.Seconds())
	if windowSec <= 0 {
		log.Printf("RateLimiter: window duration must be at least 1 second")
		windowSec = 1
	}

	return func(c *gin.Context) {
		ctx := c.Request.Context()
		var id string

		// Каскадное определение клиента: UserID -> Bearer Token -> Client IP
		if userID, exists := c.Get("userID"); exists && userID != nil && fmt.Sprintf("%v", userID) != "" {
			id = fmt.Sprintf("user:%v", userID)
		} else {
			authHeader := c.GetHeader("Authorization")
			token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
			if token == "" {
				token = strings.TrimSpace(c.Query("token"))
			}

			if token != "" {
				if len(token) > 64 {
					token = token[:64]
				}
				id = fmt.Sprintf("token:%s", token)
			} else {
				id = fmt.Sprintf("ip:%s", c.ClientIP())
			}
		}

		currentTimeSlot := time.Now().Unix() / windowSec
		redisKey := fmt.Sprintf("rate_limit:%v:%v", id, currentTimeSlot)

		// Pipelining для атомарного выполнения Incr и Expire
		pipe := rdb.Pipeline()
		incrCmd := pipe.Incr(ctx, redisKey)
		pipe.Expire(ctx, redisKey, window)

		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
			return
		}

		count := incrCmd.Val()

		c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", limit))
		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", maxVal(0, limit-count)))

		if count > limit {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "Too Many Requests",
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

func maxVal(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
