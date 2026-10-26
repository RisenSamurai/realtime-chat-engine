package chat

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/RisenSamurai/realtime-chat-engine/pkg/concurrency"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RoomWSHandler coordinates real-time WebSocket communication, Redis Pub/Sub multiplexing,
// and background database writes via a bounded worker pool.
type RoomWSHandler struct {
	repo        RoomRepository
	rdb         *redis.Client
	logger      *slog.Logger
	workerPool  *RoomWorkerPool
	rateLimiter *concurrency.Limiter
}

// NewRoomWSHandler constructs a new RoomWSHandler.
func NewRoomWSHandler(
	repo RoomRepository,
	rdb *redis.Client,
	ai AIAssistant,
	logger *slog.Logger,
) *RoomWSHandler {
	handler := &RoomWSHandler{
		repo:        repo,
		rdb:         rdb,
		logger:      logger,
		rateLimiter: concurrency.NewLimiter(10),
	}
	// 8 persist workers (buffer 1000), 4 AI workers (buffer 100)
	handler.workerPool = NewRoomWorkerPool(repo, ai, 8, 1000, 4, 100, logger)
	return handler
}

// Close gracefully terminates background worker pools, ensuring in-flight messages are committed.
func (h *RoomWSHandler) Close() {
	if h.workerPool != nil {
		h.workerPool.Stop()
	}
}

// HandleWS upgrades HTTP to WebSocket and manages real-time bidirectional streaming.
func (h *RoomWSHandler) HandleWS(c *gin.Context) {
	roomIDStr := c.Param("room_id")
	roomID, err := uuid.Parse(roomIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid room id"})
		return
	}

	userID := c.GetString("userID")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user unauthenticated"})
		return
	}

	// 1. IDOR Protection: verify user membership in the requested room
	authCtx, authCancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer authCancel()

	members, err := h.repo.GetRoomMembers(authCtx, roomID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "room not found"})
		return
	}

	isMember := false
	for _, m := range members {
		if m.UserID.String() == userID {
			isMember = true
			break
		}
	}
	if !isMember {
		c.JSON(http.StatusForbidden, gin.H{"error": "access denied to room"})
		return
	}

	// 2. Allowed origins configuration
	allowedOrigins := []string{
		"localhost:*",
		"127.0.0.1:*",
	}
	if originsEnv := os.Getenv("ALLOWED_ORIGINS"); originsEnv != "" {
		for _, o := range strings.Split(originsEnv, ",") {
			if trimmed := strings.TrimSpace(o); trimmed != "" {
				allowedOrigins = append(allowedOrigins, trimmed)
			}
		}
	}

	// 3. Upgrade connection to WebSocket
	conn, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{
		OriginPatterns: allowedOrigins,
	})
	if err != nil {
		h.logger.Error("websocket upgrade failed", slog.Any("error", err))
		return
	}
	conn.SetReadLimit(8192) // 8KB max frame size
	defer conn.Close(websocket.StatusInternalError, "connection terminated")

	wsCtx, wsCancel := context.WithCancel(c.Request.Context())
	defer wsCancel()

	// 4. Background Heartbeat (Ping/Pong) every 30s to detect broken sockets
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-wsCtx.Done():
				return
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(wsCtx, 5*time.Second)
				err := conn.Ping(pingCtx)
				pingCancel()
				if err != nil {
					wsCancel()
					return
				}
			}
		}
	}()

	channelName := "room:" + roomID.String()
	pubsub := h.rdb.Subscribe(wsCtx, channelName)
	defer pubsub.Close()

	ch := pubsub.Channel()

	// 5. Dedicated non-blocking queue for client writes (Slow Consumer Protection)
	clientWriteCh := make(chan []byte, 64)

	// Goroutine writing frames to the WebSocket connection
	go func() {
		defer wsCancel()
		for {
			select {
			case <-wsCtx.Done():
				return
			case payload, ok := <-clientWriteCh:
				if !ok {
					return
				}
				writeCtx, writeCancel := context.WithTimeout(wsCtx, 3*time.Second)
				err := conn.Write(writeCtx, websocket.MessageText, payload)
				writeCancel()
				if err != nil {
					return
				}
			}
		}
	}()

	// Goroutine consuming from Redis Pub/Sub and pushing to the socket queue non-blockingly
	go func() {
		defer wsCancel()
		for {
			select {
			case <-wsCtx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				select {
				case clientWriteCh <- []byte(msg.Payload):
				default:
					h.logger.WarnContext(wsCtx, "slow consumer detected on websocket, dropped frame",
						slog.String("user_id", userID),
						slog.String("room_id", roomID.String()),
					)
				}
			}
		}
	}()

	// 6. Main loop reading client messages from the WebSocket
	for {
		_, data, err := conn.Read(wsCtx)
		if err != nil {
			break
		}

		var incoming RoomEvent
		if err := json.Unmarshal(data, &incoming); err != nil {
			continue
		}

		incoming.RoomID = roomID
		incoming.SenderID = userID
		incoming.SenderType = "user"
		incoming.CreatedAt = time.Now().UTC()

		payload, _ := json.Marshal(incoming)

		// Broadcast message instantly to all participants in this room via Redis Pub/Sub
		h.rdb.Publish(wsCtx, channelName, payload)

		// Safely enqueue into the background persistence worker pool
		h.workerPool.EnqueuePersist(incoming)
	}
}
