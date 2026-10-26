package chat

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// RoomEvent represents an immutable real-time chat event exchanged over WebSockets and Redis Pub/Sub.
type RoomEvent struct {
	Type       string    `json:"type"`        // "chat", "ai_chunk", "typing"
	RoomID     uuid.UUID `json:"room_id"`     // Identifier of the chat room
	SenderID   string    `json:"sender_id"`   // User ID or AI Assistant identifier
	SenderType string    `json:"sender_type"` // "user" | "assistant"
	Content    string    `json:"content"`     // Message body text
	CreatedAt  time.Time `json:"created_at"`  // Event generation timestamp
}

// RoomMember represents a participant in a chat room.
type RoomMember struct {
	UserID      uuid.UUID `json:"user_id"`
	DisplayName string    `json:"display_name"`
	Role        string    `json:"role"`
}

// RoomRepository specifies the database storage operations for rooms and messages.
type RoomRepository interface {
	GetRoomMembers(ctx context.Context, roomID uuid.UUID) ([]RoomMember, error)
	SaveMessage(ctx context.Context, event RoomEvent) error
	ExpireInactiveRooms(ctx context.Context) (int64, error)
}

// AIAssistant defines the contract for real-time AI background reactions.
type AIAssistant interface {
	Trigger(ctx context.Context, roomID uuid.UUID, event RoomEvent) error
}
