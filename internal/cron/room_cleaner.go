package cron

import (
	"context"
	"log/slog"
	"time"
)

// RoomStore defines the data access contract required by the room inactivity watcher.
type RoomStore interface {
	ExpireInactiveRooms(ctx context.Context) (int64, error)
}

// StartRoomInactivityWatcher periodically identifies and transitions inactive rooms to expired status.
// It respects the provided parent context and cleanly stops its ticker when cancelled.
func StartRoomInactivityWatcher(ctx context.Context, store RoomStore, logger *slog.Logger) {
	ticker := time.NewTicker(1 * time.Hour)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				execCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				rowsAffected, err := store.ExpireInactiveRooms(execCtx)
				cancel()

				if err != nil {
					logger.ErrorContext(ctx, "failed to expire inactive rooms", slog.Any("error", err))
				} else if rowsAffected > 0 {
					logger.InfoContext(ctx, "inactive rooms expired successfully", slog.Int64("count", rowsAffected))
				}
			}
		}
	}()
}
