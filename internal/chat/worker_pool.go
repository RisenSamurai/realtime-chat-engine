package chat

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

type aiTask struct {
	roomID uuid.UUID
	event  RoomEvent
}

// RoomWorkerPool manages bounded background workers for room message persistence and AI triggers.
// It applies backpressure via buffered channels, prevents resource exhaustion, and guarantees clean draining upon shutdown.
type RoomWorkerPool struct {
	logger *slog.Logger
	repo   RoomRepository
	ai     AIAssistant

	// Message persistence queue
	persistQueue chan RoomEvent
	persistWg    sync.WaitGroup

	// AI trigger queue
	aiQueue chan aiTask
	aiWg    sync.WaitGroup

	// Shutdown control
	ctx    context.Context
	cancel context.CancelFunc
}

// NewRoomWorkerPool initializes worker pools for database persistence and AI interactions.
func NewRoomWorkerPool(
	repo RoomRepository,
	ai AIAssistant,
	persistWorkers int,
	persistBufferSize int,
	aiWorkers int,
	aiBufferSize int,
	logger *slog.Logger,
) *RoomWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &RoomWorkerPool{
		logger:       logger,
		repo:         repo,
		ai:           ai,
		persistQueue: make(chan RoomEvent, persistBufferSize),
		aiQueue:      make(chan aiTask, aiBufferSize),
		ctx:          ctx,
		cancel:       cancel,
	}

	// 1. Start persistence workers
	for i := 0; i < persistWorkers; i++ {
		pool.persistWg.Add(1)
		go func(workerID int) {
			defer pool.persistWg.Done()
			for {
				select {
				case <-ctx.Done():
					// Drain remaining items before quitting
					for {
						select {
						case event := <-pool.persistQueue:
							drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
							_ = pool.repo.SaveMessage(drainCtx, event)
							drainCancel()
						default:
							return
						}
					}
				case event, ok := <-pool.persistQueue:
					if !ok {
						return
					}
					taskCtx, taskCancel := context.WithTimeout(context.Background(), 10*time.Second)
					if err := pool.repo.SaveMessage(taskCtx, event); err != nil {
						pool.logger.Error("failed to persist room message", slog.Any("error", err))
					} else if pool.ai != nil {
						// Trigger AI evaluation asynchronously after DB write
						pool.EnqueueAI(event.RoomID, event)
					}
					taskCancel()
				}
			}
		}(i)
	}

	// 2. Start AI assistant workers
	for i := 0; i < aiWorkers; i++ {
		pool.aiWg.Add(1)
		go func(workerID int) {
			defer pool.aiWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-pool.aiQueue:
					if !ok {
						return
					}
					if pool.ai != nil {
						aiCtx, aiCancel := context.WithTimeout(context.Background(), 35*time.Second)
						_ = pool.ai.Trigger(aiCtx, task.roomID, task.event)
						aiCancel()
					}
				}
			}
		}(i)
	}

	return pool
}

// EnqueuePersist safely enqueues a room message for background DB persistence.
// Returns false if the queue is full (applying backpressure) or if the pool is stopping.
func (p *RoomWorkerPool) EnqueuePersist(event RoomEvent) bool {
	if p.ctx.Err() != nil {
		return false
	}
	select {
	case <-p.ctx.Done():
		return false
	case p.persistQueue <- event:
		return true
	default:
		p.logger.Warn("room persist queue full, message dropped from background queue",
			slog.String("room_id", event.RoomID.String()),
		)
		return false
	}
}

// EnqueueAI safely enqueues an AI assistant evaluation task.
func (p *RoomWorkerPool) EnqueueAI(roomID uuid.UUID, event RoomEvent) bool {
	if p.ctx.Err() != nil {
		return false
	}
	select {
	case <-p.ctx.Done():
		return false
	case p.aiQueue <- aiTask{roomID: roomID, event: event}:
		return true
	default:
		p.logger.Warn("room AI trigger queue full, skipping AI evaluation",
			slog.String("room_id", roomID.String()),
		)
		return false
	}
}

// Stop gracefully shuts down the worker pool, draining pending database writes.
func (p *RoomWorkerPool) Stop() {
	p.cancel()
	p.persistWg.Wait()
	p.aiWg.Wait()
}
