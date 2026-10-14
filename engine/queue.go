package engine

import (
	"context"
	"log"
	"sync"
	"time"

	"bifrost-proxy/domain"
)

// CommitTask represents an asynchronous task to synchronize Iceberg delta to DuckLake
type CommitTask struct {
	Table            domain.TableIdentifier
	MetadataLocation string
	SnapshotID       int64
}

// QueueService defines the interface for task submission
type QueueService interface {
	Submit(task CommitTask) bool
	Stop()
}

// IngestionQueue manages a bounded buffer channel and sequential worker pool
type IngestionQueue struct {
	taskChan     chan CommitTask
	engine       *SyncEngine
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	RetryBackoff time.Duration
}

func NewIngestionQueue(engine *SyncEngine, bufferSize int) *IngestionQueue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &IngestionQueue{
		taskChan:     make(chan CommitTask, bufferSize),
		engine:       engine,
		ctx:          ctx,
		cancel:       cancel,
		RetryBackoff: 100 * time.Millisecond,
	}

	// 1 sequential worker eliminates write contention and enforces RAM limits
	q.wg.Add(1)
	go q.worker()

	log.Printf("IngestionQueue initialized with buffer size %d (1 sequential worker active)", bufferSize)
	return q
}

func (q *IngestionQueue) Submit(task CommitTask) bool {
	select {
	case q.taskChan <- task:
		return true
	default:
		log.Printf("Warning: IngestionQueue is full! Rejecting commit for table: %s", task.Table)
		return false
	}
}

func (q *IngestionQueue) worker() {
	defer q.wg.Done()
	for {
		select {
		case <-q.ctx.Done():
			log.Println("IngestionQueue worker shutting down...")
			return
		case task, ok := <-q.taskChan:
			if !ok {
				return
			}
			log.Printf("IngestionQueue: Processing commit for %s (Metadata: %s)", task.Table, task.MetadataLocation)
			if q.engine != nil {
				retrySchedule := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 1500 * time.Millisecond}
				if q.RetryBackoff > 0 {
					retrySchedule = []time.Duration{q.RetryBackoff, q.RetryBackoff * 5, q.RetryBackoff * 15}
				}
				var syncErr error
				for attempt := 1; attempt <= 3; attempt++ {
					syncErr = q.engine.SyncDeltaIncremental(task)
					if syncErr == nil {
						break
					}
					log.Printf("IngestionQueue: Attempt %d failed for %s: %v", attempt, task.Table, syncErr)
					if attempt < 3 {
						delay := retrySchedule[attempt-1]
						select {
						case <-time.After(delay):
						case <-q.ctx.Done():
							return
						}
					}
				}
				if syncErr != nil {
					log.Printf("IngestionQueue: Final sync error after 3 attempts for %s: %v", task.Table, syncErr)
				}
			}
		}
	}
}

func (q *IngestionQueue) Stop() {
	q.cancel()
	q.wg.Wait()
	log.Println("IngestionQueue stopped cleanly.")
}
