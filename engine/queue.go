package engine

import (
	"context"
	"log"
	"sync"

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
	taskChan chan CommitTask
	engine   *SyncEngine
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewIngestionQueue(engine *SyncEngine, bufferSize int) *IngestionQueue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &IngestionQueue{
		taskChan: make(chan CommitTask, bufferSize),
		engine:   engine,
		ctx:      ctx,
		cancel:   cancel,
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
				if err := q.engine.SyncDeltaIncremental(task); err != nil {
					log.Printf("IngestionQueue: Sync error for %s: %v", task.Table, err)
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
