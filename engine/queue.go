package engine

import (
	"context"
	"log"
	"sync"
)

// CommitTask represents an asynchronous task to synchronize Iceberg delta to DuckLake
type CommitTask struct {
	Namespace        string
	TableName        string
	MetadataLocation string
	SnapshotID       int64
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

	// Start 1 single sequential worker to eliminate write contention and enforce RAM limits
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
		log.Printf("Warning: IngestionQueue is full! Dropping or shedding load for table: %s.%s", task.Namespace, task.TableName)
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
			log.Printf("IngestionQueue: Processing commit for %s.%s (Metadata: %s)", task.Namespace, task.TableName, task.MetadataLocation)
			if q.engine != nil {
				if err := q.engine.SyncDeltaIncremental(task); err != nil {
					log.Printf("IngestionQueue: Sync error for %s.%s: %v", task.Namespace, task.TableName, err)
				}
			}
		}
	}
}

func (q *IngestionQueue) Stop() {
	q.cancel()
	close(q.taskChan)
	q.wg.Wait()
	log.Println("IngestionQueue stopped cleanly.")
}
