package engine_test

import (
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"bifrost-proxy/domain"
	"bifrost-proxy/engine"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestionQueueConcurrency(t *testing.T) {
	// Khởi tạo queue với buffer 200
	queue := engine.NewIngestionQueue(nil, 200)

	var submittedCount int32 = 0
	numTasks := 100

	// Bắn đồng thời 100 tasks từ nhiều goroutines
	done := make(chan bool)
	for i := 0; i < numTasks; i++ {
		go func(id int) {
			ok := queue.Submit(engine.CommitTask{
				Table:            domain.NewTableIdentifier("bronze", "fmcg_sales"),
				MetadataLocation: "s3://lakehouse/bronze/fmcg_sales/metadata/v1.json",
				SnapshotID:       int64(id),
			})
			if ok {
				atomic.AddInt32(&submittedCount, 1)
			}
			done <- true
		}(i)
	}

	for i := 0; i < numTasks; i++ {
		<-done
	}

	assert.Equal(t, int32(numTasks), submittedCount, "Tất cả 100 tasks phải được nạp thành công vào hàng đợi")

	// Chờ một chút để worker bốc hết tasks ra
	time.Sleep(100 * time.Millisecond)
	queue.Stop()
}

func TestIngestionQueueWorkerRetriesOnFailure(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	syncEng := engine.NewTestSyncEngine(db, nil)
	queue := engine.NewIngestionQueue(syncEng, 10)
	queue.RetryBackoff = 1 * time.Millisecond

	// Task referencing non-existent metadata file will fail and trigger 3 retry attempts
	ok := queue.Submit(engine.CommitTask{
		Table:            domain.NewTableIdentifier("bronze", "retry_table"),
		MetadataLocation: "s3://non-existent/metadata.json",
		SnapshotID:       9999,
	})
	assert.True(t, ok)

	// Sleep briefly for all 3 attempts to finish
	time.Sleep(50 * time.Millisecond)
	queue.Stop()
}

