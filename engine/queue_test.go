package engine_test

import (
	"sync/atomic"
	"testing"
	"time"

	"bifrost-proxy/domain"
	"bifrost-proxy/engine"

	"github.com/stretchr/testify/assert"
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
