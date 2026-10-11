# Fix-04: Backpressure Queue & Real Snapshot ID Extraction

**What to build:** Trích xuất chính xác `snapshot-id` từ payload commit của Redpanda (thay vì tự sinh bằng `time.Now()`). Khi hàng đợi `IngestionQueue` bị đầy (buffer 1000), API lập tức trả về `HTTP 429 Too Many Requests` (hoặc `HTTP 503`), chấm dứt hoàn toàn việc âm thầm vứt bỏ task của client.

**Blocked by:** Fix-02: Strict CAS Optimistic Concurrency & Error Propagation

**Status:** ready-for-agent

- [x] Parse `snapshot-id` từ payload `UpdateTableRequest` trong `api/catalog.go`.
- [x] Xử lý giá trị trả về của `queue.Submit()`: nếu `false`, trả về `HTTP 429 Too Many Requests`.
- [x] Viết unit test xác nhận khi queue đầy thì client nhận mã 429 kèm error JSON chuẩn.
