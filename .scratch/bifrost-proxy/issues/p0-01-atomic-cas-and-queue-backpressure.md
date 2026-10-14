# 01: p0-01-atomic-cas-and-queue-backpressure

**What to build:**
Tái cấu trúc luồng commit trong `storage/postgres.go` và `api/catalog.go`. Gộp việc kiểm tra điều kiện optimistic concurrency (`ValidateRequirements`) và ghi nhận con trỏ (`SaveTableCommit`) vào một Database Transaction duy nhất sử dụng khóa dòng `SELECT ... FOR UPDATE` trên bảng `catalog_tables`. Tự động thực hiện `UPDATE` khi bảng đã tồn tại thay vì ép buộc `previousLocation != ""` (khắc phục triệt để lỗi 409 Conflict từ lần commit thứ 2). Đồng thời kiểm tra dung lượng `IngestionQueue` ngay trong transaction để Rollback khi queue đầy, tránh split-brain giữa Iceberg pointer và DuckLake catalog.

**Blocked by:**
None (can start immediately)

**Status:**
completed

- [x] Mở rộng giao diện Catalog Store để hỗ trợ Transaction hoặc phương thức Commit nguyên khối `ExecuteAtomicCommit(ctx, id, req, canSubmitFunc) error`.
- [x] Sử dụng `SELECT ... FOR UPDATE` trên bảng `catalog_tables` để khóa dòng trước khi kiểm tra `requirements`.
- [x] Tự động chuyển đổi giữa `INSERT` (khi bảng chưa tồn tại) và `UPDATE` (khi bảng đã tồn tại và thỏa mãn `requirements`), không bắt buộc `previous-metadata-location` phải có giá trị.
- [x] Tích hợp kiểm tra `queue.Submit` trước khi `tx.Commit()`: nếu queue đầy thì rollback transaction và trả về lỗi `HTTP 429 Too Many Requests`.
- [x] Viết unit test kiểm thử các trường hợp commit tuần tự nhiều lần và commit đồng thời với các requirements khác nhau.
