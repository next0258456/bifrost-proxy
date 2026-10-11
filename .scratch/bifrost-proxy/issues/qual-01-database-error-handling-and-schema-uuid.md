# 01: qual-01-database-error-handling-and-schema-uuid

**What to build:**
Khắc phục vi phạm nuốt lỗi cơ sở dữ liệu trong `engine/incremental.go` bằng cách kiểm tra và trả về lỗi đúng chuẩn, tự sinh UUID v4 cho các bản ghi `ducklake_schema`, và mở rộng hàm mapper hỗ trợ các biến thể kiểu `fixed[L]` sang `blob`.

**Blocked by:**
None (can start immediately)

**Status:**
completed

- [x] Loại bỏ toàn bộ `_ = e.db.QueryRow(...)` trong `engine/incremental.go`, kiểm tra lỗi và trả về lỗi rõ ràng khi truy vấn thất bại.
- [x] Sử dụng `uuid.New().String()` khi chèn bản ghi mới vào `ducklake_catalog.ducklake_schema`.
- [x] Cập nhật `MapIcebergTypeToDuckLake` để hỗ trợ `strings.HasPrefix(raw, "fixed")` chuyển thành `blob`.
- [x] Bổ sung unit test kiểm tra các kiểu `fixed[16]`, `fixed[32]`.
- [x] Toàn bộ unit/integration test trong package `engine` và repo pass 100%.
