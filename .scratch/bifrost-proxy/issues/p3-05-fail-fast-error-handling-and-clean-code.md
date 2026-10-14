# p3-05: Fail-Fast Error Handling and Clean Code

**What to build:** Xóa bỏ toàn bộ các vi phạm nuốt lỗi được phát hiện bởi Standards Reviewer:
1. Xử lý fail-fast cho các câu truy vấn `SELECT GREATEST(...)` trong `engine/incremental.go`.
2. Bắt lỗi `strconv.Atoi` khi parse scale của decimal và xử lý log cảnh báo rõ ràng khi `DecodeIcebergBinaryBound` thất bại trong `engine/manifest.go`.
3. Bắt chính xác ngoại lệ tạo bucket MinIO trong `seed_benchmark_minio.py`.

**Blocked by:** Không

**Status:** completed

- [x] Bọc lỗi tường minh với `%w` cho các câu query `SELECT GREATEST(...)` kiểm tra bộ đếm trong `engine/incremental.go`.
- [x] Bắt lỗi `strconv.Atoi(scaleStr)` khi giải mã `decimal(P, S)` trong `engine/manifest.go`.
- [x] Ghi nhận log cảnh báo khi giải mã bounds nhị phân thất bại trong `extractColumnBoundsStats` (`engine/manifest.go`).
- [x] Xử lý ngoại lệ `ClientError` cụ thể trong `seed_benchmark_minio.py` thay vì `except Exception: pass`.
- [x] Chạy unit tests xác nhận không có lỗi biên dịch hoặc lỗi hồi quy.
