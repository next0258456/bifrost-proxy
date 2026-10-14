# cr-01: cr-01-position-delete-active-file-binding

**What to build:**
Siết chặt điều kiện truy vấn phân giải `refDataFileID` cho Position Delete trong `engine/incremental.go`. Thêm bắt buộc điều kiện `table_id = $2` và `end_snapshot IS NULL` để đảm bảo file delete chỉ trỏ vào file dữ liệu đang active của chính bảng đó. Đồng thời triệt tiêu hành vi nuốt lỗi khi thực thi câu lệnh SQL đóng `end_snapshot` cho các file dữ liệu và file delete bị xóa: nếu `db.Exec` trả về lỗi, lập tức ngắt tiến trình và trả về `fmt.Errorf` (Fail-Fast).

**Blocked by:**
None (can start immediately)

**Status:**
completed

- [x] Cập nhật câu truy vấn tìm kiếm `data_file_id` cho Position Delete trong `engine/incremental.go` với điều kiện `WHERE path = $1 AND table_id = $2 AND end_snapshot IS NULL LIMIT 1;`.
- [x] Bổ sung kiểm tra lỗi và trả về `fmt.Errorf` khi thực hiện `closeDataFileSQL` và `closeDeleteFileSQL`, xóa bỏ hoàn toàn lệnh `log.Printf` nuốt lỗi.
- [x] Kiểm tra lỗi khi gọi `ExtractDataFilesFromStorage`: nếu có lỗi tải manifest thì lập tức return error thay vì chỉ ghi log warning.
