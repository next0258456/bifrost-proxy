# 01: ff-01-centralized-id-allocator-and-failfast-db

**What to build:**
Tập trung hóa logic cấp phát định danh số tự tăng (`next ID`) trên các bảng DuckLake catalog PostgreSQL thành một hàm helper nội bộ (`allocateNextID`), loại bỏ trùng lặp mã nguồn (Duplicated Code Smell). Đồng thời thực thi nguyên tắc Fail-Fast cho toàn bộ các truy vấn cơ sở dữ liệu (thay vì chỉ log cảnh báo rồi nuốt lỗi) và phân biệt rõ ràng giữa `sql.ErrNoRows` và lỗi cơ sở dữ liệu thật sự khi tra cứu tham chiếu file xóa Position Deletes.

**Blocked by:**
None (can start immediately)

**Status:**
completed

- [x] Tạo helper nội bộ `allocateNextID(table, idCol string) (int64, error)` để cấp phát ID kế tiếp cho các bảng DuckLake catalog.
- [x] Áp dụng helper này cho `ducklake_schema`, `ducklake_table`, `ducklake_data_file`, và `ducklake_delete_file`.
- [x] Xử lý kiểm tra lỗi và trả về `fmt.Errorf` ngay lập tức (fail-fast) nếu truy vấn kiểm tra `ducklake_schema` hoặc `ducklake_snapshot` gặp lỗi hệ thống.
- [x] Bắt lỗi tường minh khi truy vấn `refDataFileID` trong Position Deletes, phân biệt `sql.ErrNoRows` (dùng fallback) với các lỗi cơ sở dữ liệu khác (trả về lỗi).
- [x] Chạy toàn bộ unit test của package `engine` và kiểm tra không có hồi quy.
