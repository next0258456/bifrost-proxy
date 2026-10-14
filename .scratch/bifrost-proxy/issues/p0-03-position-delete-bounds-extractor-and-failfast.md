# 03: p0-03-position-delete-bounds-extractor-and-failfast

**What to build:**
Sửa bộ giải mã đường dẫn file tham chiếu trong Position Delete files (`field 2147483546`) tại `engine/manifest.go` và loại bỏ nhánh fallback nguy hiểm trong `engine/incremental.go`. Quét đúng cấu trúc mảng record key-value của `lower_bounds` và `upper_bounds` để lấy ra chuỗi đường dẫn dữ liệu đích. Khi đồng bộ vào `ducklake_delete_file`, nếu không tìm thấy file dữ liệu gốc tương ứng trong danh mục, lập tức dừng và trả về lỗi Fail-Fast thay vì gán nhầm vào `MAX(data_file_id)`.

**Blocked by:**
02: p0-02-avro-union-unwrap-and-delta-manifest-filter

**Status:**
completed

- [x] Cập nhật trích xuất `lower_bounds` và `upper_bounds` trong `ReadManifestFile`, hỗ trợ cả cấu trúc map lẫn mảng các cặp `{"key": 2147483546, "value": ...}`.
- [x] Bổ sung kiểm tra đảm bảo `lower_bound == upper_bound` (chỉ chấp nhận Position Delete trỏ tới 1 file duy nhất như chuẩn C++ quy định).
- [x] Xóa bỏ hoàn toàn nhánh `SELECT COALESCE(MAX(data_file_id), 1)` trong `engine/incremental.go`.
- [x] Nếu không tìm thấy `refDataFileID` (hoặc `ReferencedPath` bị rỗng), lập tức trả về `fmt.Errorf` fail-fast.
- [x] Viết unit test kiểm thử việc bóc tách trường 2147483546 và kiểm thử fail-fast khi thiếu file tham chiếu.
