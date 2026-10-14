# 02: p0-02-avro-union-unwrap-and-delta-manifest-filter

**What to build:**
Sửa lỗi giải mã Avro Union trong `engine/manifest.go` và chuẩn hóa bộ lọc manifest delta. Xây dựng tiện ích bóc tách giá trị thực từ cấu trúc `map[string]interface{}` mà thư viện `goavro` sinh ra khi gặp kiểu nullable union (như `["null", "long"]` đối với `added_snapshot_id`). Phân loại chính xác các manifest entry trong `ReadManifestFile`: chỉ nhận các entry có `status == 1 (ADDED)`, bỏ qua `status == 0 (EXISTING)`, và thu thập danh sách file có `status == 2 (DELETED)` để cập nhật đóng `end_snapshot` trong `SyncEngine`.

**Blocked by:**
None (can start immediately)

**Status:**
completed

- [x] Tạo helper function `unwrapAvroUnion(val interface{}) interface{}` để trích xuất giá trị thực từ map union của `goavro`.
- [x] Áp dụng `unwrapAvroUnion` để đọc trường `added_snapshot_id` trong `ReadManifestList`, chỉ lấy đúng các manifest file thuộc snapshot hiện tại.
- [x] Tách kết quả của `ReadManifestFile` thành 2 danh sách rõ ràng: `AddedFiles []ManifestDataFile` và `DeletedFiles []string`.
- [x] Bỏ qua hoàn toàn các manifest entry có `status == 0 (EXISTING)`.
- [x] Viết unit test cho `unwrapAvroUnion` và `ReadManifestList`/`ReadManifestFile` xác nhận không bị đọc lặp file cũ.
