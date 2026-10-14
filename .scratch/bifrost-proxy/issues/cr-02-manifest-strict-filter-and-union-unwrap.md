# cr-02: cr-02-manifest-strict-filter-and-union-unwrap

**What to build:**
Chuẩn hóa logic xử lý manifest trong `engine/manifest.go`:
1. Định nghĩa hằng số định danh trường `const IcebergReferencedDataFilePathFieldID = 2147483546`.
2. Đổi tên toàn bộ biến viết tắt khó hiểu (`aFiles`, `dFiles`, `mdf`) sang tên mô tả rõ nghĩa (`addedFiles`, `deletedFiles`, `manifestDataFile`).
3. Chuyển đổi bộ lọc trong `ReadManifestFile` sang kiểm tra nghiêm ngặt: chỉ `status == 1 (ADDED)` mới được thêm vào danh sách file mới, `status == 2 (DELETED)` được thêm vào danh sách đóng snapshot, và `status == 0 (EXISTING)` bị bỏ qua. Các status không hợp lệ khác bị bỏ qua mà không tự ý đưa vào danh sách thêm mới.
4. Áp dụng `unwrapAvroUnion` cho giá trị trích xuất từ cấu trúc Map trong `extractRefPath` (thay vì ép kiểu byte trực tiếp).
5. Xóa bỏ hoàn toàn lệnh `continue` khi đọc hoặc phân tích manifest trong `ExtractDataFilesFromStorage`: nếu có lỗi I/O hoặc lỗi giải mã trên bất kỳ manifest con nào, lập tức fail-fast và trả về lỗi.

**Blocked by:**
None (can start immediately)

**Status:**
completed

- [x] Khai báo hằng số `IcebergReferencedDataFilePathFieldID = 2147483546` và thay thế toàn bộ magic numbers trong `manifest.go`.
- [x] Chuẩn hóa tên biến sang `addedFiles`, `deletedFiles`, `manifestDataFile`.
- [x] Cập nhật nhánh kiểm tra `status == 1` chặt chẽ trong `ReadManifestFile`.
- [x] Bổ sung `unwrapAvroUnion` khi bóc tách giá trị từ Map trong `extractRefPath`.
- [x] Trả về `fmt.Errorf` ngay khi đọc hoặc phân tích manifest file con thất bại trong `ExtractDataFilesFromStorage`.
