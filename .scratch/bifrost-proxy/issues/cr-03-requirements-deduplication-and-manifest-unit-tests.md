# cr-03: cr-03-requirements-deduplication-and-manifest-unit-tests

**What to build:**
Tái cấu trúc và bổ sung bộ kiểm thử đơn vị cô lập:
1. Trong `storage/postgres.go`: Trích xuất hàm trợ năng nội bộ `validateRequirements(reqs []domain.TableRequirement, tableExists bool, loc string, currUUID *string, currSnapID *int64, currSchema *int) error` để dùng chung cho cả `ValidateRequirements` và `CommitTableTransaction`, loại bỏ hoàn toàn 55 dòng mã trùng lặp.
2. Xây dựng tệp unit test `engine/manifest_test.go` độc lập:
   - Kiểm thử `unwrapAvroUnion` và `unwrapInt64` với các kiểu dữ liệu Avro (union null-long, union bytes, map union lồng nhau, primitive thô).
   - Kiểm thử `ReadManifestFile` xác thực phân loại chính xác các trạng thái entry: ADDED (1), EXISTING (0), DELETED (2) và các mã lạ.
   - Kiểm thử trích xuất trường bounds đường dẫn tham chiếu Position Delete cho cả hai dạng cấu trúc: Map và mảng cặp key-value.

**Blocked by:**
cr-02: cr-02-manifest-strict-filter-and-union-unwrap

**Status:**
completed

- [x] Trích xuất hàm `validateRequirements` trong `storage/postgres.go` và cập nhật cả `ValidateRequirements` lẫn `CommitTableTransaction` gọi qua hàm này.
- [x] Tạo mới tệp kiểm thử `engine/manifest_test.go` với các test case độc lập xác nhận giải mã Avro union và bộ lọc status.
- [x] Xác nhận toàn bộ unit test chạy thành công.
