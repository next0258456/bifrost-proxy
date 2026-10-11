# 02: hard-02-complete-type-mapper-and-namespace-isolation

**What to build:**
Mở rộng bộ ánh xạ kiểu dữ liệu Iceberg sang DuckLake SQL types hỗ trợ đầy đủ các kiểu phong phú (`timestamp`, `timestamptz`, `date`, `time`, `decimal`, `uuid`, `blob`), tách thành pure mapper function chuyên trách, đăng ký bảng kèm thông tin schema/namespace để chống va chạm tên bảng, và điền đầy đủ metadata audit vào `ducklake_snapshot_changes`.

**Blocked by:**
01: hard-01-centralized-pg-and-deadcode-cleanup

**Status:**
completed

- [x] Trích xuất hàm `MapIcebergTypeToDuckLake(icebergType string) string` hỗ trợ đầy đủ: `long`, `int`/`integer`, `short`, `string`, `double`, `float`, `boolean`, `timestamp`, `timestamptz`, `date`, `time`, `uuid`, `binary`/`fixed`, `decimal(p,s)`.
- [x] Xử lý đăng ký bảng trong `ducklake_catalog.ducklake_table` và `ducklake_catalog.ducklake_schema` đảm bảo phân tách theo namespace (`domain.TableIdentifier.Namespace`).
- [x] Ghi nhận đúng thông tin audit (operation, snapshot_id) vào `ducklake_catalog.ducklake_snapshot_changes`.
- [x] Viết unit tests kiểm tra toàn diện hàm mapper và việc bảo toàn kiểu dữ liệu.
