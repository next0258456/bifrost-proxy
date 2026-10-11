# 01: hard-01-centralized-pg-and-deadcode-cleanup

**What to build:**
Loại bỏ hardcode thông số kết nối PostgreSQL trong SyncEngine bằng cách sử dụng cấu hình tập trung `cfg.PGConnString`, đồng thời xóa bỏ hoàn toàn các đoạn code chết (dead code) không còn được sử dụng (`SyncToDuckLake` và các câu lệnh static format dư thừa).

**Blocked by:**
None (can start immediately)

**Status:**
completed

- [x] Lệnh `ATTACH ... AS ducklake_catalog (TYPE POSTGRES)` trong `engine/duckdb.go` sử dụng trực tiếp cấu hình `cfg.PGConnString`.
- [x] Xóa bỏ hoàn toàn hàm mồ côi `func (e *SyncEngine) SyncToDuckLake(...)` trong `engine/duckdb.go`.
- [x] Loại bỏ lệnh `fmt.Sprintf` dư thừa trên static string tại `engine/duckdb.go`.
- [x] Toàn bộ unit test và integration test trong repo pass 100%.
