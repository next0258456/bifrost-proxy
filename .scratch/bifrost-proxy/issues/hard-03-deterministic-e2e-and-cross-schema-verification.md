# 03: hard-03-deterministic-e2e-and-cross-schema-verification

**What to build:**
Chuyển đổi kiểm thử E2E sang cơ chế tất định (deterministic polling), loại bỏ toàn bộ `time.sleep` tĩnh, đồng thời bổ sung kịch bản kiểm thử ghi và đọc bảng có các kiểu dữ liệu nâng cao (`timestamp`, `decimal`) phân tách theo nhiều namespace qua DuckLake PostgreSQL backend.

**Blocked by:**
02: hard-02-complete-type-mapper-and-namespace-isolation

**Status:**
completed

- [x] Loại bỏ hoàn toàn `time.sleep` tĩnh trong `test_e2e_pyiceberg_ducklake.py`, thay bằng vòng lặp polling với timeout tất định (exponential/fixed interval có max timeout).
- [x] Bổ sung kịch bản kiểm thử Iceberg schema chứa các trường `timestamp` và `decimal`.
- [x] Xác nhận client DuckDB thực hiện `ATTACH 'ducklake:postgres:...' AS prod_lake` và query trả về dữ liệu chính xác 100% không dùng `iceberg_scan`.
- [x] Docker build và toàn bộ test suite pass 100%.
