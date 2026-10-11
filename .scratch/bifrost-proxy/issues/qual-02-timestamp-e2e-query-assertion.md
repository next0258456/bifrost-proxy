# 02: qual-02-timestamp-e2e-query-assertion

**What to build:**
Cập nhật kịch bản kiểm thử E2E `test_e2e_pyiceberg_ducklake.py` truy vấn đầy đủ cả 5 trường (`order_id`, `customer_name`, `amount`, `tax`, `created_at`), xác nhận tính toàn vẹn của dữ liệu temporal `TimestampType` khi đọc qua Native DuckLake PostgreSQL backend.

**Blocked by:**
01: qual-01-database-error-handling-and-schema-uuid

**Status:**
completed

- [x] Cập nhật các câu lệnh `SELECT` trong `test_e2e_pyiceberg_ducklake.py` ở cả 2 luồng (Iceberg Scan và Native DuckLake) để truy vấn cả trường `created_at`.
- [x] In ra và đối chiếu toàn bộ 5 trường dữ liệu giữa Iceberg và Native DuckLake.
- [x] Chạy kiểm thử `python test_e2e_pyiceberg_ducklake.py` thành công 100%.
- [x] Docker build và toàn bộ test suite pass 100%.
