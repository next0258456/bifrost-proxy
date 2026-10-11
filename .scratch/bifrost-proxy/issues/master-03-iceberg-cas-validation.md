# Master-03: Strict Iceberg CAS Requirements Validation in Postgres

**What to build:** Nâng cấp tầng kiểm tra CAS: đối chiếu các điều kiện nghiệp vụ thật từ Iceberg payload (`assert-table-uuid`, `assert-ref-snapshot-id`, `assert-current-schema-id`) với dữ liệu trong PostgreSQL. Nếu sai lệch, lập tức từ chối với mã `HTTP 409 Conflict` (CommitFailedException) kèm thông báo lỗi chi tiết.

**Blocked by:** Master-02: Typed Iceberg Commit Parser & HTTP API Hardening

**Status:** completed

- [x] Mở rộng bảng `catalog_tables` trong Postgres lưu thêm `table_uuid`, `current_snapshot_id`, `current_schema_id`.
- [x] Triển khai hàm xác thực `ValidateRequirements(ctx, id, requirements)` trong `storage/postgres.go`.
- [x] Trả về mã lỗi `HTTP 409 Conflict` (CommitFailedException) khi có bất kỳ requirement nào không thỏa mãn.
- [x] Thực hiện Atomic CAS pointer swap trong PostgreSQL.
