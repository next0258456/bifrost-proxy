# Sync-04: Row-Level Deletes (Delete v2) & Schema Evolution Mapping

**What to build:** Nâng cấp Worker để xử lý các tính năng nâng cao của Iceberg v2:
- Khi gặp file Position Delete (`content = 1`), bóc trường `2147483546` để liên kết chính xác với `data_file_id` và ghi vào `ducklake_delete_file`.
- Khi có `ALTER TABLE` (thêm/sửa cột), ghi nhận phiên bản cột mới vào `ducklake_column` (`start_snapshot`, `end_snapshot`).
- Viết End-to-End test kiểm chứng việc truy vấn qua DuckDB/Superset tự động loại trừ dòng bị DELETE và hiển thị đúng cột mới.

**Blocked by:** Sync-03: Incremental Parquet Data Files & Snapshot Changes Sync

**Status:** ready-for-agent

- [ ] Phân tích file manifest tìm các delete files (`content = 1`), trích xuất field `2147483546` lấy `referenced_data_file_id`.
- [ ] Ghi nhận vào bảng `ducklake_delete_file` liên kết với data file gốc.
- [ ] Xử lý schema changes: Thêm dòng mới vào `ducklake_column` khi thêm cột, update `end_snapshot` khi xóa cột.
- [ ] Viết test End-to-End: Thao tác DELETE và ALTER TABLE từ Postgres được phản ánh chính xác trong các bảng metadata của DuckLake.
