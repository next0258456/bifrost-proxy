# Fix-06: Delete v2 (Position Deletes) & Schema Evolution Mapping

**What to build:** Nâng cấp Worker xử lý Delete v2: bóc trường `2147483546` để liên kết chính xác với `data_file_id`, chèn vào `ducklake_delete_file`. Xử lý `ALTER TABLE`: ghi nhận cột mới với `start_snapshot`, cập nhật `end_snapshot` cho cột bị xóa vào bảng `ducklake_column`.

**Blocked by:** Fix-05: Real Delta Parquet & Snapshot Changes Sync

**Status:** ready-for-agent

- [x] Phân tích manifest delta tìm các delete files (`content = 1`), đọc field `2147483546` để lấy `referenced_data_file_id`.
- [x] Ghi nhận vào bảng `ducklake_delete_file` liên kết với data file gốc.
- [x] Xử lý schema changes: Thêm dòng mới vào `ducklake_column` khi thêm cột, update `end_snapshot` khi xóa cột.
- [x] Viết test xác nhận file delete và column evolution được ghi nhận đúng chuẩn vào DuckLake.
