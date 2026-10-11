# Sync-03: Incremental Parquet Data Files & Snapshot Changes Sync

**What to build:** Hiện thực hóa cơ chế $O(\Delta)$ Incremental: Background Worker bóc tách file manifest delta từ Iceberg snapshot mới nhất. Thay vì re-scan toàn bộ bảng, worker chỉ trích xuất danh sách các file Parquet mới (`added_data_files`) và các file bị xóa, sau đó thực thi chèn trực tiếp các bản ghi này vào 3 bảng metadata chuẩn của DuckLake: `ducklake_snapshot`, `ducklake_data_file`, `ducklake_snapshot_changes`.

**Blocked by:** Sync-02: Ingestion Queue & Resource-Tuned DuckDB Worker

**Status:** ready-for-agent

- [x] Viết hàm bóc tách manifest delta từ `metadata.json` lấy danh sách data files mới (`added_data_files`) và số dòng `record_count`.
- [x] Viết câu lệnh SQL Insert delta vào bảng `ducklake_snapshot` và `ducklake_data_file`.
- [x] Ghi nhận nhật ký thay đổi snapshot vào `ducklake_snapshot_changes`.
- [x] Viết Integration test xác nhận việc chèn 1 dòng mới chỉ sinh ra đúng 1 record tương ứng trong `ducklake_data_file`.
