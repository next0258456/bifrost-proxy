# Fix-05: Real Delta Parquet & Snapshot Changes Sync

**What to build:** Xóa bỏ toàn bộ các hằng số giả lập (`1, 100, 1000`). Background worker bóc tách delta manifest thật: trích xuất danh sách các file Parquet mới (`added_data_files`), số dòng, kích thước file, và thực thi câu lệnh SQL chèn dữ liệu thật vào `ducklake_data_file` và `ducklake_snapshot_changes` với `snapshot-id` thật.

**Blocked by:** Fix-04: Backpressure Queue & Real Snapshot ID Extraction

**Status:** ready-for-agent

- [x] Viết logic parse delta manifest từ S3/MinIO để rút trích danh sách file Parquet mới.
- [x] Loại bỏ hằng số giả lập trong `engine/incremental.go`.
- [x] Thực thi câu lệnh SQL `INSERT INTO ducklake_catalog.ducklake_data_file` với path, record count, file size thật.
- [x] Viết test xác nhận dữ liệu trong `ducklake_data_file` khớp với file Parquet thực tế.
