# Master-05: Real Delete v2 Mapping & Dynamic Schema Evolution

**What to build:** Khai tử các cột hardcode (`id`, `data`). Trích xuất danh sách cột và kiểu dữ liệu thật từ Iceberg schema nạp vào `ducklake_column` (`start_snapshot`, `end_snapshot`). Bóc tách file delete (`content = 1`), đọc field `2147483546` để liên kết `referenced_data_file_id` vào bảng `ducklake_delete_file`.

**Blocked by:** Master-04: Real Iceberg Manifest Delta Parsing & DuckLake Inserts

**Status:** ready-for-agent

- [ ] Trích xuất các trường cột thật từ `IcebergMetadata.Schemas` và chèn vào `ducklake_catalog.ducklake_column` với `start_snapshot`.
- [ ] Xử lý file position delete (`content = 1`), đọc field `2147483546` để liên kết với `ducklake_data_file.data_file_id`.
- [ ] Chèn file delete thật vào `ducklake_catalog.ducklake_delete_file`.
- [ ] Xóa bỏ hoàn toàn các cột hardcode mẫu.
