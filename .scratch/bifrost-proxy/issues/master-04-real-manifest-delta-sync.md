# Master-04: Real Iceberg Manifest Delta Parsing & DuckLake Inserts

**What to build:** Khai tử hoàn toàn các chuỗi đường dẫn tự bịa (`snapshot-%d.parquet`). Kết nối `ParseMetadataJSON` vào Worker: tải file `metadata.json` thật từ MinIO, đọc manifest thật để lấy danh sách file Parquet thật (đúng đường dẫn, đúng số dòng, đúng file size) và thực thi `INSERT` vào `ducklake_data_file` cùng `ducklake_snapshot_changes`.

**Blocked by:** Master-03: Strict Iceberg CAS Requirements Validation in Postgres

**Status:** ready-for-agent

- [ ] Worker tải `metadata.json` thật từ MinIO qua `StorageService.GetMetadataByLocation`.
- [ ] Parse `IcebergMetadata` và trích xuất danh sách snapshot và summary delta.
- [ ] Chèn các file Parquet THẬT vào `ducklake_catalog.ducklake_data_file`.
- [ ] Ghi nhận sự kiện delta thật vào `ducklake_catalog.ducklake_snapshot_changes`.
- [ ] Xóa bỏ hoàn toàn việc tự sinh đường dẫn mẫu và hằng số hardcode.
