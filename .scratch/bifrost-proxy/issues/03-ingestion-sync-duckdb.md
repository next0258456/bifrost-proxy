# 03: Ingestion Sync Pipeline (Commit API & DuckDB Core)

**What to build:** Linh hồn của Bifrost Proxy. Nhúng in-process DuckDB vào app Golang thông qua `go-duckdb` cùng với các extension `iceberg` và `ducklake`. Triển khai endpoint `POST /v1/namespaces/{ns}/tables/{table}` (giao thức Commit) để nhận payload JSON từ Redpanda, sau đó yêu cầu DuckDB đồng bộ dữ liệu sang chuẩn DuckLake và ghi lại xuống S3.

**Blocked by:** 02: Table Metadata Retrieval (Catalog Read & S3)

**Status:** ready-for-agent

- [ ] Khởi tạo thành công Embedded DuckDB (CGO) và load 2 extensions `iceberg`, `ducklake`.
- [ ] Endpoint `POST` nhận payload Compare-And-Swap chuẩn Iceberg.
- [ ] Thực thi query DuckDB thành công để đồng bộ Iceberg Metadata sang định dạng DuckLake lưu trên S3.
- [ ] End-to-end Test: Gọi HTTP POST, sau đó kiểm tra S3 xác nhận file DuckLake được sinh ra đúng chuẩn.
