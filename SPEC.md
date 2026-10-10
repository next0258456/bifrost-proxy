# Bifrost Proxy - Specification

*Labels: ready-for-agent*

## Problem Statement

Hệ thống cần ingest dữ liệu từ các luồng Streaming như Redpanda và phục vụ truy vấn tốc độ cao. Tuy nhiên:
- Nếu dùng thuần DuckLake sẽ đòi hỏi phải tự viết và bảo trì toàn bộ hệ thống Ingestion (Kafka Consumers, Parquet Writers, Offset Managers) cực kỳ khổ sở và khó bảo trì.
- Nếu dùng thuần Iceberg, tốc độ truy vấn sẽ bị cản trở và chậm lại do phải xử lý lớp metadata JSON/Avro cồng kềnh.

Cần một giải pháp "Treo đầu dê Iceberg, bán thịt chó DuckLake" để giải quyết dứt điểm nỗi đau Ingestion mà vẫn giữ được tốc độ truy vấn cực đại.

## Solution

**Bifrost Proxy** - một Trạm Dịch Thuật Đa Ngữ (Rosetta Stone) siêu nhẹ viết bằng Golang đóng vai trò là một Iceberg REST Catalog giả mạo.

Từ vòng ngoài (The Trickster), Proxy hoạt động như một Iceberg Catalog chuẩn, cho phép Redpanda (thông qua Iceberg Sink) sung sướng tự động gom batch, ghi Parquet và đẩy metadata mà không cần cấu hình phức tạp.
Từ vòng trong (The Core Engine), Proxy lén lút sử dụng Embedded DuckDB để đọc Metadata JSON từ Redpanda và chuyển hóa, đồng bộ sang chuẩn DuckLake. Qua đó, cung cấp cả luồng truy vấn Tiêu chuẩn (Iceberg JSON) và luồng Cực Tốc (Native DuckLake) giải phóng người dùng khỏi Vendor Lock-in.

## User Stories

1. As a Data Engineer, I want to use standard Redpanda Iceberg Sink to push data to the proxy, so that I do not have to manually write and maintain Kafka consumers or Parquet writers.
2. As a Data Analyst using Spark/Superset, I want to query the data through standard Iceberg REST API, so that my existing tools work without any modifications.
3. As a Data Analyst using DuckDB, I want to query the data via the Native DuckLake format, so that I can achieve maximum query performance.
4. As a DevOps Engineer, I want the proxy to consume no more than 300MB RAM (Hard Limit) when deployed in a Docker container (including memory allocated for Embedded DuckDB), so that resource costs and footprint are minimized.
5. As a System Administrator, I want the proxy to respond to Auth (Fake Token) requests in `< 5ms`, so that the ingestion process is never delayed by authentication handshakes.
6. As a System Administrator, I want the proxy to have a Cold Start latency of `< 15ms`, a Warm Average Ping of `< 2ms`, and a Max Tail Latency of `< 10ms`, so that the system significantly outperforms the Java/JVM-based Apache Polaris baseline.
7. As a Backend Developer, I want to build the API using Golang, so that I can leverage fast HTTP routing, flexible JSON parsing, and excellent Goroutine I/O concurrency without the steep learning curve of Rust's borrow checker.

## Implementation Decisions

- **Language:** Golang.
- **Web Framework:** Sử dụng framework HTTP siêu nhẹ (Fiber hoặc Gin) để đáp ứng các yêu cầu khắt khe về độ trễ (Latency SLAs).
- **Core Engine:** Nhúng trực tiếp lõi DuckDB (In-process) vào bộ nhớ qua CGO wrapper `go-duckdb`.
- **Data Synchronization:** Khi nhận lệnh Commit, DuckDB sẽ sử dụng các extension `iceberg` và `ducklake` để đọc, validate, và đồng bộ trạng thái của bảng từ Iceberg JSON sang định dạng chuẩn của DuckLake.
- **Storage Layer:** MinIO / S3.
- **API Contracts:** Proxy tuân thủ 100% Iceberg REST Protocol cho các endpoints:
  - `POST /v1/oauth/tokens`: Trả về Fake Token để bypass Auth phức tạp.
  - `GET /v1/namespaces/{ns}/tables/{table}`: Load Iceberg Table.
  - `POST /v1/namespaces/{ns}/tables/{table}`: Giao thức Commit (Compare-And-Swap) để nhận report từ hệ thống Ingestion.

## Testing Decisions

- **Test Seams:** Theo triết lý số lượng seam càng ít càng tốt (lý tưởng là 1), seam cao nhất và duy nhất cho hệ thống này là **HTTP REST API Boundary**. Chúng ta sẽ coi Bifrost Proxy như một hộp đen.
- **Good Tests:** Các bài test sẽ chỉ kiểm chứng external behavior. Sẽ có các test case gửi request (như Redpanda đang gọi `POST /v1/oauth/tokens` và `POST .../tables/{table}`) vào HTTP endpoints của proxy. Test sẽ assert HTTP Response có chuẩn Iceberg không, đồng thời kiểm tra state (dữ liệu/cấu trúc file) trong môi trường MinIO/S3 giả lập để đảm bảo DuckLake format đã được sync chính xác. Không viết test gắn chặt (couple) với các internal functions của Go.
- **Modules Tested:** API Routing Layer, Embedded DuckDB Integration, Storage I/O.
- **Prior Art/Tools:** Sử dụng `httptest` (chuẩn của Go) để mock HTTP request, và `Testcontainers` (hoặc mock S3 server) để kiểm chứng kết quả thực tế ở Storage layer.

## Out of Scope

- Triển khai cơ chế Authentication thật bằng OAuth2/OIDC phức tạp (hệ thống hiện cố tình trả về Fake Token).
- Xây dựng Query Engine/Execution Engine hoàn chỉnh (Proxy chỉ đóng vai trò Trạm Dịch Thuật/Gateway).
- Xây dựng Custom Connectors cho Redpanda/Kafka.

## Further Notes

- Bifrost Proxy sinh ra với sứ mệnh "phế truất" Apache Polaris. Mọi giới hạn tài nguyên và Latency SLAs được đề cập (như khởi động lạnh <15ms, RAM <300MB) là **Hard constraints**. Mã nguồn cần tối ưu Memory Allocation ngay từ đầu.
