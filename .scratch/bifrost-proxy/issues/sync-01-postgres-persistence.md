# Sync-01: Synchronous Catalog Persistence (Postgres & CAS)

**What to build:** Proxy kết nối với PostgreSQL (`polaris` DB trong container `postgres:15`) bằng thư viện Go `pgx`. Khi nhận HTTP `POST /v1/namespaces/{ns}/tables/{table}` từ Redpanda, kiểm tra điều kiện Optimistic Concurrency Control (CAS), ghi đè con trỏ `metadata_location` vào bảng `catalog_tables` đồng bộ trong < 2ms, và trả lời `200 OK` cho Redpanda ngay (< 15ms). Endpoint `GET /v1/namespaces/{ns}/tables/{table}` đọc từ Postgres để lấy file MinIO trả về.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [x] Cài đặt driver PostgreSQL `github.com/jackc/pgx/v5`.
- [x] Khởi tạo connection pool tới Postgres và tự động tạo bảng `catalog_tables(namespace, table_name, metadata_location, previous_metadata_location, updated_at)`.
- [x] Triển khai hàm `SaveTablePointer` và `GetTablePointer` trong tầng storage/db.
- [x] Cập nhật endpoint `POST` và `GET` trong `api/catalog.go` sử dụng Postgres con trỏ.
- [x] Viết unit/integration test xác nhận commit đồng bộ lưu thành công vào Postgres và trả về < 15ms.
