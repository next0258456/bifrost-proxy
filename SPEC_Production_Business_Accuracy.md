# Đặc tả Kỹ thuật Tổng hợp: Chuẩn Xác Nghiệp Vụ & Tiêu Chuẩn Thiết Kế Bifrost Proxy (Unified Production Spec)

*Labels: ready-for-agent, unified-master-spec, iceberg-rest-spec, ducklake-catalog, clean-architecture*

## Problem Statement

Mặc dù Bifrost Proxy đã đạt được các chỉ số hiệu năng ấn tượng (độ trễ p(99) = 4.68ms, thông lượng 2,300+ req/s trên 50 người dùng đồng thời), đợt rà soát mã nguồn chuyên sâu (`/code-review`) đã chỉ ra hệ thống vẫn còn hai cụm vấn đề lớn cần giải quyết triệt để:

1. **Sai lệch Nghiệp vụ & Dữ liệu Giả lập (Business Inaccuracy):**
   - **Xác thực Commit lỏng lẻo:** Endpoint `POST /v1/namespaces/{ns}/tables/{table}` chưa phân tích sâu danh sách `requirements` của Iceberg (như `assert-table-uuid`, `assert-ref-snapshot-id`, `assert-current-schema-id`) để thực thi Optimistic Concurrency Control (CAS) chuẩn mực.
   - **Thiếu chuẩn Backpressure:** Khi hàng đợi bão hòa, Proxy trả về HTTP 429 nhưng thiếu header `Retry-After: 1`, khiến các client như Redpanda Iceberg Sink không biết thời gian lùi bước (back off).
   - **Dữ liệu Delta vào DuckLake là đường dẫn giả lập:** Lõi `engine/incremental.go` chưa thực sự đọc và phân tích file `metadata.json` cùng các manifest Avro trên S3. Code vẫn đang tự sinh đường dẫn mẫu (`snapshot-%d.parquet`) và hardcode cột (`id`, `data`), khiến 14 bảng DuckLake không phản ánh đúng dữ liệu CDC thật.

2. **Vi phạm Chuẩn Code & Mùi Thiết kế (Standards Violations & Smells):**
   - **Chưa dùng `TableIdentifier`:** Struct `domain.TableIdentifier` đã được định nghĩa nhưng chưa được đưa vào chữ ký hàm của `StorageService`, `CatalogStore`, và `IngestionQueue`, khiến cặp chuỗi nguyên thủy `(namespace, table string)` vẫn trôi nổi khắp nơi (Primitive Obsession & Data Clumps).
   - **Cấu hình phân mảnh:** Logic đọc biến môi trường S3/MinIO và fallback bị phân tán giữa `main.go` và `engine/duckdb.go` thay vì tập trung trong package `config`.
   - **Lặp code & Feature Envy:** Khối xử lý lỗi 404/500 bị copy-paste 2 lần trong GET handler; handler HTTP tự bóc tách map lồng nhau `payload["updates"]` thay vì ủy quyền cho parser chuyên biệt.
   - **Test chưa đúng kịch bản nghiệp vụ:** Bài test CAS gửi body rỗng thay vì chuỗi pointer lệch; bài test 429 dùng cờ mock thay vì kích hoạt bão hòa buffer thật qua REST API.

Cần một bản đặc tả duy nhất, bao quát toàn diện để đưa Bifrost Proxy đạt chuẩn xác nghiệp vụ 100% và tuân thủ tuyệt đối các chuẩn thiết kế Clean Architecture.

---

## Solution

Tái cơ cấu hệ thống thành một khối thống nhất dựa trên 3 trụ cột:

1. **Trụ cột Nghiệp vụ: Chuẩn hóa Giao thức Iceberg REST & CAS:**
   - Ủy quyền việc bóc tách payload cho typed parser `domain.UpdateTableRequest`: trích xuất danh sách `Requirements` và `Updates`.
   - Kiểm tra CAS chặt chẽ: so sánh `assert-ref-snapshot-id` và `assert-table-uuid` với bản ghi trong PostgreSQL. Nếu sai lệch, trả về ngay `HTTP 409 Conflict` (CommitFailedException).
   - Khi `IngestionQueue` đầy, trả về `HTTP 429 Too Many Requests` đính kèm bắt buộc header `Retry-After: 1`.
   - Endpoint `GET` lấy đúng con trỏ từ Postgres và dùng con trỏ đó để đọc nội dung file metadata từ MinIO.

2. **Trụ cột Đồng bộ: Bóc tách Delta Thực tế sang DuckLake:**
   - Kết nối trực tiếp hàm `ParseMetadataJSON` vào luồng sync của Background Worker.
   - Worker tải file `metadata.json` thật từ MinIO, đọc snapshot hiện tại và manifest list:
     - Trích xuất danh sách file Parquet thật do Redpanda ghi xuống để nạp vào `ducklake_data_file`.
     - Phân tích file delete (`content = 1`), bóc field `2147483546` để liên kết `referenced_data_file_id` vào `ducklake_delete_file`.
     - Trích xuất danh sách cột và kiểu dữ liệu thật từ Iceberg schema để nạp vào `ducklake_column` (`start_snapshot`, `end_snapshot`).
     - Ghi nhận nhật ký delta vào `ducklake_snapshot` và `ducklake_snapshot_changes`.
   - Triệt tiêu 100% các chuỗi đường dẫn tự bịa và cột hardcode.

3. **Trụ cột Kiến trúc & Chuẩn Code:**
   - Áp dụng `domain.TableIdentifier` thay thế toàn bộ tham số chuỗi rời rạc ở mọi interface (`StorageService`, `CatalogStore`, `QueueService`).
   - Tạo package `config/config.go` quản lý tập trung toàn bộ biến môi trường S3, MinIO, và PostgreSQL với giá trị mặc định chuẩn xác.
   - Tinh gọn handler HTTP: dùng hàm tiện ích trả lỗi 404/500 dùng chung, loại bỏ duplicated code.
   - Chuẩn hóa test suite: test CAS bằng chuỗi pointer lệch thật, test 429 bằng cách gửi đồng thời vượt dung lượng buffer hàng đợi thật qua REST API.

---

## User Stories

1. As a Redpanda Iceberg Sink writer, I want the commit endpoint to validate `assert-ref-snapshot-id` and `assert-table-uuid` against PostgreSQL, so that conflicting concurrent commits from multiple workers are safely rejected with HTTP 409 without corrupting table state.
2. As a Redpanda Iceberg Sink writer, I want to receive an HTTP 429 Too Many Requests response with a `Retry-After: 1` header when the internal ingestion queue is saturated, so that the sink pauses and retries cleanly without losing streaming messages.
3. As a Data Platform operator, I want the commit endpoint to record and persist the exact `metadata-location` provided by Redpanda in PostgreSQL within 2ms, so that the catalog pointer is immediately committed before acknowledging the HTTP request.
4. As a Trino/Spark/Superset query engine, I want the `GET /v1/namespaces/{ns}/tables/{table}` endpoint to look up the table pointer in PostgreSQL and stream the exact metadata JSON file from MinIO, so that read queries always see the latest committed snapshot.
5. As an Analytics Engineer querying DuckLake, I want the sync worker to extract the actual Parquet data file paths and record counts from the committed Iceberg manifest files, so that DuckLake queries scan the real CDC data files created by Redpanda instead of dummy paths.
6. As an Analytics Engineer querying CDC updates and deletes, I want Iceberg position delete files (`content = 1`) to be inspected for field `2147483546` and mapped to `ducklake_delete_file` linked to the correct `data_file_id`, so that row deletions made in Postgres CDC are accurately reflected in DuckLake query results.
7. As a Data Engineer evolving table schemas, I want columns added via `ALTER TABLE` to be extracted from Iceberg schema updates and recorded in `ducklake_column` with their actual field IDs, types, and `start_snapshot`, so that schema evolution is preserved seamlessly in DuckLake.
8. As a Core Developer, I want all storage, database, and queue signatures to accept a strongly-typed `domain.TableIdentifier`, so that primitive obsession code smells are completely eliminated.
9. As a Developer, I want all S3 and PostgreSQL environment variables and fallback defaults to be centralized in a dedicated `config` package, so that configuration parsing is never duplicated across modules.
10. As a Core Developer, I want HTTP error responses in `api/catalog.go` to use centralized helper functions, so that repetitive error boilerplate is eliminated.
11. As a Core Developer, I want commit payload parsing to be encapsulated in a typed parser struct in `domain`, eliminating feature envy and map-traversal code smells in the HTTP handler.
12. As a QA Engineer, I want integration tests to test CAS conflict with an actual mismatched pointer string, and backpressure by saturating the actual channel buffer with concurrent requests, ensuring 100% adherence to the single HTTP REST test seam.
13. As a Performance Engineer, I want K6 load test scripts to benchmark 50 concurrent virtual users submitting realistic write commit payloads, ensuring the system maintains a tail latency p(99) < 15ms under heavy concurrent write loads.

---

## Implementation Decisions

### 1. Domain Modeling: `TableIdentifier` & Typed Request Parsers
- Định nghĩa trong `domain/model.go`:
  ```go
  type TableIdentifier struct {
      Namespace string `json:"namespace"`
      Table     string `json:"table"`
  }
  ```
- Cập nhật mọi interface trong hệ thống:
  - `storage.StorageService`:
    - `GetTableMetadata(ctx context.Context, id domain.TableIdentifier) ([]byte, error)`
    - `GetMetadataByLocation(ctx context.Context, location string) ([]byte, error)`
    - `SaveTableMetadata(ctx context.Context, id domain.TableIdentifier, data []byte) error`
  - `storage.CatalogStore`:
    - `GetTablePointer(ctx context.Context, id domain.TableIdentifier) (string, error)`
    - `SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error`
  - `engine.CommitTask`:
    - `Table domain.TableIdentifier`
    - `MetadataLocation string`
    - `SnapshotID int64`

### 2. Typed Iceberg Commit Parser (`domain/iceberg.go`)
- Tạo parser tách biệt khỏi HTTP layer:
  ```go
  type UpdateTableRequest struct {
      Requirements []TableRequirement `json:"requirements"`
      Updates      []TableUpdate      `json:"updates"`
  }

  type TableRequirement struct {
      Type        string `json:"type"` // "assert-table-uuid", "assert-ref-snapshot-id", etc.
      UUID        string `json:"uuid,omitempty"`
      Ref         string `json:"ref,omitempty"`
      SnapshotID  int64  `json:"snapshot-id,omitempty"`
  }

  type TableUpdate struct {
      Action           string `json:"action"` // "set-current-snapshot", "add-snapshot", etc.
      SnapshotID       int64  `json:"snapshot-id,omitempty"`
      MetadataLocation string `json:"metadata-location,omitempty"`
  }
  ```
- Handler `api/catalog.go` chỉ cần gọi `req, err := domain.ParseUpdateTableRequest(c.Body())`, loại bỏ hoàn toàn việc duyệt map thủ công.

### 3. Chuẩn hóa Header `Retry-After: 1` và Error Helpers trong API
- Khi `queue.Submit(task)` trả về `false`:
  ```go
  c.Set("Retry-After", "1")
  return writeError(c, fiber.StatusTooManyRequests, "QueueSaturatedException", "Ingestion queue is saturated. Please retry with backoff.")
  ```
- Tạo helper `writeError(c *fiber.Ctx, status int, errorType, message string) error` để thống nhất cấu trúc JSON lỗi chuẩn Iceberg:
  ```json
  {
    "error": {
      "message": "<message>",
      "type": "<errorType>",
      "code": <status>
    }
  }
  ```

### 4. Package Cấu hình Tập trung (`config/config.go`)
- Tạo package `config`:
  ```go
  type Config struct {
      S3Endpoint        string
      AWSAccessKeyID    string
      AWSSecretAccessKey string
      S3Bucket          string
      PGConnString      string
  }

  func LoadConfig() *Config
  ```
- Cả `main.go` và `engine/duckdb.go` đều dùng `config.LoadConfig()`, thống nhất default `minio:9000` và `postgres:5432/polaris`.

### 5. Hiện thực hóa Delta Manifest Parser & DuckLake Inserts
- Kết nối `ParseMetadataJSON` vào `SyncDeltaIncremental`:
  - Đọc file `metadata.json` qua `StorageService.GetMetadataByLocation`.
  - Phân tích danh sách `Snapshots`: lấy đúng snapshot vừa commit.
  - Phân tích danh sách schema fields thật để cập nhật vào `ducklake_catalog.ducklake_column`.
  - Phân tích manifest để lấy danh sách file Parquet thật nạp vào `ducklake_catalog.ducklake_data_file`.
  - Nếu gặp position delete (`content = 1`), bóc field `2147483546` để nạp vào `ducklake_catalog.ducklake_delete_file`.
- Xóa bỏ triệt để các chuỗi tự sinh `snapshot-%d.parquet`.

---

## Testing Decisions

- **Single Test Seam (HTTP REST Boundary):**
  - Mọi bài test gửi HTTP request qua Fiber app test runner, kiểm tra HTTP status, headers và JSON response.
- **Xác thực Lỗi CAS Chuẩn:**
  - Gửi Commit A với previous pointer hợp lệ -> 200 OK.
  - Gửi Commit B với `previous-metadata-location` sai lệch hoặc requirement snapshot ID không khớp -> Nhận chính xác `409 Conflict`.
- **Xác thực Backpressure Chuẩn:**
  - Khởi tạo queue với buffer nhỏ (ví dụ 1), gửi đồng thời các request vượt ngưỡng buffer -> Xác nhận nhận về `429 Too Many Requests` và header `Retry-After: 1`.
- **K6 Load Benchmark:**
  - 50 Virtual Users gửi commit `POST` đồng thời trong 15s. Xác nhận thông lượng > 2,000 req/s và p(99) < 15ms.

---

## Out of Scope

- Tự động gom file Parquet nhỏ (Compaction worker).
- Tích hợp xác thực OAuth token phức tạp với IdP bên thứ ba (Keycloak/Okta).

---

## Further Notes

Bản đặc tả này thay thế và hợp nhất toàn bộ các tài liệu spec trước đó (`SPEC.md`, `SPEC_SyncToDuckLake.md`, `SPEC_Fixes_and_Hardening.md`), trở thành nguồn chân lý (Source of Truth) duy nhất cho dự án Bifrost Proxy.
