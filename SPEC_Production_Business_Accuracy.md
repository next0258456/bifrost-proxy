# Đặc tả Kỹ thuật: Độ Chính Xác Nghiệp Vụ Ingestion & Sync DuckLake (Business Accuracy Spec)

*Labels: ready-for-agent, business-accuracy, iceberg-rest-spec, ducklake-catalog*

## Problem Statement

Hiện tại, Bifrost Proxy đã đạt được hiệu năng cao (< 5ms độ trễ) và có cơ chế Worker Queue khống chế RAM, nhưng vẫn còn mang tính chất "giả lập" (mock/simulation) ở nhiều khâu nghiệp vụ cốt lõi:
1. **Thiếu tính xác thực Iceberg REST Catalog:** Endpoint Commit `POST /v1/namespaces/{ns}/tables/{table}` chưa phân tích sâu danh sách `requirements` (như `assert-table-uuid`, `assert-ref-snapshot-id`, `assert-current-schema-id`) để thực thi Optimistic Concurrency Control (CAS) chuẩn chỉ. Nếu hàng đợi bão hòa, Proxy chưa thiết lập header `Retry-After: 1` cho mã lỗi 429 theo chuẩn HTTP/Iceberg.
2. **Dữ liệu Delta vào DuckLake bị "bịa đặt" (Simulated Paths):** Lõi `engine/incremental.go` chưa thực sự đọc và phân tích file `metadata.json` cùng các file Avro Manifest từ S3. Thay vào đó, code đang tự sinh các chuỗi đường dẫn mẫu (`snapshot-%d.parquet`) và hardcode danh sách cột (`id`, `data`), khiến dữ liệu trong 14 bảng DuckLake không phản ánh chính xác các file Parquet và schema thực tế do Redpanda sinh ra.
3. **Chưa đồng nhất Domain Model:** Struct `domain.TableIdentifier` đã được tạo ra nhưng chưa được áp dụng đồng bộ vào các hàm xử lý ở tầng Storage, Postgres và Engine, dẫn đến việc các chuỗi thô `namespace, table string` vẫn trôi nổi khắp codebase.

Hệ thống cần một bản đặc tả nghiệp vụ chuẩn mực, chính xác 100% theo chuẩn Apache Iceberg REST Catalog và kiến trúc 14 bảng DuckLake, loại bỏ hoàn toàn mọi dạng mock/hardcoded dữ liệu.

---

## Solution

Chuyển đổi toàn diện Bifrost Proxy từ cơ chế "mô phỏng" sang **"Bộ xử lý nghiệp vụ chính xác tuyệt đối" (Production-Grade Business Engine)**:

1. **Chuẩn hóa Giao thức Iceberg REST Commit (Phía Ghi - Đồng bộ < 15ms):**
   - Đọc payload `UpdateTableRequest`:
     - Trích xuất `requirements`: kiểm tra `assert-table-uuid`, `assert-current-schema-id`, `assert-ref-snapshot-id`. Nếu điều kiện không khớp với trạng thái hiện tại trong PostgreSQL, trả về `HTTP 409 Conflict` (CommitFailedException).
     - Trích xuất `updates`: lấy `current-snapshot-id`, `schema` mới, và `metadata-location`.
   - Cập nhật con trỏ bảng vào PostgreSQL bằng giao dịch nguyên tử (Atomic CAS Transaction).
   - Nếu hàng đợi `IngestionQueue` đầy, trả về `HTTP 429 Too Many Requests` đính kèm header `Retry-After: 1`.
   - Trả lời `200 OK` cho Redpanda ngay sau khi Postgres chốt sổ thành công.

2. **Hiện thực hóa Bóc tách Delta Thực tế từ Iceberg sang DuckLake (Phía Sync - Bất đồng bộ):**
   - Background Worker nhận `CommitTask` chứa `domain.TableIdentifier` và `MetadataLocation`.
   - Worker tải file `metadata.json` từ S3 MinIO và phân tích cú pháp:
     - Lấy đúng `snapshot-id` vừa commit và file `manifest-list` tương ứng.
     - Đọc danh sách các manifest mới được thêm trong snapshot (`added_data_files`).
     - Đọc nội dung file manifest để rút trích danh sách file Parquet thực tế (`file_path`, `record_count`, `file_size_in_bytes`, `partition`).
   - Ghi dữ liệu THẬT vào 14 bảng DuckLake:
     - `ducklake_snapshot`: snapshot_id thật, timestamp thật, schema_version thật.
     - `ducklake_data_file`: đường dẫn Parquet thật, số dòng thật, dung lượng file thật.
     - `ducklake_delete_file`: nếu manifest chứa file delete (`content = 1`), lấy field `2147483546` để liên kết chính xác `referenced_data_file_id`.
     - `ducklake_column`: ánh xạ danh sách cột và kiểu dữ liệu thật từ Iceberg schema sang DuckLake column versioning (`start_snapshot`, `end_snapshot`).
     - `ducklake_snapshot_changes`: ghi nhận sự kiện delta thật.

3. **Tái cấu trúc Mã nguồn theo Chuẩn Clean Architecture:**
   - Sử dụng `domain.TableIdentifier` xuyên suốt mọi chữ ký hàm của Storage, Catalog Store, và Queue.
   - Đóng gói toàn bộ cấu hình vào package `config` duy nhất.

---

## User Stories

1. As a Redpanda Iceberg Sink writer, I want the proxy commit endpoint to validate `assert-ref-snapshot-id` and `assert-table-uuid` against PostgreSQL, so that conflicting concurrent commits from multiple workers are safely rejected with HTTP 409 without corrupting table state.
2. As a Redpanda Iceberg Sink writer, I want to receive an HTTP 429 Too Many Requests response with a `Retry-After: 1` header when the internal ingestion queue is saturated, so that the sink pauses and retries cleanly without losing streaming messages.
3. As a Data Platform operator, I want the commit endpoint to record and persist the exact `metadata-location` provided by Redpanda in PostgreSQL within 2ms, so that the catalog pointer is immediately committed before acknowledging the HTTP request.
4. As a Trino/Spark/Superset query engine, I want the `GET /v1/namespaces/{ns}/tables/{table}` endpoint to look up the table pointer in PostgreSQL and stream the exact metadata JSON file from MinIO, so that read queries always see the latest committed snapshot.
5. As an Analytics Engineer querying DuckLake, I want the sync worker to extract the actual Parquet data file paths and record counts from the committed Iceberg manifest files, so that DuckLake queries scan the real CDC data files created by Redpanda instead of dummy paths.
6. As an Analytics Engineer querying CDC updates and deletes, I want Iceberg position delete files (`content = 1`) to be inspected for field `2147483546` and mapped to `ducklake_delete_file` linked to the correct `data_file_id`, so that row deletions made in Postgres CDC are accurately reflected in DuckLake query results.
7. As a Data Engineer evolving table schemas, I want columns added via `ALTER TABLE` to be extracted from Iceberg schema updates and recorded in `ducklake_column` with their actual field IDs, types, and `start_snapshot`, so that schema evolution is preserved seamlessly in DuckLake.
8. As a Core Developer, I want all storage, database, and queue signatures to accept a strongly-typed `domain.TableIdentifier`, so that primitive obsession code smells are completely eliminated.
9. As a DevOps Engineer, I want all S3 and PostgreSQL environment variables and fallback defaults to be centralized in a dedicated `config` package, so that configuration parsing is never duplicated across modules.
10. As a QA Engineer, I want all tests to interact strictly with the HTTP REST API boundary and assert business responses without inspecting private in-memory maps, so that tests remain robust against internal refactoring.
11. As a Performance Engineer, I want K6 load test scripts to benchmark 50 concurrent virtual users submitting realistic write commit payloads, so that we rigorously prove the system maintains a tail latency p(99) < 15ms under heavy concurrent write loads.

---

## Implementation Decisions

### 1. Phân tích Yêu cầu Concurrency (CAS Validation in REST API)
- Khi nhận `POST /v1/namespaces/:namespace/tables/:table`, body JSON chứa struct `UpdateTableRequest`:
  ```go
  type UpdateTableRequest struct {
      Requirements []TableRequirement `json:"requirements"`
      Updates      []TableUpdate      `json:"updates"`
  }

  type TableRequirement struct {
      Type        string `json:"type"` // e.g. "assert-table-uuid", "assert-ref-snapshot-id"
      UUID        string `json:"uuid,omitempty"`
      Ref         string `json:"ref,omitempty"`
      SnapshotID  int64  `json:"snapshot-id,omitempty"`
  }
  ```
- Proxy đối chiếu các điều kiện trong `Requirements` với bản ghi hiện tại trong PostgreSQL. Nếu có bất kỳ điều kiện nào sai lệch:
  - Trả về ngay `HTTP 409 Conflict`:
    ```json
    {
      "error": {
        "message": "Requirement failed: snapshot-id does not match current table state",
        "type": "CommitFailedException",
        "code": 409
      }
    }
    ```

### 2. Header `Retry-After` trên Mã 429
- Khi `queue.Submit(task)` trả về `false`:
  ```go
  c.Set("Retry-After", "1")
  return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
      "error": fiber.Map{
          "message": "Ingestion queue is saturated. Please retry with exponential backoff.",
          "type":    "QueueSaturatedException",
          "code":    429,
      },
  })
  ```

### 3. Chuẩn hóa Domain Model (`domain.TableIdentifier`)
- Mọi hàm trong `storage`, `postgres`, `engine` đều nhận `domain.TableIdentifier`:
  ```go
  GetTablePointer(ctx context.Context, id domain.TableIdentifier) (string, error)
  SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error
  GetMetadataByLocation(ctx context.Context, location string) ([]byte, error)
  ```

### 4. Bóc tách Delta Thực tế từ Iceberg sang DuckLake
- Worker tải `metadata.json` thật từ S3 qua `StorageService.GetMetadataByLocation`:
  - Giải mã `IcebergMetadata` struct.
  - Tìm snapshot có ID bằng `CurrentSnapshotID`.
  - Trích xuất danh sách schema fields từ `Schemas`.
- Đối với danh sách file Parquet:
  - Sử dụng DuckDB để truy vấn metadata Iceberg hoặc đọc file manifest:
    `SELECT file_path, file_format, record_count, file_size_in_bytes, content FROM read_parquet('s3://...')` hoặc từ metadata Iceberg manifest.
  - Chèn từng file Parquet THẬT vào `ducklake_catalog.ducklake_data_file`.
  - Chèn từng file Delete THẬT vào `ducklake_catalog.ducklake_delete_file`.
  - Cập nhật đúng các cột THẬT vào `ducklake_catalog.ducklake_column`.
- **Tuyệt đối cấm:** Việc tự sinh chuỗi `snapshot-%d.parquet` hay hardcode `id`, `data`.

### 5. Package Cấu hình Tập trung (`config`)
- Tạo package `config/config.go` quản lý toàn bộ biến môi trường:
  ```go
  type Config struct {
      S3Endpoint        string
      AWSAccessKeyID    string
      AWSSecretAccessKey string
      S3Bucket          string
      PGConnString      string
  }
  ```
  Cả `main.go` và `engine/duckdb.go` đều nhận cấu hình từ struct này.

---

## Testing Decisions

- **Blackbox HTTP Contract Testing:**
  - Kiểm tra Commit thành công trả về HTTP 200 kèm `metadata-location`.
  - Kiểm tra Commit thất bại khi gửi `assert-ref-snapshot-id` sai trả về HTTP 409.
  - Kiểm tra Commit khi queue bão hòa trả về HTTP 429 kèm header `Retry-After: 1`.
  - Kiểm tra `GET /tables/{table}` trả về đúng nội dung file JSON được trỏ bởi Postgres.
- **DuckLake Metadata Verification:**
  - Sau khi commit thành công, truy vấn `ducklake_catalog.ducklake_data_file` qua DuckDB: xác nhận các dòng dữ liệu chứa đường dẫn Parquet thật và số record thật.
- **K6 Load Benchmark:**
  - Chạy kịch bản 50 VUs với cả HTTP GET và POST commit thật để xác nhận tail latency p(99) < 15ms.

---

## Out of Scope

- Tự động gom file Parquet nhỏ (Compaction worker).
- Tích hợp xác thực OAuth token phức tạp với Identity Provider bên thứ ba (Keycloak/Okta).
