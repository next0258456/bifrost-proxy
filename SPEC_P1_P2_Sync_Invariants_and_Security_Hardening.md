# Đặc tả Kỹ thuật: Khắc phục Điểm gãy P1 & P2 - Bọc Transaction Sync, Chuẩn hóa Invariants, Recursive Type Mapper & Phân tách Quyền Truy cập (SPEC_P1_P2_Sync_Invariants_and_Security_Hardening.md)

*Labels: ready-for-agent, p1-hardening, ducklake-invariants, transaction-safety, recursive-type-mapper, self-healing-sync, zero-direct-access*

## Problem Statement

Sau khi các điểm gãy P0 ở tầng giao thức Iceberg REST và bộ lọc Avro được xử lý, hệ thống vẫn đối mặt với các điểm gãy kỹ thuật cấp độ P1 và P2 làm suy giảm tính toàn vẹn dữ liệu, gây rủi ro lệch pha (desync drift) vĩnh viễn và vi phạm kiến trúc bảo mật:

1. **Thiếu Transaction bao bọc luồng Sync, Nuốt lỗi Bước 6 & Rủi ro Silent Drop (Bug 06 - P1):**
   - Trong `SyncEngine.SyncDeltaIncremental`, việc cập nhật 7 bảng của DuckLake Catalog chạy qua các lệnh `db.Exec` rời rạc không có transaction. Khi một lệnh ở bước sau thất bại, các bản ghi đã ghi ở bước trước không được rollback, dẫn đến tình trạng hỏng dữ liệu phân mảnh (partial metadata sync).
   - Truy vấn đọc danh sách cột đang active ở Bước 6 (`ducklake_column`) nuốt lỗi khi kết nối database gặp sự cố, khiến hệ thống chèn trùng lặp toàn bộ danh sách cột.
   - Khi `NewSyncEngine` khởi tạo DuckDB thất bại trong `main.go`, chương trình chỉ in log cảnh báo và tiếp tục chạy với `syncEngine = nil`. Kết quả là worker hút hết task trong queue và âm thầm vứt bỏ (silent drop), khiến hệ thống tưởng chừng vẫn nhận commit nhưng DuckLake catalog không hề được cập nhật.
   - Trong `IngestionQueue`, khi `SyncDeltaIncremental` gặp sự cố nhất thời (transient errors: mạng gián đoạn nhẹ, lock timeout), worker chỉ in log rồi bỏ qua luôn task. Điều này khiến DuckLake bị kẹt lại snapshot cũ và vĩnh viễn mất đồng bộ (desync drift) với Iceberg ở các snapshot kế tiếp.

2. **Chưa hỗ trợ Recursive Type Mapper cho Complex / Nested Types (P1 Hardening):**
   - Hàm `MapIcebergTypeToDuckLake` hiện chỉ xử lý các kiểu dữ liệu nguyên thủy (primitive types) cơ bản.
   - Khi client Iceberg (Spark, Flink, PyIceberg) commit các kiểu dữ liệu nâng cao như mảng lồng nhau (`list<T>`), từ điển (`map<K, V>`), cấu trúc lồng nhau (`struct<field: type, ...>`), thời gian có múi giờ (`timestamptz`), hàm mapping trả về nguyên văn chuỗi thô.
   - Bảng `ducklake_column.column_type` lưu chuỗi thô này khiến DuckDB khi `ATTACH ducklake:postgres...` không thể phân giải cú pháp kiểu dữ liệu tương ứng, dẫn tới lỗi crash câu lệnh SELECT của người dùng phân tích.

3. **Sai lệch Ràng buộc Metadata (Catalog Invariants) của DuckLake (Bug 05 - P2):**
   - Các bộ đếm `schema_version`, `next_catalog_id`, `next_file_id` trong `ducklake_snapshot` đang bị gán cứng cố định `(1, 100, 1000)` thay vì tăng tiến theo từng thay đổi như quy định trong mã nguồn tham chiếu C++ (`iceberg_to_ducklake.cpp`).
   - Cột `changes_made` trong `ducklake_snapshot_changes` đang nhận chuỗi thô từ Iceberg thay vì danh sách token chuẩn của DuckLake (`created_schema:"...",inserted_into_table:...`).
   - Bỏ trống hoàn toàn bảng thống kê cột (`ducklake_file_column_stats`) và thông tin phân vùng (`ducklake_partition_info`), làm vô hiệu hóa khả năng Data Skipping và Pruning của DuckDB khi truy vấn.

4. **Xung đột Kiến trúc "Zero Direct Access" với Cổng 5432 (Bug 07 - P2):**
   - Tài liệu và kịch bản hiện tại hướng dẫn client DuckDB/Superset kết nối trực tiếp vào cổng 5432 bằng tài khoản `admin` của PostgreSQL. Điều này vi phạm nghiêm ngặt nguyên tắc Zero Direct Access quy định trong kiến trúc Data Platform, phơi bày quyền ghi/xóa toàn bộ catalog cho mọi worker phân tích dữ liệu.

## Solution

Triển khai gói giải pháp chuẩn hóa cho các điểm gãy P1 và P2:

1. **Bọc Transaction Nguyên khối cho SyncEngine & Fail-Fast Khởi động:**
   - Bao bọc toàn bộ các lệnh ghi vào bảng DuckLake catalog trong một database transaction duy nhất (`BEGIN ... COMMIT`) với cơ chế `defer tx.Rollback()`. Nếu bất kỳ thao tác nào thất bại, toàn bộ metadata của snapshot đó được rollback sạch sẽ.
   - Bắt buộc kiểm tra lỗi và trả về `fmt.Errorf` tại truy vấn đọc cột active ở Bước 6.
   - Trong `main.go`, nếu khởi tạo `NewSyncEngine` thất bại, lập tức dừng chương trình bằng `log.Fatalf` (Fail-Fast), ngăn chặn triệt để tình trạng silent drop task.
   - Bổ sung cơ chế **Retry với Exponential Backoff** (tối đa 3 lần) cho các lỗi tạm thời và hỗ trợ cơ chế **Catch-up Re-sync**: khi worker nhận snapshot mới, tự động phát hiện snapshot bị thiếu trong DuckLake từ lịch sử `metadata.json` để đồng bộ bù (Self-Healing).

2. **Xây dựng Bộ Recursive Type Mapper Đầy đủ (Iceberg -> DuckLake):**
   - Nâng cấp `MapIcebergTypeToDuckLake` thành bộ phân giải đệ quy:
     - `list<T>` / `array<T>` -> `MapType(T)[]` hoặc `LIST(MapType(T))` chuẩn DuckDB.
     - `map<K, V>` -> `MAP(MapType(K), MapType(V))`.
     - `struct<name: type, ...>` -> `STRUCT(name MapType(type), ...)`.
     - `timestamptz` / `timestamp with time zone` -> `timestamptz`.
     - `json` -> `json`.
     - Fallback an toàn cho các kiểu không xác định về `varchar` để tránh crash DuckDB parser.

3. **Chuẩn hóa Ràng buộc Metadata (DuckLake Catalog Invariants):**
   - Xây dựng cơ chế cập nhật tăng tiến cho các bộ đếm trong `ducklake_snapshot` dựa trên số lượng schema/table và file thực tế được thêm vào snapshot, bám sát logic của `iceberg_to_ducklake.cpp`.
   - Chuẩn hóa định dạng cột `changes_made` theo danh sách token phân tách bằng dấu phẩy: `inserted_into_table:<table_id>`, `deleted_from_table:<table_id>`, `created_table:"<name>"`.
   - Trích xuất thông tin min/max bounds và null counts từ manifest để nạp vào bảng `ducklake_file_column_stats`.

4. **Thiết lập Phân quyền Read-Only cho Tầng Phân tích:**
   - Cấu hình role chuyên biệt `ducklake_reader` trong PostgreSQL chỉ có quyền `SELECT` trên các bảng `ducklake_*` và bảng `catalog_tables`.
   - Cập nhật tài liệu và kịch bản `ATTACH` sử dụng tài khoản `ducklake_reader` kèm cờ `READ_ONLY`, bảo vệ toàn vẹn tài khoản quản trị hạ tầng.

## User Stories

1. As a Data Platform Engineer, I want the proxy process to immediately crash on startup if DuckDB fails to initialize, so that silent ingestion task drops never occur in production.
2. As a Data Reliability Engineer, I want all metadata table updates within a delta sync to be wrapped in an atomic database transaction, so that partial metadata writes are impossible.
3. As a Data Engineer, I want the sync worker to retry transient errors and auto catch-up missing snapshots from Iceberg metadata history, so that DuckLake never permanently desyncs.
4. As an Analytics Engineer, I want complex types (nested structs, lists, maps, timestamptz) to map accurately to DuckDB types, so that querying Iceberg tables via DuckLake never fails on unrecognized column types.
5. As a Query Optimization Specialist, I want column statistics (min/max/nulls) to be ingested into `ducklake_file_column_stats`, so that DuckDB queries can leverage parquet metadata pruning for sub-second execution.
6. As a Lakehouse Architect, I want the metadata snapshots to follow exact DuckLake specification invariants for versioning and changes, so that standard DuckLake catalog tools interpret the catalog without corruption warnings.
7. As a Security Officer, I want BI and query engines (Superset, DuckDB) to connect to the catalog using a dedicated read-only role, so that production catalog tables cannot be tampered with or accidentally dropped.

## Implementation Decisions

1. **Quản lý Transaction trong SyncEngine:**
   - Thay vì dùng `e.db.Exec(...)` trực tiếp, mở transaction:
     ```go
     tx, err := e.db.BeginTx(ctx, nil)
     if err != nil {
         return fmt.Errorf("failed to begin transaction: %w", err)
     }
     defer tx.Rollback()
     // Toàn bộ các thao tác insert/update đều gọi qua tx.Exec(...)
     // Khi hoàn thành tất cả các bước:
     return tx.Commit()
     ```
   - Sửa truy vấn đọc cột active:
     ```go
     rows, err := tx.Query(...)
     if err != nil {
         return fmt.Errorf("failed to query active columns for table %d: %w", tableID, err)
     }
     ```

2. **Chấm dứt Silent Drop & Thêm Worker Self-Healing:**
   - Trong `main.go`:
     ```go
     syncEngine, err := engine.NewSyncEngine(cfg, store)
     if err != nil {
         log.Fatalf("Fatal: failed to initialize DuckDB sync engine: %v", err)
     }
     defer syncEngine.Close()
     ```
   - Trong `engine/queue.go`:
     - Bổ sung vòng lặp retry (3 lần với backoff 100ms, 500ms, 1500ms) khi `SyncDeltaIncremental` gặp lỗi.
     - Khi nhận task mới, kiểm tra snapshot id hiện tại của table trong DuckLake; nếu phát hiện bị lỡ snapshot, lần theo mảng `meta.Snapshots` để sync bù.

3. **Bộ Recursive Type Mapper:**
   - Phân tích cú pháp kiểu dữ liệu lồng nhau:
     ```go
     func MapIcebergTypeToDuckLake(icebergType string) string {
         raw := strings.ToLower(strings.TrimSpace(icebergType))
         switch {
         case strings.HasPrefix(raw, "list<") && strings.HasSuffix(raw, ">"):
             elem := raw[5 : len(raw)-1]
             return MapIcebergTypeToDuckLake(elem) + "[]"
         case strings.HasPrefix(raw, "map<") && strings.HasSuffix(raw, ">"):
             // phân tách key và value
             return fmt.Sprintf("MAP(%s, %s)", keyType, valType)
         case strings.HasPrefix(raw, "timestamptz") || strings.Contains(raw, "with time zone"):
             return "timestamptz"
         // ...
         }
     }
     ```

4. **Chuẩn hóa Tokens `changes_made` & Bộ Đếm Snapshot:**
   - Xây dựng mảng `changes []string`:
     - Khi thêm bảng mới: thêm `fmt.Sprintf("created_table:\"%s\"", tableUUID)`
     - Khi thêm file dữ liệu: thêm `fmt.Sprintf("inserted_into_table:%d", tableID)`
     - Khi có file xóa: thêm `fmt.Sprintf("deleted_from_table:%d", tableID)`
   - Nối thành chuỗi bằng `strings.Join(changes, ",")`.
   - Truy vấn bộ đếm hiện tại từ snapshot liền trước (`SELECT COALESCE(MAX(schema_version), 1), COALESCE(MAX(next_catalog_id), 1), COALESCE(MAX(next_file_id), 1)`) và tăng tiến tương ứng.

5. **Tạo Role Phân Quyền PostgreSQL:**
   - Trong kịch bản khởi tạo database (hoặc migration script):
     ```sql
     CREATE ROLE ducklake_reader WITH LOGIN PASSWORD 'reader_password';
     GRANT USAGE ON SCHEMA ducklake_catalog TO ducklake_reader;
     GRANT SELECT ON ALL TABLES IN SCHEMA ducklake_catalog TO ducklake_reader;
     GRANT SELECT ON catalog_tables TO ducklake_reader;
     ALTER DEFAULT PRIVILEGES IN SCHEMA ducklake_catalog GRANT SELECT ON TABLES TO ducklake_reader;
     ```
   - Cập nhật chuỗi kết nối trong `docs/ARCHITECTURE.md`, `README.md` và `test_e2e_pyiceberg_ducklake.py`.

## Testing Decisions

- **Transactional Rollback Verification:**
  - Viết bài kiểm thử cố tình gây lỗi ở bước nạp schema để xác nhận không có bản ghi mồ côi nào lọt vào `ducklake_snapshot` hay `ducklake_data_file`.
- **Fail-Fast Startup Test:**
  - Kiểm thử truyền cấu hình đường dẫn DuckDB hoặc PostgreSQL sai để khẳng định `main.go` thoát với mã lỗi thay vì chạy ngầm.
- **Recursive Type Mapper Unit Tests:**
  - Kiểm thử các kiểu phức tạp: `list<string>`, `list<int>`, `map<string, double>`, `timestamptz`, `struct`, và fallback types.
- **Worker Retry & Catch-up Test:**
  - Giả lập lỗi transient để kiểm tra retry; giả lập snapshot bị lỡ để xác nhận worker tự động sync bù snapshot cũ trước khi apply snapshot mới.
- **Read-Only Access Test:**
  - Kết nối bằng tài khoản `ducklake_reader` và thử chạy lệnh `INSERT`/`DROP TABLE` để khẳng định bị từ chối quyền truy cập (`permission denied`).

## Out of Scope

- Thay đổi cơ chế lưu trữ Parquet trên S3/MinIO.
- Xây dựng daemon định kỳ nén file (Compaction).

## Further Notes

Đặc tả này hoàn thiện lớp bảo vệ tính toàn vẹn, khả năng tự phục hồi (Self-Healing) và bảo mật chiều sâu cho Bifrost Proxy, đưa hệ thống lên tiêu chuẩn cấp doanh nghiệp (Enterprise Standard).
