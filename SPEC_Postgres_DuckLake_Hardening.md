# Đặc tả Kỹ thuật: Đồng bộ Metadata DuckLake Trực tiếp PostgreSQL & Dọn dẹp Mã nguồn (SPEC_Postgres_DuckLake_Hardening.md)

*Labels: ready-for-agent, ducklake-postgres, code-cleanup, schema-evolution*

## Problem Statement

Hiện tại, hệ thống Bifrost Proxy đã bước đầu hỗ trợ ghi metadata DuckLake vào PostgreSQL backend thay vì tệp cục bộ `.ducklake`. Tuy nhiên, qua quá trình rà soát `/code-review`, hệ thống vẫn còn tồn tại các rủi ro kỹ thuật và sai lệch chuẩn kiến trúc:
1. **Vi phạm chuẩn Centralized Config:** Chuỗi kết nối PostgreSQL dùng để đính kèm catalog (`ATTACH '...' AS ducklake_catalog`) trong DuckDB SyncEngine đang bị gán cứng thông số container, bỏ qua biến môi trường cấu hình tập trung `PG_CONN_STRING`.
2. **Tàn dư mã nguồn chết (Dead Code) & Mùi mã nguồn (Code Smells):** Các hàm mẫu cũ như `SyncToDuckLake` (gọi lệnh không tồn tại `CALL iceberg_to_ducklake`) vẫn còn sót lại trong codebase; logic ánh xạ kiểu dữ liệu bị lặp lại và viết inline thiếu chặt chẽ.
3. **Thiếu hỗ trợ toàn diện kiểu dữ liệu Iceberg:** Engine chỉ mới ánh xạ 7 kiểu dữ liệu nguyên thủy cơ bản. Khi gặp bảng chứa các trường `timestamp`, `timestamptz`, `date`, `decimal`, hoặc `uuid`, DuckLake sẽ nhận chuỗi thô chưa chuẩn hóa và có thể gây lỗi truy vấn khi client DuckDB attach.
4. **Cô lập Namespace và An toàn Đa luồng:** Tên bảng khi đăng ký vào `ducklake_table` chưa kết hợp đầy đủ `namespace` dẫn đến nguy cơ xung đột giữa các schema (`bronze.sales` vs `silver.sales`), và việc cấp phát mã định danh `table_id`, `data_file_id` dựa trên `MAX(...) + 1` chưa có cơ chế sequence an toàn.

## Solution

Tái cấu trúc và hoàn thiện toàn diện lõi DuckLake PostgreSQL Backend:
1. Thống nhất sử dụng cấu hình tập trung từ `config.Config` cho toàn bộ các kết nối PostgreSQL (kể cả lệnh `ATTACH` trong DuckDB engine).
2. Xóa bỏ 100% dead code (`SyncToDuckLake` và các câu lệnh `fmt.Sprintf` dư thừa), tách riêng module mapper chuẩn hóa toàn bộ kiểu dữ liệu Iceberg sang DuckLake SQL types.
3. Bổ sung hỗ trợ đầy đủ các kiểu dữ liệu phong phú (`timestamp`, `timestamptz`, `date`, `time`, `decimal`, `uuid`, `binary`).
4. Chuẩn hóa đăng ký bảng theo định danh namespace (`domain.TableIdentifier`) để tránh xung đột bảng.
5. Cập nhật và tinh gọn kịch bản kiểm thử E2E, loại bỏ việc sleep tĩnh và polling xác nhận đồng bộ metadata.

## User Stories

1. As a Data Platform Engineer, I want the DuckDB SyncEngine to attach PostgreSQL using the centralized `cfg.PGConnString`, so that the service can run seamlessly across local, staging, and production environments without changing code.
2. As a Data Platform Engineer, I want dead code and unused legacy functions (such as `SyncToDuckLake`) to be completely purged, so that the codebase remains lean, maintainable, and free of confusion.
3. As an Analytics Engineer, I want tables with timestamps, dates, decimals, and UUID columns to be properly mapped to DuckLake column types, so that DuckDB and Superset can query analytical datasets with full fidelity.
4. As a Data Engineer writing schemas across multiple tiers (bronze, silver, gold), I want table registration in `ducklake_table` to respect the full `namespace.table` identifier, so that tables with identical names in different namespaces do not collide.
5. As a QA Engineer, I want the end-to-end integration test to deterministically verify that PyIceberg write commits are immediately queryable via `ATTACH 'ducklake:postgres:...'` without relying on `time.sleep()`.

## Implementation Decisions

1. **Chuẩn hóa PostgreSQL Connection String:**
   - Trong `engine/duckdb.go`, chuyển đổi URI PostgreSQL chuẩn từ `cfg.PGConnString` (dạng libpq/connection string) sang cú pháp `ATTACH '<pg_conn>' AS ducklake_catalog (TYPE POSTGRES);`.
   - Loại bỏ hoàn toàn các chuỗi kết nối hardcode `host=postgres port=5432`.

2. **Dọn dẹp Dead Code & Tái cấu trúc Mapper (Refactoring):**
   - Xóa bỏ hoàn toàn hàm `SyncToDuckLake` khỏi `engine/duckdb.go` và interface liên quan.
   - Trích xuất logic chuyển đổi kiểu dữ liệu thành một hàm pure converter `MapIcebergTypeToDuckLake(icebergType string) string` hỗ trợ đầy đủ:
     - `long` -> `int64`
     - `int`, `integer` -> `int32`
     - `short` -> `int16`
     - `string` -> `varchar`
     - `double` -> `float64`
     - `float` -> `float32`
     - `boolean` -> `boolean`
     - `timestamp`, `timestamptz` -> `timestamp with time zone` (hoặc `timestamp`)
     - `date` -> `date`
     - `time` -> `time`
     - `uuid` -> `uuid`
     - `binary`, `fixed` -> `blob`
     - `decimal(...)` -> giữ nguyên định dạng `decimal(p, s)`

3. **Đăng ký Bảng có Namespace trong DuckLake Catalog:**
   - Khi insert vào `ducklake_catalog.ducklake_table`, lưu trữ `table_name` rõ ràng hoặc liên kết đúng `schema_id` tương ứng với `namespace`, đảm bảo không trùng lặp khi nhiều namespace có bảng cùng tên.

4. **Đồng bộ Thông tin Snapshot Changes:**
   - Hoàn thiện câu lệnh ghi nhật ký vào `ducklake_catalog.ducklake_snapshot_changes` với các thông số audit thật từ Iceberg snapshot summary (thay vì toàn bộ `NULL`).

## Testing Decisions

- **Single Test Seam (HTTP REST Boundary & End-to-End DuckLake Queries):**
  - Giữ vững nguyên tắc single test seam: tất cả các bài test kiểm thử luồng ghi commit qua `POST /v1/namespaces/{ns}/tables/{table}`.
  - Sau khi commit thành công, client DuckDB độc lập thực hiện `ATTACH 'ducklake:postgres:...' AS lake` và truy vấn trực tiếp bảng bằng SQL chuẩn để đối chiếu kết quả.
- **Loại bỏ Flaky Sleep trong Kiểm thử E2E:**
  - Thay thế `time.sleep(1.0)` bằng polling có timeout tối đa 3 giây để kiểm tra sự xuất hiện của metadata trong Postgres catalog.

## Out of Scope

- Triển khai compaction worker tự động gộp file Parquet nhỏ.
- Hỗ trợ transaction DDL phân tán giữa Iceberg và PostgreSQL.

## Further Notes

Đặc tả này tập trung vào việc làm sạch (code hardening), loại bỏ nợ kỹ thuật (technical debt), và hoàn thiện tính đúng đắn cho cơ chế DuckLake PostgreSQL Backend vừa mới được áp dụng.
