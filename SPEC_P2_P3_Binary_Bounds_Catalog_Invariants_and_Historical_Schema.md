# Đặc tả Kỹ thuật: Giải mã Binary Bounds, Đồng bộ Bộ đếm Snapshot & Phục hồi Schema Lịch sử (SPEC_P2_P3_Binary_Bounds_Catalog_Invariants_and_Historical_Schema.md)

*Labels: ready-for-agent, ducklake-invariants, binary-bounds, zone-map-pruning, historical-schema, clean-code, fail-fast*

## Problem Statement

Sau khi hoàn thành đợt gia cố Phase 1 & Phase 2, các bài kiểm thử End-to-End và kiểm thử tải trọng k6 đều đạt 100% SLA. Tuy nhiên, quy trình Code Review hai trục (Standards & Spec) đã phát hiện các điểm nghẽn kỹ thuật sâu ở tầng biểu diễn dữ liệu nhị phân và tính toàn vẹn trạng thái:

1. **Lỗi Giải Mã Bounds Nhị Phân (Binary Bounds Deserialization Defect):**
   - Theo đặc tả Iceberg Single-Value Serialization (v1 & v2), thông tin chặn dưới (`lower_bounds`) và chặn trên (`upper_bounds`) trong Avro Manifest không được lưu dưới dạng chuỗi UTF-8, mà được mã hóa nhị phân dạng little-endian (đối với `int`, `long`, `float`, `double`, `date`, `timestamp`, `timestamptz`) hoặc two's-complement big-endian (đối với `decimal`).
   - Việc proxy sử dụng `utf8.Valid(b)` để kiểm tra và ép kiểu sang string khiến toàn bộ bounds của các cột số, ngày tháng, thời gian và số thực bị coi là chuỗi không hợp lệ và chuyển thành `NULL`. Hệ quả: DuckLake Catalog mất hoàn toàn khả năng lọc phân vùng và loại bỏ file (Zone Map Pruning / Min-Max file skipping), buộc DuckDB phải quét toàn bộ các file Parquet (Full Table Scan).

2. **Lệch Pha Bộ Đếm Snapshot và ID Thực Thể Catalog (Catalog Counter Invariants Drift):**
   - Bảng `ducklake_snapshot` duy trì các bộ đếm đơn điệu `next_catalog_id` và `next_file_id` theo nguyên tắc tiến trình DuckLake. Tuy nhiên, các hàm cấp phát ID thực tế (`schema_id`, `table_id`, `data_file_id`, `delete_file_id`) lại truy vấn `MAX(id) + 1` độc lập.
   - Khi có nhiều snapshot liên tiếp hoặc có tác vụ xóa file, các bộ đếm trong `ducklake_snapshot` có nguy cơ tách rời và không phản ánh chính xác ID tiếp theo của hệ thống catalog, vi phạm các ràng buộc bất biến (invariants) của DuckLake PostgreSQL Catalog.

3. **Lệch Pha Schema Lịch Sử Trong Luồng Tự Phục Hồi (Catch-Up Historical Schema Drift):**
   - Khi worker phát hiện khoảng trống snapshot (gap) và kích hoạt cơ chế tự phục hồi (Self-Healing History Catch-Up), hàm xử lý luôn sử dụng `meta.CurrentSchemaID` để đối chiếu schema và nạp cột.
   - Nếu bảng đã trải qua tiến hóa lược đồ (Schema Evolution - ví dụ: thêm cột mới ở snapshot 5), các snapshot cũ trong quá khứ (snapshot 1, 2) khi được sync bù lại bị áp dụng schema mới nhất, gây sai lệch lịch sử cấu trúc dữ liệu của các snapshot cũ.

4. **Nguy Cơ Crash Parser Khi Gặp Kiểu Lạ & Mùi Code Tiềm Ẩn:**
   - Bộ chuyển đổi kiểu `MapIcebergTypeToDuckLake` trả về nguyên bản chuỗi chưa biết (`default: return raw`) thay vì fallback an toàn về `varchar`, gây nguy cơ crash DuckDB parser nếu gặp kiểu custom/extension.
   - Các mùi code đã được Standards Reviewer ghi nhận: `Feature Envy` ở hàm cấp phát ID, `Data Clumps` ở 3 bộ đếm snapshot, trùng lặp phân tích delimiter của struct, và nuốt lỗi tiềm ẩn tại `ParseMetadataFile`.

---

## Solution

Triển khai giải pháp toàn diện giải mã chuẩn nhị phân Iceberg, đồng bộ bộ đếm catalog và quản lý schema theo snapshot lịch sử:

1. **Bộ Giải Mã Nhị Phân Iceberg Bounds (Iceberg Binary Bounds Decoder):**
   - Xây dựng module giải mã `DecodeIcebergBinaryBound(rawBytes []byte, icebergType string) (string, error)` tuân thủ nghiêm ngặt Iceberg Single-Value Serialization Spec:
     - `int` / `int32`: 4 bytes little-endian signed integer.
     - `long` / `int64`: 8 bytes little-endian signed integer.
     - `float` / `float32`: 4 bytes little-endian IEEE 754 floating point.
     - `double` / `float64`: 8 bytes little-endian IEEE 754 floating point.
     - `boolean`: 1 byte (`0x00` = false, `0x01` = true).
     - `date`: 4 bytes little-endian signed int biểu diễn số ngày kể từ mốc Epoch `1970-01-01` -> định dạng chuỗi `YYYY-MM-DD`.
     - `time`: 8 bytes little-endian signed int biểu diễn microsecond từ nửa đêm -> định dạng chuỗi `HH:MM:SS.ffffff`.
     - `timestamp` / `timestamptz`: 8 bytes little-endian signed int biểu diễn microsecond kể từ Epoch -> định dạng chuẩn ISO-8601.
     - `string`: Kiểm tra UTF-8 hợp lệ.
     - `uuid`: 16 bytes nhị phân định dạng thành canonical UUID string v4.
     - `decimal(P, S)`: Giải mã big-endian two's-complement unscaled value chia cho $10^S$.
   - Liên kết kiểu trường (`icebergType`) từ danh sách trường của bảng để giải mã chính xác min/max bounds trước khi ghi vào `ducklake_file_column_stats` (`min_value`, `max_value`).

2. **Thống Nhất Bộ Đếm Đơn Điệu DuckLake Catalog:**
   - Gom các bộ đếm thành cấu trúc `SnapshotCounters { SchemaVersion, NextCatalogID, NextFileID }`.
   - Đồng bộ hóa chặt chẽ giữa việc cấp phát ID cho thực thể mới (`schema`, `table`, `data_file`, `delete_file`) và việc ghi nhận bộ đếm kế tiếp vào `ducklake_snapshot`, bảo đảm `next_catalog_id` luôn trỏ tới ID catalog kế tiếp và `next_file_id` trỏ tới ID file kế tiếp.

3. **Phục Hồi Chính Xác Schema Theo Snapshot Lịch Sử:**
   - Trong quá trình đồng bộ (cả snapshot hiện tại lẫn snapshot bù lịch sử trong catch-up), xác định schema hoạt động tương ứng với snapshot đó:
     - Ưu tiên trường `schema-id` được chỉ định bên trong Snapshot Entry.
     - Nếu snapshot không có `schema-id`, tìm schema hợp lệ có hiệu lực tại thời điểm snapshot được tạo.
     - Chỉ sử dụng `CurrentSchemaID` làm fallback cuối cùng khi không có thông tin lịch sử schema.

4. **Hoàn Thiện Bộ Type Mapper & Dọn Dẹp Mã Nguồn (Clean Code Standards):**
   - Chuyển `default` trong `MapIcebergTypeToDuckLake` fallback an toàn về `"varchar"` (kèm cảnh báo log), đảm bảo an toàn tuyệt đối cho DuckDB parser.
   - Refactor `allocateNextID` thành hàm nhận trực tiếp `tx *sql.Tx` để xóa bỏ `Feature Envy`.
   - Trích xuất hàm tách delimiter dùng chung cho parser kiểu `struct`.
   - Xóa bỏ việc nuốt lỗi tại `ParseMetadataFile`, bọc lỗi tường minh với `%w`.

---

## User Stories

1. As a Data Platform Engineer, I want numeric and timestamp column bounds from Iceberg manifests to be correctly decoded from little-endian binary into canonical strings in DuckLake, so that DuckDB can perform file pruning (Min-Max skipping) and accelerate analytical queries.
2. As a Data Analyst, I want floating-point min/max statistics to preserve exact IEEE-754 precision in DuckLake column statistics, so that range queries (`WHERE price BETWEEN 10.5 AND 99.9`) filter out irrelevant Parquet data files instantly.
3. As a Data Reliability Engineer, I want catalog entity identifiers (`data_file_id`, `delete_file_id`, `table_id`) to stay strictly aligned with `next_catalog_id` and `next_file_id` in `ducklake_snapshot`, so that the catalog state remains 100% compliant with DuckLake invariant specifications.
4. As an Analytics Engineer, I want date and timestamp bounds to be formatted as ISO-8601 strings rather than raw byte strings or NULLs, so that temporal filter pushdown functions seamlessly in BI tools like Superset and DuckDB.
5. As a System Operator, I want historical snapshots replayed during self-healing catch-up to resolve against their originating schemas rather than the current latest schema, so that column metadata reflects the true historical state of the table.
6. As a DevOps Engineer, I want unknown or third-party extension types to safely fallback to `varchar` instead of passing raw invalid syntax, so that DuckDB's SQL catalog parser never crashes on exotic data types.
7. As a Code Reviewer, I want `ParseMetadataFile` to return errors explicitly using Go `%w` wrapping, so that file corruption or missing metadata paths are never silently ignored.
8. As a Maintainer, I want `allocateNextID` to operate cleanly on database transactions without unnecessary receiver bindings, so that the codebase eliminates Feature Envy and adheres to Fowler clean code principles.
9. As a Maintainer, I want struct field parsing logic in the recursive type mapper to use a single unified delimiter parser, so that duplicate code between `:` and space separators is eliminated.
10. As a Reliability Engineer, I want the full test suite including unit tests and E2E PyIceberg multi-snapshot assertions to verify decoded bounds and file skipping, so that regressions in metadata decoding are caught before production deployment.

---

## Implementation Decisions

### 1. Module Giải Mã Nhị Phân Iceberg Bounds (Iceberg Binary Bounds Decoder)
- Xây dựng hàm giải mã nhị phân chuyên biệt nhận vào mảng byte thô (`[]byte`) và chuỗi kiểu dữ liệu Iceberg chuẩn (`string`).
- **Quy tắc giải mã theo kiểu:**
  - **Số nguyên (`int`, `integer`):** Đọc 4 bytes theo thứ tự `binary.LittleEndian.Uint32`, ép kiểu sang `int32` và chuyển thành chuỗi thập phân (`strconv.FormatInt(int64(v), 10)`).
  - **Số nguyên 64-bit (`long`, `bigint`):** Đọc 8 bytes theo thứ tự `binary.LittleEndian.Uint64`, ép kiểu sang `int64` và chuyển thành chuỗi thập phân.
  - **Số thực đơn (`float`):** Đọc 4 bytes `binary.LittleEndian.Uint32`, giải mã qua `math.Float32frombits`, định dạng chuỗi với độ chính xác tiêu chuẩn (`strconv.FormatFloat(float64(v), 'g', -1, 32)`).
  - **Số thực kép (`double`):** Đọc 8 bytes `binary.LittleEndian.Uint64`, giải mã qua `math.Float64frombits`, định dạng chuỗi với `strconv.FormatFloat(v, 'g', -1, 64)`.
  - **Boolean (`boolean`):** Kiểm tra byte đầu tiên (`0x00` -> `"false"`, `0x01` -> `"true"`).
  - **Date (`date`):** Đọc 4 bytes `int32` biểu diễn số ngày tính từ `1970-01-01`. Cộng vào `time.Unix(0, 0).UTC()` và format dạng `2006-01-02`.
  - **Timestamp (`timestamp`, `timestamptz`):** Đọc 8 bytes `int64` biểu diễn microseconds tính từ Epoch. Chuyển đổi thành `time.UnixMicro(us).UTC()` và format dạng `2006-01-02 15:04:05.000000`.
  - **UUID (`uuid`):** Đọc 16 bytes trực tiếp qua bộ phân tích UUID chuẩn để sinh chuỗi canonical.
  - **Chuỗi (`string`, `varchar`):** Xác thực `utf8.Valid(b)` và trả về chuỗi text trực tiếp.
  - **Decimal (`decimal(P, S)`):** Đọc mảng byte big-endian bù hai (two's-complement), chuyển thành `big.Int` và chia tỉ lệ theo hệ số $10^S$.

### 2. Truyền Kiểu Cột Từ Manifest & SchemaEntry Sang Bộ Đọc Bounds
- Khi đọc `ManifestDataFile`, thu thập từ điển ánh xạ `columnID -> icebergType` từ `activeSchema.Fields`.
- Bộ trích xuất `extractColumnBoundsStats` gọi hàm giải mã nhị phân kèm theo kiểu tương ứng của cột đó. Nếu không tìm thấy kiểu cột trong schema, fallback kiểm tra UTF-8 hoặc trả về định dạng hex an toàn.

### 3. Đồng Bộ Bộ Đếm Snapshot Với Cấp Phát ID Catalog
- Định nghĩa struct `SnapshotCounters`:
  ```go
  type SnapshotCounters struct {
      SchemaVersion int64
      NextCatalogID int64
      NextFileID    int64
  }
  ```
- Hàm nạp bộ đếm từ snapshot liền kề:
  ```go
  SELECT COALESCE(MAX(schema_version), 0), COALESCE(MAX(next_catalog_id), 0), COALESCE(MAX(next_file_id), 0)
  FROM ducklake_catalog.ducklake_snapshot;
  ```
- Khi cấp phát `schema_id`, `table_id`, `data_file_id`, `delete_file_id`, các giá trị ID được cấp phát tăng dần bắt đầu từ bộ đếm cơ sở.
- Giá trị lưu vào `ducklake_snapshot` của snapshot hiện tại sẽ là:
  - `next_catalog_id = max(current_next_catalog_id, last_allocated_catalog_id + 1)`
  - `next_file_id = max(current_next_file_id, last_allocated_file_id + 1)`
  - `schema_version = current_schema_version + (1 nếu có schema evolution)`

### 4. Truy Cứu Schema Cho Snapshot Lịch Sử Trong Tự Phục Hồi (Catch-Up)
- Bổ sung logic tìm schema cho snapshot cụ thể:
  ```go
  func resolveSchemaForSnapshot(meta *IcebergMetadata, snap *SnapshotEntry) *SchemaEntry
  ```
- Nếu `snap.SchemaID` có giá trị và tìm thấy trong `meta.Schemas`, sử dụng schema đó.
- Nếu không có, tìm schema có `schema-id` nhỏ nhất nhưng tạo trước hoặc cùng lúc với `snap.TimestampMs`.
- Fallback về schema đầu tiên hoặc `CurrentSchemaID` nếu không tìm thấy schema nào khác.

### 5. Dọn Dẹp Mã Nguồn (Code Smells Refactoring)
- Chuyển `allocateNextID(tx *sql.Tx, table, idCol string) (int64, error)` thành package function độc lập, xóa bỏ receiver `(e *SyncEngine)`.
- Trích xuất hàm phân tách cặp trường trong kiểu `struct`:
  ```go
  func parseStructField(part string) (fieldName, fieldType string, ok bool)
  ```
- Trong `MapIcebergTypeToDuckLake`, nhánh `default` trả về `"varchar"`.
- Bắt lỗi tường minh khi gọi `ParseMetadataFile` và trả về qua `%w`.

---

## Testing Decisions

### 1. Unit Tests Cho Bộ Giải Mã Nhị Phân (Binary Bounds Unit Tests)
- Kiểm thử toàn diện hàm giải mã với các fixture byte thực tế của Iceberg:
  - `int32`: Chặn dưới 101 (`[101 0 0 0]`) $\rightarrow$ `"101"`.
  - `int64`: Chặn dưới 10000000000 $\rightarrow$ `"10000000000"`.
  - `float32`: Chặn dưới $250.5$ $\rightarrow$ `"250.5"`.
  - `double`: Chặn dưới $999.99$ $\rightarrow$ `"999.99"`.
  - `date`: Chặn dưới 19680 (ngày 2023-11-20) $\rightarrow$ `"2023-11-20"`.
  - `timestamp`: Chặn dưới Epoch microsecond $\rightarrow$ chuỗi timestamp chuẩn ISO.
  - `boolean`: `0x01` $\rightarrow$ `"true"`.
  - `string`: Chuỗi UTF-8 `"CongAnh"`.

### 2. Kiểm Thử Bộ Đếm Snapshot & ID Không Bị Lệch Pha
- Chạy liên tiếp 3 commit snapshot chứa cả data file và delete file.
- Truy vấn `ducklake_snapshot` và xác nhận `next_catalog_id` và `next_file_id` luôn lớn hơn mọi `table_id`, `schema_id`, `data_file_id`, `delete_file_id` đã được ghi nhận.

### 3. Kiểm Thử Phục Hồi Schema Lịch Sử
- Tạo metadata giả lập chứa 2 schema: Schema 0 (2 cột ban đầu) và Schema 1 (thêm 2 cột mới).
- Snapshot 1001 gắn với Schema 0; Snapshot 1002 gắn với Schema 1.
- Kích hoạt catch-up sync từ Snapshot 1002 và kiểm tra:
  - Bản ghi data file của Snapshot 1001 trong `ducklake_column` chỉ có 2 cột của Schema 0.
  - Bản ghi của Snapshot 1002 cập nhật đầy đủ 4 cột của Schema 1.

### 4. Kiểm Thử End-to-End Tích Hợp & Zone Map Pruning
- Chạy `test_e2e_pyiceberg_ducklake.py`:
  - Ghi 2 batches bằng PyIceberg.
  - Kết nối DuckDB qua `ducklake_reader` với `READ_ONLY true`.
  - Truy vấn kiểm tra `ducklake_file_column_stats`: xác nhận `min_value` và `max_value` của cột `order_id` (kiểu long) chứa giá trị chuỗi số thật (`101`, `105`), cột `amount` (float64) chứa số thực chuẩn xác (`12.0`, `999.99`), thay vì `NULL`.
  - Chạy câu lệnh truy vấn có lọc min/max: `SELECT * FROM prod_lake.bronze.orders WHERE amount > 500.0;` và xác nhận kết quả chính xác tuyệt đối.

---

## Out of Scope

- Tự động phân tích sâu nội dung bên trong file Parquet trên S3 khi manifest thiếu toàn bộ metadata bounds.
- Hỗ trợ các kiểu hình học (Geometry/Geography) chưa nằm trong chuẩn Iceberg REST spec v2.

---

## Further Notes

Đặc tả này xử lý triệt để các tồn dư kỹ thuật được chỉ ra bởi Code Review, đảm bảo lớp metadata nạp vào DuckLake không chỉ chuẩn về tính toàn vẹn (ACID, RBAC, Invariants) mà còn đạt chuẩn tối ưu hóa truy vấn phân tích (Zone Map File Skipping) tương đương với engine native C++ `iceberg_to_ducklake`.
