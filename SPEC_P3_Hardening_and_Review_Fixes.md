# Đặc tả Kỹ thuật: Khắc phục Toàn diện Code Review Phase 3 (SPEC_P3_Hardening_and_Review_Fixes.md)

*Labels: ready-for-agent, code-review-hardening, fail-fast, binary-bounds, timestamp-schema, zero-swallowed-errors*

---

## 1. Problem Statement

Sau đợt rà soát Code Review hai trục (Standards & Spec) cho toàn bộ thay đổi Phase 3 (`29ff891...HEAD`), hệ thống đã phát hiện một số vi phạm chuẩn kỹ thuật và sai lệch đặc tả:

1. **Lỗi Nuốt Lỗi Truy Vấn DB Trong Kiểm Tra Bộ Đếm Invariants:**
   - Trong `engine/incremental.go`, các câu truy vấn `SELECT GREATEST(...)` để bảo đảm `next_catalog_id` và `next_file_id` chỉ được bọc trong `if err == nil`. Nếu câu truy vấn lỗi (timeout, mất kết nối), lỗi bị gán đè và âm thầm bỏ qua, vi phạm nghiêm trọng nguyên tắc *Trụ cột 1: Tuyệt đối không nuốt lỗi*.

2. **Lỗi Bỏ Qua Parse Error & Corrupted Bounds:**
   - Trong `engine/manifest.go`:
     - Phân tích scale của decimal dùng `scale, _ = strconv.Atoi(scaleStr)` bỏ qua error qua định danh trống `_`.
     - Bộ trích xuất bounds bỏ qua lỗi giải mã `err` từ `DecodeIcebergBinaryBound` khiến dữ liệu hỏng bị âm thầm lọt qua.
   - Trong `seed_benchmark_minio.py`: Bắt ngoại lệ bằng `except Exception: pass` che giấu lỗi kết nối thực sự tới S3.

3. **Sai Lệch Phục Hồi Schema Lịch Sử Bằng Timestamp:**
   - Trong `engine/incremental.go`, `resolveSchemaForSnapshot` khi gặp snapshot không có trường `schema-id` đã gán cứng `SchemaID == 0` thay vì đối chiếu `snap.TimestampMs` với dòng thời gian tạo schema để tìm schema tương ứng tại thời điểm snapshot ra đời.

4. **Định Dạng Microsecond Của Bounds Kiểu Time Bị Cắt Cụt:**
   - Đặc tả quy định kiểu `time` phải có chuỗi canonical định dạng `HH:MM:SS.ffffff` (luôn đủ 6 chữ số microsecond). Cài đặt hiện tại nếu phần dư microsecond bằng 0 thì chỉ in `%02d:%02d:%02d`, làm mất độ chính xác.

5. **Thiếu Khẳng Định Giá Trị Bounds Cụ Thể Trong E2E Test:**
   - `test_e2e_pyiceberg_ducklake.py` mới chỉ kiểm tra `len(stats_rows) > 0` và lọc `WHERE amount > 100.0`. Cần assert chính xác giá trị chặn trên/chặn dưới (`101`, `105`, `12.0`, `999.99`) và thêm bài test lọc phân vùng `WHERE amount > 500.0`.

---

## 2. Solution & Technical Specifications

### Trụ Cột 1: Loại Bỏ Triệt Để Lỗi Bị Nuốt (Fail-Fast Standards)
- **`engine/incremental.go`:** Bọc mọi câu truy vấn `tx.QueryRow` trong kiểm tra invariants:
  ```go
  if err := tx.QueryRow(queryMaxCatalogID).Scan(&maxCatalogID); err != nil {
      return fmt.Errorf("failed to query max catalog id for invariants: %w", err)
  }
  if err := tx.QueryRow(queryMaxFileID).Scan(&maxFileID); err != nil {
      return fmt.Errorf("failed to query max file id for invariants: %w", err)
  }
  ```
- **`engine/manifest.go`:**
  - Bắt lỗi `strconv.Atoi(scaleStr)` khi phân tích `decimal(P, S)`, nếu lỗi trả về `fmt.Errorf("invalid decimal scale in %q: %w", icebergType, err)`.
  - Ghi log cảnh báo hoặc xử lý rõ ràng khi `DecodeIcebergBinaryBound` trả về lỗi giải mã đối với dữ liệu không hợp lệ.
- **`seed_benchmark_minio.py`:** Kiểm tra cụ thể mã lỗi `BucketAlreadyOwnedByYou` hoặc `BucketAlreadyExists`, không dùng `except Exception: pass`.

### Trụ Cột 2: Phục Hồi Schema Theo Mốc Thời Gian Snapshot (`snap.TimestampMs`)
- Trong `resolveSchemaForSnapshot`:
  1. Nếu snapshot có trường `schema-id` hợp lệ -> sử dụng trực tiếp.
  2. Nếu không có `schema-id`, so khớp `snap.TimestampMs`:
     - Nếu bảng chỉ có 1 schema, sử dụng schema đó.
     - Nếu có nhiều schemas, chọn schema có `schema-id` nhỏ nhất nhưng có hiệu lực trước hoặc tại thời điểm `snap.TimestampMs`. Nếu không có metadata thời gian schema, đối chiếu theo `SnapshotEntry.TimestampMs` gần nhất trước sự kiện tiến hóa lược đồ hoặc fallback về `CurrentSchemaID`.

### Trụ Cột 3: Chuẩn Hóa Canonical Format Kiểu Time
- Trong `engine/manifest.go`, khi giải mã kiểu `time`:
  - Microseconds trong ngày ($0 \le us < 86,400,000,000$).
  - Luôn format chuỗi định dạng đầy đủ:
    ```go
    hours := totalUS / 3600000000
    minutes := (totalUS % 3600000000) / 60000000
    seconds := (totalUS % 60000000) / 1000000
    micro := totalUS % 1000000
    return fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, micro), nil
    ```

### Trụ Cột 4: Khẳng Định Giá Trị Tuyệt Đối & Pruning Trong E2E Test
- Cập nhật `test_e2e_pyiceberg_ducklake.py`:
  - Khẳng định các giá trị min/max cụ thể đã giải mã trong `ducklake_file_column_stats`:
    - `order_id` (cột #1): `min_value == '101'`, `max_value >= '103'`
    - `customer_name` (cột #2): `min_value == 'Bifrost'`, `max_value == 'DuckLake'`
    - `amount` (cột #3): `min_value == '12'`, `max_value == '999.99'`
  - Bổ sung truy vấn kiểm tra cắt giảm file Parquet:
    ```sql
    SELECT order_id, customer_name, amount FROM prod_lake.bronze.orders WHERE amount > 500.0 ORDER BY order_id;
    ```
    Khẳng định trả về đúng 2 bản ghi (`Order #102: Bifrost | 999.99` và `Order #104: Kafka | 777.0`), chứng minh DuckDB áp dụng min/max stats để skip các data file chứa order nhỏ.

---

## 3. Danh Sách Tickets Phân Rã

1. **`p3-05-fail-fast-error-handling-and-clean-code.md`**:
   - Xóa bỏ nuốt lỗi `SELECT GREATEST(...)` trong `engine/incremental.go`.
   - Bắt lỗi parse `decimal(P,S)` và ghi nhận lỗi corrupted bounds trong `engine/manifest.go`.
   - Khắc phục `except Exception: pass` trong `seed_benchmark_minio.py`.
2. **`p3-06-timestamp-schema-resolution-and-time-bounds-format.md`**:
   - Triển khai logic tìm schema lịch sử theo `snap.TimestampMs` trong `resolveSchemaForSnapshot`.
   - Định dạng chuẩn 6 chữ số microsecond `%02d:%02d:%02d.%06d` cho kiểu `time` trong `DecodeIcebergBinaryBound`.
   - Viết unit test kiểm thử schema resolution theo timestamp và time formatting.
3. **`p3-07-e2e-bounds-assertions-and-file-pruning-verification.md`**:
   - Cập nhật `test_e2e_pyiceberg_ducklake.py` assert chính xác giá trị min/max đã decode.
   - Thêm câu truy vấn kiểm tra file pruning `WHERE amount > 500.0`.
   - Chạy full test suite, E2E test và k6 benchmark xác nhận 100% checks.
