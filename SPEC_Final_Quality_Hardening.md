# Đặc tả Kỹ thuật: Hoàn thiện Chuẩn hóa Lỗi, UUID & Kiểm thử E2E Toàn diện (SPEC_Final_Quality_Hardening.md)

*Labels: ready-for-agent, code-quality-hardening, error-handling, e2e-testing*

## Problem Statement

Mặc dù 3 ticket `hard-01`, `hard-02`, và `hard-03` đã giải quyết thành công việc cấu hình tập trung PostgreSQL, xóa bỏ dead code, triển khai bộ mapper kiểu dữ liệu Iceberg và cô lập schema theo namespace, đợt rà soát `/code-review` độc lập vừa qua đã phát hiện 4 thiếu sót kỹ thuật còn tồn đọng:
1. **Vi phạm chuẩn xử lý lỗi (Swallowed Errors):** Trong `engine/incremental.go`, các lệnh `QueryRow` khi tra cứu và cấp phát ID cho schema/table/data_file đang dùng toán tử `_ =` để nuốt lỗi, vi phạm nghiêm trọng chuẩn *"Tuyệt đối không nuốt lỗi"* đã quy định trong `SPEC_Fixes_and_Hardening.md`.
2. **Hardcode Schema UUID rỗng:** Schema UUID khi chèn vào `ducklake_schema` đang bị gán cứng giá trị nil `'00000000-0000-0000-0000-000000000000'` thay vì tự sinh UUID v4 hợp lệ.
3. **Ánh xạ kiểu `fixed[L]` chưa toàn diện:** Mapper kiểu `fixed` mới chỉ kiểm tra so khớp chuỗi chính xác `raw == "fixed"`, bỏ sót các kiểu Iceberg có tham số kích thước như `fixed[16]`.
4. **Kiểm thử E2E bỏ sót xác thực trường `timestamp`:** Script `test_e2e_pyiceberg_ducklake.py` đã tạo schema và nạp dữ liệu cho trường `created_at` (`TimestampType`), nhưng câu lệnh SQL `SELECT` đối chiếu ở cả Iceberg và DuckLake lại bỏ sót cột này (`SELECT order_id, customer_name, amount, tax`).

## Solution

Thực hiện một đợt chuẩn hóa chất lượng cuối cùng (Quality & Reliability Hardening):
1. **Xử lý triệt để lỗi cơ sở dữ liệu:** Loại bỏ toàn bộ `_ = e.db.QueryRow(...)`, bắt buộc kiểm tra lỗi và trả về lỗi rõ ràng (hoặc log cảnh báo có ngữ cảnh khi là truy vấn fallback có kiểm soát).
2. **Tự sinh UUID chuẩn cho Schema:** Sử dụng `uuid.New().String()` khi tạo mới bản ghi trong `ducklake_schema`.
3. **Mở rộng Type Mapper:** Bổ sung regex hoặc prefix match cho `fixed` (ví dụ `strings.HasPrefix(raw, "fixed")`) ánh xạ về `blob`.
4. **Bổ sung `created_at` vào câu truy vấn E2E:** Cập nhật câu lệnh `SELECT order_id, customer_name, amount, tax, created_at` trong `test_e2e_pyiceberg_ducklake.py` để chứng minh toàn bộ dữ liệu thời gian microsecond được ghi nhận và truy vấn chính xác qua DuckLake.

## User Stories

1. As a Backend Engineer, I want all database queries in `SyncDeltaIncremental` to strictly check and handle errors, so that unexpected database failures or schema corruptions are instantly surfaced rather than silently suppressed.
2. As a Data Architect, I want newly created DuckLake schemas to possess a unique, valid UUID v4, so that multi-schema metadata follows DuckLake specification standards.
3. As a Data Engineer ingesting cryptographic hashes or binary IDs stored as `fixed[16]`, I want the type mapper to accurately recognize and map them to `blob`, so that table columns are preserved without syntax parsing errors.
4. As a QA Engineer, I want the E2E verification test to explicitly query and assert the `created_at` timestamp field across both the Iceberg and Pure DuckLake paths, proving end-to-end data fidelity for temporal data.

## Implementation Decisions

1. **Chuẩn hóa Xử lý Lỗi Tầng Database:**
   - Trong `engine/incremental.go`: Thay thế các câu lệnh `_ = e.db.QueryRow(...)` bằng biến `err`. Nếu gặp lỗi không mong muốn (khác `sql.ErrNoRows`), lập tức trả về lỗi có bọc ngữ cảnh `fmt.Errorf(...)`.
2. **Tạo UUID cho Schema:**
   - Sử dụng `uuid.New().String()` từ package `github.com/google/uuid` (đã có sẵn trong go.mod) khi sinh UUID cho `ducklake_schema`.
3. **Bổ sung Prefix Matching cho `fixed` Type:**
   - Cập nhật hàm `MapIcebergTypeToDuckLake`:
     ```go
     case raw == "binary" || strings.HasPrefix(raw, "fixed"):
         return "blob"
     ```
4. **Cập nhật SQL Query trong E2E Script:**
   - Cập nhật `SELECT` trong `test_e2e_pyiceberg_ducklake.py` để lấy và đối chiếu cả 5 trường: `order_id, customer_name, amount, tax, created_at`.

## Testing Decisions

- **Single Test Seam:**
  - Tiếp tục kiểm thử blackbox qua HTTP boundary `POST /v1/namespaces/{ns}/tables/{table}` kết hợp truy vấn SQL qua `ATTACH 'ducklake:postgres:...'`.
- **Unit Tests:**
  - Bổ sung test case `fixed[16]` và `fixed[32]` vào `TestMapIcebergTypeToDuckLake` trong `engine/incremental_test.go`.
- **E2E Test Execution:**
  - Chạy `python test_e2e_pyiceberg_ducklake.py` thành công với đầy đủ cả 5 cột, xác nhận dữ liệu `created_at` giữa PyIceberg và DuckLake khớp 100%.

## Out of Scope

- Di chuyển toàn bộ hệ thống sang sử dụng PostgreSQL sequence thay cho `MAX(...) + 1` (do DuckLake core có cơ chế snapshot cas nội tại).

## Further Notes

Bản đặc tả này đóng lại toàn bộ các phát hiện còn sót lại từ `/code-review`, đưa codebase của Bifrost Proxy đạt chuẩn chất lượng sản xuất (Production-Ready).
