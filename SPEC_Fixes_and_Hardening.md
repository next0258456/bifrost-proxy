# Đặc tả Kỹ thuật: Khắc phục & Hoàn thiện Hệ thống Bifrost Proxy (Remediation & Hardening Spec)

*Labels: ready-for-agent, code-review-remediation, production-hardening*

## Problem Statement

Quá trình nghiệm thu mã nguồn (`/code-review`) đã chỉ ra 16 lỗi và thiếu sót mang tính hệ thống (7 vi phạm chuẩn thiết kế/code smells và 9 sai lệch nghiệp vụ) trong đợt triển khai vừa qua. 
Cụ thể:
1. **Lỗi Toàn vẹn Dữ liệu & Tính nhất quán (ACID & CAS):** Cơ chế Compare-And-Swap (CAS) chưa được kích hoạt ở tầng PostgreSQL; hàm ghi con trỏ nuốt chửng lỗi của database; endpoint đọc `GET` bỏ qua kết quả con trỏ vừa tra cứu; và cơ chế xếp hàng (Queue) âm thầm vứt bỏ task khi quá tải mà không báo lỗi cho client.
2. **Dữ liệu Delta Giả mạo (Mocking Leak):** Lõi đồng bộ DuckLake mới chỉ chèn các hằng số giả lập (`1, 100, 1000`) vào `ducklake_snapshot` và chuỗi fake vào `ducklake_snapshot_changes`, chưa thực sự bóc tách danh sách file Parquet thật, chưa xử lý file xóa Delete v2, và chưa cập nhật bảng phiên bản cột `ducklake_column`.
3. **Mùi Code & Vệ sinh Kho lưu trữ:** Trùng lặp logic đọc biến môi trường với các giá trị mặc định xung đột nhau; thiếu đóng gói domain (`TableIdentifier`); và repository bị ô nhiễm bởi các file diff/html rác.

Hệ thống cần một bản đặc tả chi tiết để xử lý dứt điểm toàn bộ 16 điểm yếu này, đưa Bifrost Proxy đạt chuẩn production thực thụ.

---

## Solution

Triển khai gói giải pháp khắc phục toàn diện theo 4 trụ cột:

1. **Trụ cột 1: Chuẩn hóa Catalog Persistence & CAS (Giải quyết lỗi Standards 1, 2, 3 và Spec 3, 5):**
   - Viết lại câu lệnh SQL trong `SaveTablePointer` có điều kiện kiểm tra `previous_metadata_location`. Nếu không khớp, trả về lỗi xung đột.
   - Endpoint `POST` bắt buộc kiểm tra lỗi: nếu Postgres lỗi thì trả về `HTTP 500`, nếu xung đột CAS thì trả về `HTTP 409 Conflict`. Tuyệt đối không nuốt lỗi.
   - Endpoint `GET` sử dụng chính xác chuỗi `metadata_location` đọc từ Postgres để tải file từ S3 MinIO.

2. **Trụ cột 2: Cơ chế Backpressure & An toàn Hàng đợi (Giải quyết lỗi Standards 5, 6, 7 và Spec 6, 7):**
   - Khi `IngestionQueue` bị đầy (hơn 1000 task), Proxy trả về `HTTP 429 Too Many Requests` (hoặc `HTTP 503 Service Unavailable`) kèm header `Retry-After`, chấm dứt việc drop ngầm dữ liệu.
   - Bắt buộc trích xuất đúng `snapshot-id` từ payload commit thay vì bịa snapshot ID bằng `time.Now().UnixMilli()`.
   - Đóng gói struct `TableIdentifier` đại diện cho `(namespace, table)`.
   - Thống nhất hàm tiện ích đọc cấu hình môi trường S3/MinIO.

3. **Trụ cột 3: Bóc tách Delta Incremental Thực tế (Giải quyết lỗi Spec 1, 2, 4):**
   - Bóc tách file delta manifest từ snapshot mới:
     - Lấy danh sách file Parquet mới (`added_data_files`), chèn dòng thật vào `ducklake_data_file`.
     - Phân tích các file Delete v2 (`content = 1`), lấy field `2147483546` để liên kết `referenced_data_file_id`, chèn vào `ducklake_delete_file`.
     - Phân tích schema changes (`add-schema`), cập nhật dòng phiên bản cột vào `ducklake_column`.

4. **Trụ cột 4: Vệ sinh Repository & Chuẩn hóa Kiểm thử Hộp Đen (Giải quyết lỗi Standards 4 và Spec 8, 9):**
   - Xóa bỏ toàn bộ các file rác khỏi Git tracking (`diff.txt`, `full_diff.txt`, `deepwiki.html`, v.v.).
   - Viết lại test suite tuân thủ nguyên tắc Single Test Seam (kiểm thử 100% qua HTTP REST API).
   - Nâng cấp `benchmark.js` để đo đạc SLA tải với 50 Virtual Users bắn `POST` commit đồng thời.

---

## User Stories

1. As a Data Engineer, I want the commit endpoint to reject conflicting writes with HTTP 409 Conflict when the previous metadata pointer does not match, so that concurrent commits from multiple writers never corrupt table state.
2. As a DevOps Engineer, I want the commit endpoint to return HTTP 500 when PostgreSQL fails, so that Redpanda is immediately aware of database failures and can retry rather than silently losing state.
3. As a Data Analyst, I want the table `GET` endpoint to resolve the exact metadata location recorded in PostgreSQL before fetching from S3, so that queries always read the true committed version.
4. As a Data Engineer, I want the proxy to return HTTP 429/503 when the internal ingestion queue is saturated, so that streaming producers apply backpressure instead of experiencing silent data loss.
5. As a System Administrator, I want the DuckLake sync engine to record the exact Iceberg `snapshot-id` from the commit payload, so that snapshot lineages match 1:1 between Iceberg and DuckLake.
6. As a Core Developer, I want newly added Parquet files in a commit to be inserted directly into `ducklake_data_file` with their real paths and row counts, so that DuckLake queries reflect actual ingested data in $O(\Delta)$ time.
7. As a Data Analyst querying CDC data, I want Iceberg position delete files (Delete v2) to be mapped to `ducklake_delete_file` linked via field `2147483546`, so that deleted rows are accurately excluded from DuckLake query results.
8. As a Data Engineer modifying table schemas, I want `ALTER TABLE` changes to generate column version records in `ducklake_column` with `start_snapshot` and `end_snapshot`, so that schema evolution functions transparently without rewriting historical Parquet files.
9. As a Developer, I want configuration defaults for S3/MinIO and Postgres to be unified and parsed through a single helper, so that there are no conflicting connection settings between modules.
10. As a Maintainer, I want clean repository hygiene with no untracked binary diff dumps or scratch logs committed, so that the codebase remains lightweight and auditable.
11. As a QA Engineer, I want integration tests to treat the proxy as a single blackbox HTTP seam, asserting behavior through HTTP responses and verified database states without inspecting internal private maps.
12. As a Performance Engineer, I want the K6 benchmark script to simulate 50 concurrent virtual users submitting `POST` commits, so that we rigorously prove the `< 15ms` SLA under write load.

---

## Implementation Decisions

### 1. PostgreSQL Schema & Atomic CAS Pointer Management
- Bảng `catalog_tables` bổ sung ràng buộc so khớp:
  ```sql
  -- Logic CAS Update
  UPDATE catalog_tables 
  SET metadata_location = $1, 
      previous_metadata_location = $2, 
      updated_at = CURRENT_TIMESTAMP
  WHERE namespace = $3 
    AND table_name = $4 
    AND (metadata_location = $2 OR $2 = '');
  ```
- Nếu không có dòng nào được cập nhật (`RowsAffected() == 0`) khi `$2 != ''`, hàm trả về `ErrCommitConflict` để API trả về `409 Conflict`.

### 2. Domain Encapsulation: TableIdentifier
- Định nghĩa struct `TableIdentifier` chuẩn cho toàn bộ codebase:
  ```go
  type TableIdentifier struct {
      Namespace string
      Table     string
  }
  ```
  Dùng struct này thay thế cho việc truyền rời rạc `namespace, table string` ở mọi chữ ký hàm.

### 3. Backpressure Queue Contract
- Khi gọi `queue.Submit(task)`:
  - Nếu channel chưa đầy: Thêm task, trả về `true`.
  - Nếu channel đầy: Không drop ngầm; trả về `false`.
- Handler HTTP khi nhận `false` sẽ trả về ngay:
  ```json
  HTTP 429 Too Many Requests
  {
      "error": {
          "message": "Ingestion queue is saturated. Please back off.",
          "type": "QueueSaturatedException",
          "code": 429
      }
  }
  ```

### 4. Bóc tách Delta Manifest & DuckLake Catalog Inserts
- Handler trích xuất `snapshot-id` từ `UpdateTableRequest` (trường `snapshot-id` trong mảng `updates`).
- Tầng sync đọc `metadata.json` tương ứng với snapshot này:
  - Lấy `manifest-list` của snapshot.
  - Đọc các manifest được đánh dấu `added`.
  - Tạo câu lệnh SQL:
    ```sql
    INSERT INTO ducklake_catalog.ducklake_data_file 
    (data_file_id, table_id, begin_snapshot, end_snapshot, path, record_count, file_size_in_bytes, partition_id)
    VALUES (...);
    ```
  - Nếu gặp position delete (`content = 1`):
    ```sql
    INSERT INTO ducklake_catalog.ducklake_delete_file 
    (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, record_count, file_size_in_bytes)
    VALUES (...);
    ```

### 5. Repository Cleanup
- Chạy `git rm` xóa các file rác: `.scratch/diff.txt`, `ascii_diff.txt`, `diff.txt`, `diff_utf8.txt`, `full_diff.txt`, `deepwiki.html`.
- Cập nhật `.gitignore` để chặn vĩnh viễn các file `*.txt`, `*.html` rác trong thư mục gốc.

---

## Testing Decisions

- **Single Test Seam (HTTP Boundary):**
  - Mọi bài test chỉ gửi HTTP request (`httptest.NewRequest`) và assert mã HTTP status, header và JSON response.
  - Tuyệt đối không kiểm tra biến private map nội bộ.
- **CAS Verification Test:**
  - Gửi Commit A với previous pointer rỗng -> Thành công (200 OK).
  - Gửi Commit B với previous pointer sai -> Nhận lỗi 409 Conflict.
- **Backpressure Test:**
  - Giảm buffer queue xuống 2, gửi 5 requests đồng thời -> Xác nhận request thứ 3 trở đi nhận 429 Too Many Requests.
- **Benchmark Update:**
  - Sửa `benchmark.js` để kịch bản K6 gửi payload `POST /v1/namespaces/bronze/tables/fmcg_sales` đo lường write latency dưới 50 VU.

---

## Out of Scope

- Triển khai compaction gom file Parquet định kỳ.
- Quản lý phân quyền RBAC/IAM phức tạp của AWS S3.
