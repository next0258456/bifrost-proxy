# Đặc tả Kỹ thuật: Khắc phục Điểm gãy P0 - Cam kết Commit Nguyên khối, Khử Lặp Avro & Sửa Position Deletes (SPEC_P0_Commit_CAS_and_Avro_Manifest_Fixes.md)

*Labels: ready-for-agent, p0-critical, iceberg-protocol, cas-concurrency, avro-delta-sync*

## Problem Statement

Hệ thống Bifrost Proxy hiện đang chịu 4 điểm gãy kỹ thuật cấp độ P0 (Critical) khiến các commit từ Iceberg client bị khóa chết, gây ảo ảnh kiểm thử tải trọng, làm sai lệch logic phân tích Avro manifest và làm hỏng tính toàn vẹn của file xóa (Position Deletes):

1. **Khóa chết Commit từ lần thứ 2 & Ảo ảnh Benchmark (Bug 01):**
   - Khi `previous-metadata-location` không được gửi (hành vi chuẩn của PyIceberg, Redpanda và k6), hàm lưu trữ luôn cố gắng thực hiện `INSERT ... ON CONFLICT DO NOTHING`. Từ request thứ 2 trở đi trên cùng một bảng, câu lệnh không tác động dòng nào (`RowsAffected() == 0`) và lập tức trả về lỗi `409 Conflict`.
   - Kịch bản benchmark trước đây chấp nhận cả `200` lẫn `409`, dẫn đến việc 99.99% request commit thực chất bị từ chối ngay tại cửa mà không hề đi qua hàng đợi đồng bộ (`IngestionQueue`) hay bộ máy chuyển hóa (`SyncEngine`).
2. **Lệch pha trạng thái (Split-Brain) & Race Condition ở luồng Commit (Bug 02):**
   - Kiểm tra điều kiện (`ValidateRequirements`) và ghi nhận commit (`SaveTableCommit`) đang thực thi rời rạc ngoài Database Transaction, tạo ra khe hở tranh chấp dữ liệu (TOCTOU) khi có nhiều request đồng thời.
   - Khi hàng đợi đồng bộ (`IngestionQueue`) bị đầy, hệ thống trả về lỗi `429 Too Many Requests`, nhưng con trỏ trong cơ sở dữ liệu đã bị cập nhật từ dòng lệnh trước đó. Khi client retry theo header `Retry-After`, commit sẽ bị từ chối vĩnh viễn với lỗi `409 Conflict`.
3. **Bẫy giải mã Avro Union & Nhân bản dữ liệu cấp số nhân (Bug 03):**
   - Trường `added_snapshot_id` trong Avro manifest list có kiểu nullable union `["null", "long"]`. Thư viện giải mã trả về kiểu map phức hợp (`map[string]interface{}`) thay vì số nguyên nguyên thủy, khiến việc ép kiểu trực tiếp thất bại và hệ thống luôn đọc toàn bộ lịch sử manifest của bảng thay vì chỉ đọc snapshot hiện tại.
   - Không lọc bỏ các mục có trạng thái `EXISTING` (`status == 0`), đồng thời bỏ qua việc xử lý các mục `DELETED` (`status == 2`), dẫn đến việc toàn bộ file Parquet cũ bị chèn lặp lại sau mỗi snapshot và dữ liệu bị nhân bản vô hạn.
4. **Bóc tách sai trường `2147483546` & Gắn nhầm Position Delete (Bug 04):**
   - Trường `lower_bounds` trong Avro được lưu dưới dạng danh sách các cặp key-value thay vì map trực tiếp, khiến việc trích xuất đường dẫn file được tham chiếu (`ReferencedPath`) luôn rỗng.
   - Cơ chế fallback nguy hiểm tự động gắn Position Delete vào file mới nhất trong bảng (`MAX(data_file_id)`), dẫn đến việc xóa sai dữ liệu của các bản ghi mới.

## Solution

Triển khai gói giải pháp nguyên khối và xử lý triệt để 4 lỗi P0:

1. **Thực thi Commit Nguyên khối với Khóa Hàng (Atomic Transactional CAS):**
   - Gộp toàn bộ quy trình kiểm tra `requirements`, kiểm tra sự tồn tại của bảng, và cập nhật con trỏ bảng vào chung một Database Transaction duy nhất có sử dụng cơ chế khóa bi quan (`SELECT ... FOR UPDATE`).
   - Tự động nhận diện trạng thái bảng: nếu bảng chưa tồn tại thì thực thi tạo mới (`INSERT`), nếu bảng đã tồn tại và thỏa mãn các điều kiện ràng buộc thì thực thi cập nhật (`UPDATE`) bất kể `previousLocation` có được truyền lên hay không.
   - Tích hợp kiểm tra dung lượng hàng đợi `IngestionQueue` ngay trong transaction; nếu hàng đợi đầy, lập tức hủy bỏ (Rollback) transaction để bảo toàn tính nhất quán tuyệt đối giữa Iceberg Pointer và DuckLake Sync.
   - Chuẩn hóa kịch bản benchmark chỉ chấp nhận mã phản hồi HTTP `200` cho các commit thành công.
2. **Chuẩn hóa Bộ Giải mã Avro & Thuật toán Lọc Manifest Delta:**
   - Xây dựng tiện ích bóc tách giá trị từ Avro Union (hỗ trợ unwrap các kiểu union của `goavro`).
   - Lọc chính xác các manifest thuộc về `added_snapshot_id`.
   - Trong từng manifest file: chỉ tiếp nhận các entry có `status == 1 (ADDED)`, bỏ qua hoàn toàn `status == 0 (EXISTING)`, và phân loại các entry `status == 2 (DELETED)` để cập nhật đóng `end_snapshot` cho các file dữ liệu tương ứng.
3. **Giải mã Chính xác Khóa Cột 2147483546 & Xóa bỏ Fallback Nguy hiểm:**
   - Quét mảng key-value trong `lower_bounds` để tìm chính xác khóa số `2147483546` và trích xuất đường dẫn file gốc.
   - Xóa bỏ hoàn toàn nhánh fallback gán Position Delete vào `MAX(data_file_id)`. Nếu không tìm thấy file dữ liệu gốc, hệ thống phải thực hiện dừng khẩn cấp (Fail-Fast) và báo lỗi tính toàn vẹn dữ liệu.

## User Stories

1. As a Data Ingestion Engine (PyIceberg / Redpanda), I want consecutive table commits to execute smoothly without encountering false 409 Conflict errors, so that continuous streaming ingestion proceeds uninterrupted.
2. As a Load Test Engineer, I want the benchmark suite to strictly validate HTTP 200 responses on commits, so that measured latency and throughput accurately reflect real-world end-to-end ingestion capacity.
3. As a Database Administrator, I want commit verification and pointer persistence to execute within an atomic transaction with row-level locks, so that race conditions and split-brain metadata states are completely prevented.
4. As a Lakehouse Platform Engineer, I want the proxy to roll back database pointer changes if the internal ingestion queue is saturated, so that clients can safely retry without triggering unresolvable snapshot conflicts.
5. As a Data Architect, I want the Avro manifest parser to correctly resolve union types and filter out existing entries, so that queries run on DuckLake reflect accurate row counts without exponential file duplication.
6. As an Analytics Engineer, I want deleted files in Iceberg commits to close the active snapshot interval (`end_snapshot`), so that queries traversing DuckLake exclude purged records.
7. As a Reliability Engineer, I want position delete files to accurately bind to their target referenced data files via field 2147483546 or fail fast, so that deletions are never erroneously mapped to unrelated parquet files.

## Implementation Decisions

1. **Giao thức Catalog Store Transactional Boundary:**
   - Mở rộng giao diện Catalog Store để hỗ trợ thực thi commit nguyên khối trong một transaction PostgreSQL (sử dụng `pgx.Tx`).
   - Câu truy vấn kiểm tra bảng và cập nhật:
     ```sql
     SELECT metadata_location, table_uuid, current_snapshot_id, current_schema_id 
     FROM catalog_tables 
     WHERE namespace = $1 AND table_name = $2 
     FOR UPDATE;
     ```
   - Nếu bảng đã tồn tại và vượt qua `ValidateRequirements`, thực hiện `UPDATE` con trỏ và metadata. Nếu chưa tồn tại và có `assert-create` (hoặc không có conflict), thực hiện `INSERT`.
   - Lệnh `queue.Submit` được kiểm tra trước khi `tx.Commit()`. Nếu `queue.Submit` trả về false, tiến hành `tx.Rollback()` và trả về HTTP 429.

2. **Tiện ích Giải mã Avro Union (`unwrapAvroUnion`):**
   - Viết helper hàm nhận `interface{}` và bóc tách giá trị thực nếu nó được bọc trong `map[string]interface{}` (ví dụ map chứa key `"long"`, `"string"`, hoặc `"bytes"`).
   - Áp dụng cho `added_snapshot_id` trong manifest list và các trường số liệu trong manifest file.

3. **Thuật toán Phân loại Delta Manifest:**
   - Trong `ReadManifestFile`, tách đầu ra thành 2 danh sách rõ rệt:
     - `AddedFiles []ManifestDataFile` (`status == 1`)
     - `DeletedFiles []string` (`status == 2`)
   - Bỏ qua toàn bộ `status == 0`.

4. **Trích xuất Khóa `2147483546` & Fail-Fast Position Deletes:**
   - Hỗ trợ duyệt qua cả định dạng map trực tiếp lẫn mảng danh sách bản ghi key-value để trích xuất đường dẫn file tham chiếu.
   - Khi đồng bộ Position Delete (`Content == 1`), nếu `ReferencedPath` không tìm thấy trong danh mục file active, trả về `fmt.Errorf` lập tức, tuyệt đối không dùng `MAX(data_file_id)`.

5. **Hiệu chỉnh Benchmark k6:**
   - Cập nhật `benchmark.js`:
     ```javascript
     check(commitRes, {
       'commit status is strictly 200': (r) => r.status === 200,
       'commit latency < 15ms': (r) => r.timings.duration < 15,
     });
     ```

## Testing Decisions

- **Single Test Seam:**
  - Kiểm thử toàn diện qua HTTP REST API Boundary (`POST /v1/namespaces/{ns}/tables/{tbl}`) kết hợp kiểm tra trạng thái dòng trong PostgreSQL và kiểm thử tải trọng k6.
- **Unit Tests:**
  - Kiểm thử hàm `unwrapAvroUnion` với các mẫu dữ liệu Avro phức hợp.
  - Kiểm thử hàm `ReadManifestFile` với các trạng thái entry `ADDED`, `EXISTING`, và `DELETED`.
- **Integration Tests:**
  - Chạy `test_e2e_pyiceberg_ducklake.py` đảm bảo nhiều lần append liên tiếp ghi nhận đúng số dòng (không nhân đôi dữ liệu).
  - Chạy `benchmark.js` với 100% commit phản hồi HTTP 200.

## Out of Scope

- Bọc transaction cho bộ máy nhúng DuckDB (thuộc phạm vi Spec P1).
- Phân quyền người dùng Read-Only cho cổng PostgreSQL (thuộc phạm vi Spec P1/P2).

## Further Notes

Đặc tả này là điều kiện tiên quyết mang tính sống còn để Bifrost Proxy có thể vận hành ổn định trong môi trường streaming thực tế.
