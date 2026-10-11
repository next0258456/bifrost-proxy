# Đặc tả Tính năng: Incremental Sync từ Iceberg sang DuckLake (Bifrost Proxy)

*Labels: ready-for-agent, incremental-sync, ducklake-native-catalog*

## 1. Problem Statement (Bài toán)

- **Tại sao cần Proxy?** Nếu chỉ là bài toán di chuyển dữ liệu hàng loạt (Batch Migration) hoặc quét lại toàn bộ bảng (Full Re-scan), chúng ta chỉ cần chạy một Cron Job cuối tuần. Lý do duy nhất Bifrost Proxy tồn tại là để giải quyết **Streaming CDC thời gian thực**: chuyển đổi tức thì các micro-commit từ Redpanda thành dữ liệu truy vấn được ngay trên DuckLake với độ trễ tối thiểu và tài nguyên cố định.
- **Điểm yếu của giải pháp cũ:** Hàm C++ `iceberg_to_ducklake` được DuckDB thiết kế như một công cụ Export 1 lần. Khi bảng phình to, mỗi commit micro-batch đều phải quét lại toàn bộ lịch sử snapshot ($O(Total)$), ngốn CPU/RAM và chắc chắn sẽ sập hệ thống (OOM) khi chạy streaming.
- **Yêu cầu cốt lõi:** Phải chuyển sang cơ chế **Incremental Delta Sync ($O(\Delta) = O(1)$)**, đồng thời tuân thủ chính xác 100% cấu trúc metadata catalog của DuckLake để DuckDB/Superset có thể truy vấn trực tiếp và hiểu trọn vẹn cả **Delete v2 (Row-level deletes)** lẫn **Schema/Partition Evolution**.

---

## 2. Solution (Giải pháp Kiến trúc)

Bifrost Proxy đóng vai trò là một **"Event Router & Metadata Synchronizer" siêu nhẹ**:
1. **Phía Catalog REST (Giao diện với Redpanda):**
   - Hứng HTTP `POST /v1/namespaces/{ns}/tables/{table}` từ Redpanda Iceberg Sink.
   - Cập nhật con trỏ bảng bền vững vào cơ sở dữ liệu PostgreSQL (`polaris` DB).
   - Trả lời `200 OK` ngay lập tức để thỏa mãn SLA `< 15ms`.
2. **Phía Đồng bộ Incremental (Giao diện với DuckLake):**
   - Đẩy thông tin commit vào **Go Worker Pool (Channel Queue)**.
   - Worker chạy ngầm bóc tách **đúng phần delta** của snapshot vừa commit (chỉ các file mới thêm hoặc mới xóa).
   - Ghi thẳng các bản ghi delta này vào **hệ thống 14 bảng Metadata Catalog chuẩn của DuckLake** trên DuckDB/S3.

---

## 3. Cấu trúc Metadata Catalog Chuẩn của DuckLake

Proxy sẽ duy trì và cập nhật trực tiếp vào hệ thống bảng metadata chuẩn của DuckLake:

| Tên bảng DuckLake | Trách nhiệm | Thao tác Incremental của Proxy |
| :--- | :--- | :--- |
| **`ducklake_snapshot`** | Quản lý phiên bản snapshot | `INSERT` snapshot ID mới, timestamp, schema version. |
| **`ducklake_data_file`** | Danh sách file Parquet đang hoạt động | `INSERT` đúng các file Parquet vừa được thêm trong commit. |
| **`ducklake_delete_file`** | Quản lý Delete v2 (Position Deletes) | `INSERT` file delete Parquet và liên kết với `data_file_id` gốc qua field `2147483546`. |
| **`ducklake_column`** | Quản lý Schema Evolution | Khi thêm cột: `INSERT` với `start_snapshot`. Khi xóa cột: `UPDATE end_snapshot`. |
| **`ducklake_partition_info`** & **`_column`** | Quản lý Partition Evolution | Ghi nhận cấu hình phân vùng mới theo snapshot tương ứng. |
| **`ducklake_file_column_stats`** | Min/Max/Null statistics của từng file | Copy stats từ manifest delta để DuckDB tối ưu scan (pruning). |
| **`ducklake_snapshot_changes`** | Nhật ký thay đổi giữa các snapshot | Ghi nhận danh sách file delta thêm/xóa trong commit. |

---

## 4. Xử lý Chuyên sâu: Delete v2 và Evolution

### 4.1. Xử lý Delete v2 (Position Deletes)
- Khi Debezium bắt lệnh `UPDATE`/`DELETE` từ Postgres, Redpanda sinh ra file Parquet chứa vị trí dòng bị xóa (`content = 1`).
- Proxy bóc manifest delta, đọc trường `2147483546` (`file_path` của data file bị ảnh hưởng).
- Proxy ghim file delete vào đúng `ducklake_data_file.data_file_id` tương ứng trong bảng `ducklake_delete_file`.
- **Hiệu năng:** Độ phức tạp $O(1)$. DuckDB khi query DuckLake tự động loại bỏ các dòng bị xóa tại thời điểm đọc.

### 4.2. Xử lý Schema Evolution
- Khi có `ALTER TABLE` (thêm/sửa/xóa cột), Iceberg gửi `UpdateTableRequest` với danh sách schema changes.
- Proxy cập nhật bảng `ducklake_column`:
  - Cột mới: Thêm dòng mới với `start_snapshot = current_snapshot, end_snapshot = NULL`.
  - Cột bị xóa: Gán `end_snapshot = current_snapshot` cho dòng cột cũ.
- File Parquet cũ giữ nguyên, DuckDB tự động điền giá trị `NULL` hoặc default khi đọc cột mới trên dữ liệu cũ.

---

## 5. Implementation Decisions (Quy chuẩn Kỹ thuật)

1. **Worker Pool (Queue):**
   - Sử dụng Buffered Channel trong Go (`chan CommitTask`) với kích thước buffer cấu hình được (mặc định 1000).
   - Số lượng worker: Mặc định 1 worker chạy tuần tự để loại trừ xung đột ghi (lock contention) và bảo vệ RAM.
2. **Khống chế Tài nguyên (Memory Limit):**
   - Thiết lập `PRAGMA threads = 1;` cho DuckDB connection.
   - Giới hạn bộ nhớ: `PRAGMA memory_limit = '250MB';`.
   - Go Runtime: `GOMEMLIMIT=250MiB` và `GOGC=50`.
   - Tổng RAM container đảm bảo luôn nằm dưới trần co giãn 512MB (idle < 50MB).
3. **Singleton Catalog Connection:**
   - Khởi tạo S3 Secret và `ATTACH` DuckLake Catalog 1 lần duy nhất lúc khởi động Proxy.
   - Tái sử dụng connection cho toàn bộ các tác vụ incremental sync, không re-attach mỗi commit.

---

## 6. Testing Decisions (Kiểm thử Hộp Đen & Đo lường)

1. **Test Đo lường SLA API (< 15ms):**
   - Bắn commit payload với 50 concurrent users (K6 benchmark). Đảm bảo HTTP response luôn trả về `< 15ms`.
2. **Test Incremental Insert ($O(1)$):**
   - Chèn dòng dữ liệu vào Postgres -> Debezium đẩy sang Redpanda -> Redpanda commit sang Proxy.
   - Kiểm tra bảng `ducklake_data_file` trên MinIO: Chỉ có đúng 1 dòng file Parquet mới được thêm vào.
3. **Test Delete v2:**
   - Thực thi lệnh `DELETE` một dòng trên Postgres.
   - Kiểm tra bảng `ducklake_delete_file`: File position delete xuất hiện và liên kết đúng `data_file_id`.
   - Truy vấn qua Superset/DuckDB: Dòng bị xóa biến mất khỏi kết quả truy vấn.
4. **Test Schema Evolution:**
   - `ALTER TABLE` thêm cột mới trên Postgres.
   - Bắn dữ liệu mới có cột mới.
   - Truy vấn qua DuckLake: Cả dữ liệu cũ (cột mới mang giá trị NULL) và dữ liệu mới (có giá trị) đều được đọc đồng nhất.

---

## 7. Out of Scope

- Compaction định kỳ gom các file Parquet nhỏ (để dành cho tiến trình background định kỳ, không làm trong luồng streaming của Proxy).
- Phục vụ nhiều Catalog đa vùng địa lý phức tạp.
