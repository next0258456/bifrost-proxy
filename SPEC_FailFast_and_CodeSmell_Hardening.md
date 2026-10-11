# Đặc tả Kỹ thuật: Khắc phục Triệt để Lỗi Tiềm ẩn, Fail-Fast và Loại bỏ Code Smells (SPEC_FailFast_and_CodeSmell_Hardening.md)

*Labels: ready-for-agent, code-quality, fail-fast, code-smells*

## Problem Statement

Mặc dù hệ thống đã kết nối thành công và ánh xạ dữ liệu trực tiếp giữa Iceberg và DuckLake PostgreSQL catalog backend, phiên `/code-review` độc lập vừa qua đã phát hiện 3 vấn đề kỹ thuật còn tồn đọng:

1. **Xử lý lỗi nửa vời (Incomplete Error Handling / Non-Fail-Fast):**
   - Khi truy vấn `ducklake_schema` và `ducklake_snapshot`, mã nguồn chỉ in log cảnh báo (`log.Printf("Warning: ...")`) mà không trả về lỗi. Vì câu truy vấn là `SELECT count(*)`, nó không bao giờ trả về `sql.ErrNoRows`. Do đó, bất kỳ lỗi hạ tầng nào (mất kết nối DB, timeout) đều khiến biến đếm giữ giá trị mặc định `0`, dẫn đến việc hệ thống cố gắng insert bản ghi trùng lặp và gây lỗi nghiêm trọng hơn.
2. **Sót nuốt lỗi ở luồng Position Deletes:**
   - Trong khối xử lý file xoá (`df.Content == 1`), hai lệnh tra cứu `refDataFileID` vẫn đang sử dụng `_ = e.db.QueryRow(...)`, vi phạm nguyên tắc "Tuyệt đối không nuốt lỗi" của dự án.
3. **Mã trùng lặp lặp đi lặp lại (Duplicated Code Smell):**
   - Mẫu truy vấn `SELECT COALESCE(MAX(<id_col>), 0) + 1 FROM <table>` lặp lại 4 lần trên các bảng `ducklake_schema`, `ducklake_table`, `ducklake_data_file`, và `ducklake_delete_file`. Mẫu mã này cần được chuẩn hóa thành một helper method nội bộ an toàn.

## Solution

Thực hiện đợt tái cấu trúc và làm cứng mã nguồn (Reliability & Clean Code Refactor):

1. **Thực thi nguyên tắc Fail-Fast cho toàn bộ truy vấn cơ sở dữ liệu:**
   - Trả về lỗi có bọc ngữ cảnh ngay lập tức nếu các câu truy vấn `ducklake_schema`, `ducklake_snapshot`, hoặc tra cứu `refDataFileID` thất bại.
   - Với các truy vấn kiểm tra sự tồn tại hoặc tra cứu ID fallback, xử lý phân biệt rõ ràng giữa trường hợp không có dữ liệu (`sql.ErrNoRows`) và lỗi hệ thống thật sự.
2. **Loại bỏ trùng lặp bằng ID Allocator Helper:**
   - Tạo một hàm tiện ích nội bộ nhận tên bảng và tên cột ID để cấp phát ID kế tiếp một cách nhất quán, tập trung logic bắt lỗi và bọc thông điệp lỗi.
3. **Chuẩn hóa đối chiếu E2E:**
   - Đảm bảo script kiểm thử E2E sử dụng unpacking tường minh cho các hàng dữ liệu, tăng tính tường minh và khả năng bảo trì.

## User Stories

1. As a Backend Engineer, I want the synchronization engine to fail fast with descriptive errors whenever database queries fail, so that data inconsistencies and primary key collisions are prevented at the earliest point.
2. As a Data Reliability Engineer, I want the deletion file resolution logic to strictly validate and handle query errors when locating referenced data files, so that position delete metadata is never corrupted.
3. As a Software Maintainer, I want next-ID generation across DuckLake catalog tables to be consolidated into a single helper method, so that code duplication is eliminated and error handling is consistently applied.
4. As a QA Engineer, I want the E2E verification test suite to use clear tuple unpacking for all column assertions, so that field mappings are robust against positional errors.

## Implementation Decisions

1. **Triển khai Fail-Fast trên toàn bộ luồng Ingestion:**
   - Trong luồng đồng bộ metadata của engine, mọi lời gọi `db.QueryRow` và `db.Exec` đều phải bắt lỗi `err != nil`.
   - Đối với truy vấn `ducklake_schema` và `ducklake_snapshot`, nếu câu truy vấn `SELECT count(*)` gặp lỗi, lập tức dừng và trả về `fmt.Errorf`.
   - Đối với `refDataFileID` trong Position Delete, kiểm tra lỗi và chỉ chấp nhận fallback giá trị mặc định khi lỗi trả về là `sql.ErrNoRows`. Các lỗi khác phải dừng và trả về `fmt.Errorf`.

2. **Tập trung hóa ID Allocator:**
   - Xây dựng một phương thức helper nội bộ trong Sync Engine (ví dụ `allocateNextID(table, column) (int64, error)`).
   - Áp dụng helper này cho `schema_id`, `table_id`, `data_file_id`, và `delete_file_id`.

3. **Giữ nguyên tính năng sinh UUID v4 cho Schema và Table:**
   - Tiếp tục duy trì UUID v4 chuẩn (`uuid.New().String()`) cho cả `ducklake_schema` và `ducklake_table` khi tạo mới.

## Testing Decisions

- **Single Test Seam:**
  - Tiếp tục duy trì seam kiểm thử cấp cao: HTTP boundary `POST /v1/namespaces/{ns}/tables/{table}` kết hợp xác thực SQL qua DuckDB client `ATTACH 'ducklake:postgres:...'`.
- **Unit Tests:**
  - Chạy toàn bộ unit test hiện có trong package `engine` và `storage` để đảm bảo không hồi quy.
- **E2E Integration Test:**
  - Chạy `python test_e2e_pyiceberg_ducklake.py` xác thực quy trình end-to-end từ PyIceberg đến DuckLake Native hoàn tất 100%.

## Out of Scope

- Thay đổi kiến trúc sang sử dụng PostgreSQL sequence chuyên biệt (do DuckLake schema hiện tại dùng ID độc lập và tự kiểm soát qua snapshot).

## Further Notes

Đặc tả này hoàn thiện triệt để mọi khuyến nghị và vi phạm từ `/code-review`, đưa Bifrost Proxy đạt chuẩn production code tuyệt đối sạch sẽ và đáng tin cậy.
