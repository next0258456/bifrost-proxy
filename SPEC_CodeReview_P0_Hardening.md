# Đặc tả Kỹ thuật: Chuẩn hóa Toàn diện & Khắc phục 6 Điểm Khuyết Code Review P0 (SPEC_CodeReview_P0_Hardening.md)

*Labels: ready-for-agent, code-review-hardening, p0-cleanup, zero-swallowed-errors, spec-alignment*

## Problem Statement

Mặc dù 4 điểm gãy P0 cốt lõi (Commit CAS lock, Queue split-brain, Avro union unwrap và Position delete bounds) đã được khắc phục và vượt qua các bài kiểm thử tải trọng k6 (strictly 200) cùng kịch bản tích hợp E2E hai lần append liên tiếp, quá trình nghiệm thu chéo độc lập (Two-Axis Code Review trên hệ quy chiếu Standards và Spec) đã phát hiện 6 điểm khuyết tật kỹ thuật và rủi ro tiềm ẩn:

1. **Rủi ro gắn sai Position Delete vào file dữ liệu cũ hoặc bảng khác:**
   Khi phân giải mã định danh `refDataFileID` cho file Position Delete, câu truy vấn chỉ lọc theo đường dẫn thô (`WHERE path = $1 LIMIT 1`). Nếu file dữ liệu này đã bị xoá ở snapshot trước (`end_snapshot IS NOT NULL`) hoặc thuộc về một bảng khác có cấu trúc thư mục tương tự, file delete sẽ bị gán sai mục tiêu, làm sai lệch kết quả truy vấn.
2. **Lọc trạng thái Manifest Entry chưa nghiêm ngặt:**
   Hàm đọc manifest entry đang sử dụng cơ chế loại trừ (`status != 0` và `status != 2`) rồi mặc định đưa vào danh sách file thêm mới (`added`). Điều này làm lọt các trạng thái bất thường khác ngoài chuẩn thay vì ép buộc chặt chẽ chỉ tiếp nhận `status == 1 (ADDED)`.
3. **Bỏ sót giải mã Avro Union trong cấu trúc Map của Bounds:**
   Khi trích xuất trường đường dẫn tham chiếu `2147483546` từ cấu trúc map, giá trị được ép kiểu trực tiếp thành mảng byte mà không qua hàm giải mã Avro union. Nếu Avro engine biểu diễn byte dưới dạng union đa kiểu `{"bytes": [...]}` trong map, việc trích xuất sẽ thất bại ngầm.
4. **Vi phạm tiêu chuẩn "Tuyệt đối không nuốt lỗi" (No Swallowed Errors):**
   Trong quá trình tải danh sách manifest từ hệ thống lưu trữ, khi gặp sự cố mạng hoặc lỗi giải mã trên một manifest file con, mã nguồn chỉ in log cảnh báo và bỏ qua bằng lệnh `continue`, hoặc chỉ ghi log khi lệnh đóng `end_snapshot` của file bị xóa gặp lỗi cơ sở dữ liệu. Điều này dẫn đến tình trạng metadata bị thiếu hụt ngầm mà hệ thống vẫn báo hoàn tất đồng bộ thành công.
5. **Thiếu vắng kiểm thử đơn vị cô lập (Unit Tests) cho tầng giải mã Manifest:**
   Toàn bộ logic bóc tách Avro union, lọc trạng thái manifest entry và phân giải trường bounds mới chỉ được kiểm thử gián tiếp qua kiểm thử tích hợp E2E, thiếu các bài unit test cô lập giả lập các khối nhị phân Avro đặc thù và các ca biên sai hỏng.
6. **Tồn đọng mã nguồn trùng lặp và biểu diễn biến số khó hiểu (Fowler Code Smells):**
   - Logic kiểm tra điều kiện lạc quan (`requirements`) bị nhân bản giữa hàm kiểm tra ngoài transaction và hàm commit nguyên khối trong kho lưu trữ catalog.
   - Số nhận dạng trường Iceberg `2147483546` bị gán cứng (magic number) rải rác trong mã nguồn thay vì được đặt hằng số định danh nghiệp vụ rõ ràng.
   - Các biến mang tên viết tắt khó đọc (`aFiles`, `dFiles`, `mdf`) làm giảm khả năng bảo trì mã nguồn.

## Solution

Triển khai gói chuẩn hóa toàn diện xử lý dứt điểm 6 điểm khuyết tật được phát hiện trong đợt Code Review:

1. **Siết chặt điều kiện liên kết Position Delete:**
   Bổ sung bắt buộc ràng buộc `table_id` và điều kiện `end_snapshot IS NULL` vào truy vấn tìm kiếm file dữ liệu tham chiếu, đảm bảo file delete chỉ được liên kết với file dữ liệu đang thực sự hoạt động của chính bảng đó.
2. **Kiểm tra nghiêm ngặt trạng thái Manifest Entry:**
   Chuyển đổi logic xử lý manifest entry sang kiểm tra xác thực danh tính rõ ràng: chỉ những entry có `status == 1` mới được ghi nhận là thêm mới (`added`), các entry có `status == 2` được thu thập vào danh sách đóng (`deleted`), và mọi entry `status == 0` hoặc trạng thái không hợp lệ bị bỏ qua an toàn hoặc báo lỗi.
3. **Đồng nhất cơ chế giải mã Avro Union cho mọi nhánh cấu trúc Bounds:**
   Áp dụng hàm giải mã union `unwrapAvroUnion` cho cả hai nhánh xử lý: dạng map thuần và dạng mảng các cặp key-value.
4. **Triệt tiêu hoàn toàn hành vi nuốt lỗi (Fail-Fast I/O & Database Execution):**
   Nếu bất kỳ manifest file nào trong manifest list không thể đọc hoặc giải mã, hoặc lệnh cập nhật đóng snapshot file xóa trong cơ sở dữ liệu thất bại, tiến trình đồng bộ lập tức dừng lại và trả về lỗi ngữ cảnh chi tiết, ngăn ngừa triệt để dữ liệu phân mảnh.
5. **Xây dựng bộ kiểm thử đơn vị độc lập cho Manifest Engine:**
   Bổ sung bộ unit test chuyên biệt kiểm thử đa dạng cấu trúc Avro union phức tạp, kiểm thử phân loại chính xác các trạng thái ADDED, EXISTING, DELETED, và kiểm thử việc trích xuất trường bounds đường dẫn tham chiếu.
6. **Tái cấu trúc mã nguồn loại bỏ Code Smells:**
   - Trích xuất hàm kiểm tra `requirements` dùng chung giữa các luồng xử lý của Catalog Store.
   - Khai báo hằng số nghiệp vụ định danh `IcebergReferencedDataFilePathFieldID = 2147483546`.
   - Chuẩn hóa tên biến theo phong cách định danh rõ nghĩa (`addedFiles`, `deletedFiles`, `manifestDataFile`).

## User Stories

1. As a Data Integrity Engineer, I want Position Delete files to only bind to active data files belonging to the exact same table, so that records are never mistakenly marked as deleted across snapshots or table boundaries.
2. As a Lakehouse Architect, I want manifest entry processing to explicitly accept only status ADDED (1) for insertion, so that unknown or malformed entry statuses never corrupt the catalog table state.
3. As an Avro Storage Specialist, I want bounds extraction to unwrap Avro union envelopes identically across map and array layouts, so that referenced data file paths are never lost regardless of Avro schema physical variations.
4. As a Site Reliability Engineer, I want manifest reading to fail immediately if any manifest file cannot be fetched or parsed, so that partial and incomplete snapshot synchronizations are strictly prevented.
5. As a Database Administrator, I want database updates marking deleted files to propagate execution errors immediately, so that failures in closing previous file lifetimes cannot happen silently.
6. As a Core Developer, I want optimistic concurrency validation logic to reside in a single shared helper within the catalog store, so that bug fixes and rule additions are never duplicated or desynchronized.
7. As a Code Reviewer, I want domain constants like Iceberg field IDs and explicit variable names to be used consistently, so that cryptic magic numbers and abbreviations do not obfuscate business intent.
8. As a Test Automation Engineer, I want unit tests that isolate manifest parsing and Avro union unwrapping from real S3 storage, so that edge cases and error handling paths can be validated in milliseconds without network dependencies.
9. As a Platform Operator, I want ingestion queue error logs to contain the exact root cause of delta sync failures, so that storage and database issues can be diagnosed instantly.

## Implementation Decisions

1. **Khử trùng lặp logic kiểm tra điều kiện lạc quan (Requirements Validation):**
   - Xây dựng một phương thức trợ năng nội bộ tiếp nhận trạng thái bản ghi bảng hiện tại (vị trí metadata, UUID, snapshot hiện tại, schema hiện tại) cùng danh sách `TableRequirement`.
   - Cả phương thức kiểm tra độc lập và phương thức commit nguyên khối trong transaction đều ủy quyền kiểm tra qua phương thức trợ năng này.

2. **Ràng buộc khóa ngoại logic cho Position Delete:**
   - Cập nhật câu lệnh truy vấn tìm kiếm `data_file_id` tham chiếu từ đường dẫn `ReferencedPath` bắt buộc phải kèm điều kiện kiểm tra bảng sở hữu và cờ trạng thái đang mở (`end_snapshot IS NULL`).

3. **Chuẩn hóa bộ lọc Manifest Entry và giải mã Bounds:**
   - Sử dụng khối điều khiển phân loại trạng thái rành mạch:
     - Trạng thái 1: Bóc tách thuộc tính và thêm vào danh sách file mới.
     - Trạng thái 2: Thu thập đường dẫn vào danh sách file cần đóng snapshot.
     - Trạng thái 0: Bỏ qua (không thay đổi trạng thái).
     - Trạng thái khác: Bỏ qua và ghi log thông tin nếu cần, không tự ý coi là file mới.
   - Trong hàm trích xuất đường dẫn file tham chiếu từ bounds, giá trị trích xuất từ map theo khóa định danh trường phải được chuyển qua hàm giải mã union trước khi ép kiểu chuỗi byte.

4. **Triệt tiêu cơ chế bỏ qua lỗi (No Swallowed Errors):**
   - Trong vòng lặp đọc manifest con: nếu việc tải byte từ kho lưu trữ hoặc giải mã Avro thất bại, hàm lập tức trả về lỗi kèm đường dẫn file vi phạm, chấm dứt toàn bộ tiến trình.
   - Trong luồng cập nhật cơ sở dữ liệu đóng file đã xóa: nếu câu lệnh cập nhật trả về lỗi, lập tức ngắt tiến trình và truyền lỗi lên tầng gọi.

5. **Định danh hằng số và quy chuẩn tên gọi:**
   - Định nghĩa hằng số công khai đại diện cho mã định danh trường Iceberg Position Delete: `2147483546`.
   - Đổi tên toàn bộ biến viết tắt trong tầng đọc manifest sang danh từ mô tả đầy đủ.

## Testing Decisions

1. **Tiêu chuẩn của một bài kiểm thử tốt:**
   - Kiểm thử hành vi thực tế của module, xác nhận đúng đầu ra ứng với từng cấu trúc đầu vào thay vì phụ thuộc vào chi tiết cài đặt nội bộ.
   - Chạy độc lập, không yêu cầu kết nối mạng hay dịch vụ lưu trữ ngoài.

2. **Các module được kiểm thử:**
   - Module xử lý Avro manifest: kiểm thử giải mã union với các biến thể (kiểu nguyên thủy, map union của goavro, mảng key-value union).
   - Module phân loại trạng thái manifest: kiểm thử bộ lọc với dữ liệu mẫu chứa cả ba trạng thái 0, 1, 2.
   - Module đồng bộ delta: kiểm thử câu lệnh cập nhật đóng file xóa và kiểm thử xử lý khi Position Delete không tìm thấy file dữ liệu active.

3. **Tiền lệ kiểm thử trong kho mã nguồn:**
   - Tham chiếu cấu trúc unit test hiện có trong các tệp kiểm thử của tầng engine và catalog store.

## Out of Scope

- Triển khai các tính năng mới thuộc nhóm P1/P2 (Transaction nguyên khối cho toàn bộ 7 bảng DuckLake Catalog, thống kê cột min/max, phân quyền người dùng read-only).
- Thay đổi cấu trúc cơ sở dữ liệu vật lý của PostgreSQL hoặc bảng DuckLake.

## Further Notes

- Sau khi hoàn thành đặc tả này, việc triển khai sẽ giải quyết dứt điểm toàn bộ 6 phát hiện từ phiên kiểm toán mã nguồn, đưa chất lượng module đạt mức tối đa và chuẩn bị nền tảng vững chắc để chuyển sang đặc tả P1 & P2.
