# 04: p0-04-k6-strictly-200-and-e2e-nonduplication-verification

**What to build:**
Hiệu chỉnh kịch bản kiểm thử tải trọng `benchmark.js` để kiểm tra nghiêm ngặt phản hồi HTTP 200 cho toàn bộ các commit (xóa bỏ cờ ảo ảnh 409). Chạy kiểm thử tải trọng bằng `k6` và kiểm thử tích hợp E2E nhiều lần append liên tiếp bằng PyIceberg (`test_e2e_pyiceberg_ducklake.py`), xác nhận tính đúng đắn: hệ thống đạt throughput cao thực tế, tỉ lệ commit thành công đạt chuẩn, và không có hiện tượng nhân bản dữ liệu trong DuckLake Catalog.

**Blocked by:**
01: p0-01-atomic-cas-and-queue-backpressure
02: p0-02-avro-union-unwrap-and-delta-manifest-filter
03: p0-03-position-delete-bounds-extractor-and-failfast

**Status:**
completed

- [x] Cập nhật `benchmark.js` chỉ chấp nhận `r.status === 200` cho commit checks.
- [x] Mở rộng kịch bản `test_e2e_pyiceberg_ducklake.py` thực hiện ghi liên tiếp 2 snapshot append và kiểm tra số dòng `COUNT(*)` trong DuckLake khớp chính xác với Iceberg (không bị nhân đôi).
- [x] Chạy load test `k6` với kịch bản sửa lỗi, xác nhận tỉ lệ commit status 200 đạt > 99%.
- [x] Toàn bộ test suite trong container Go và Docker compose pass 100%.
