# 02: ff-02-e2e-explicit-unpacking-and-full-verification

**What to build:**
Tái cấu trúc việc duyệt và đối chiếu các hàng kết quả trong kịch bản kiểm thử tích hợp E2E `test_e2e_pyiceberg_ducklake.py` bằng cách unpacking tuple/record tường minh cho 5 trường (`order_id`, `customer_name`, `amount`, `tax`, `created_at`), loại bỏ code smell Primitive Obsession/Positional Indexing. Kiểm thử end-to-end toàn diện bảo đảm luồng ghi PyIceberg -> Bifrost Proxy -> DuckLake Native PostgreSQL hoạt động trơn tru 100%.

**Blocked by:**
01: ff-01-centralized-id-allocator-and-failfast-db

**Status:**
completed

- [x] Cập nhật vòng lặp in và so sánh dữ liệu trong `test_e2e_pyiceberg_ducklake.py` để unpack tường minh các biến `(order_id, customer_name, amount, tax, created_at)`.
- [x] Chạy kiểm thử tự động `python test_e2e_pyiceberg_ducklake.py` kiểm tra kết quả giữa Iceberg Scan và DuckLake Native.
- [x] Xây dựng lại container Docker của Bifrost Proxy và chạy kiểm thử toàn bộ test suite.
