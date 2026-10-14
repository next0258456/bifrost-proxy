# p3-07: E2E Bounds Assertions and File Pruning Verification

**What to build:** Khắc phục yêu cầu E2E assertions và kiểm chứng file pruning:
1. Thêm assertions chính xác cho các giá trị min/max của từng cột trong `test_e2e_pyiceberg_ducklake.py`.
2. Bổ sung truy vấn lọc `WHERE amount > 500.0` kiểm chứng khả năng bỏ qua Parquet file (file skipping / zone map pruning).
3. Chạy full bộ kiểm thử (Go unit tests, E2E PyIceberg, k6 benchmark) xác nhận 100% checks.

**Blocked by:** p3-06: Timestamp-based Schema Resolution and Time Bounds Formatting

**Status:** completed

- [x] Cập nhật `test_e2e_pyiceberg_ducklake.py` assert cụ thể giá trị `min_value` và `max_value` của cột `order_id` (101, 105), `customer_name` ('Bifrost', 'DuckLake'), `amount` (12, 999.99).
- [x] Bổ sung truy vấn `SELECT ... WHERE amount > 500.0` và khẳng định chỉ trả về đúng 2 bản ghi (> $500).
- [x] Chạy Go unit tests, E2E test và k6 benchmark tải 50 VUs xác nhận 100% checks và SLA.
