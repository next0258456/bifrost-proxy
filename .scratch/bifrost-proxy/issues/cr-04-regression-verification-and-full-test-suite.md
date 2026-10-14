# cr-04: cr-04-regression-verification-and-full-test-suite

**What to build:**
Thực hiện chu trình kiểm thử hồi quy toàn diện trên môi trường Docker thật:
1. Rebuild container `bifrost-proxy` với mã nguồn đã hoàn thiện và khởi động lại dịch vụ.
2. Chạy bài kiểm thử tải trọng `benchmark.js` bằng k6, xác nhận tỷ lệ commit strictly 200 đạt 100%, độ trễ SLA p(99) < 15ms.
3. Chạy bài kiểm thử tích hợp thực tế `test_e2e_pyiceberg_ducklake.py` (2 lần append liên tiếp qua PyIceberg và đối chiếu với DuckLake Native Catalog), xác nhận 5 dòng khớp hoàn hảo và không bị nhân đôi.
4. Chạy toàn bộ test suite của Go để đảm bảo không có bất kỳ regression nào.

**Blocked by:**
cr-01: cr-01-position-delete-active-file-binding
cr-02: cr-02-manifest-strict-filter-and-union-unwrap
cr-03: cr-03-requirements-deduplication-and-manifest-unit-tests

**Status:**
completed

- [x] Rebuild và restart container `bifrost-proxy` bằng `docker compose`.
- [x] Chạy load test k6 xác nhận 100% commit strictly 200 checks và SLA < 15ms.
- [x] Chạy kiểm thử E2E PyIceberg 2-batch append xác nhận 5/5 dòng khớp giữa Iceberg và Pure DuckLake.
- [x] Xác nhận toàn bộ bài kiểm thử hoàn thành mỹ mãn.
