# Sync-02: Ingestion Queue & Resource-Tuned DuckDB Worker

**What to build:** Dựng Go Buffered Channel (`chan CommitTask`) và Worker Pool (1 worker chạy tuần tự). Khởi tạo kết nối DuckDB singleton với các PRAGMA khống chế tài nguyên (`PRAGMA threads = 1;`, `PRAGMA memory_limit = '250MB';`, và `ATTACH 's3://lakehouse/bifrost_catalog' AS ducklake_catalog`). Sau khi Ticket 01 ghi Postgres thành công, event được đẩy vào channel để Worker lấy ra và in log/xử lý background an toàn, không rò rỉ RAM.

**Blocked by:** Sync-01: Synchronous Catalog Persistence (Postgres & CAS)

**Status:** ready-for-agent

- [x] Định nghĩa struct `CommitTask` chứa namespace, table_name, metadata_location, snapshot_id.
- [x] Tạo module Queue với Buffered Channel (buffer 1000) và hàm khởi chạy background worker tuần tự.
- [x] Cấu hình DuckDB Singleton với `PRAGMA threads = 1;` và `PRAGMA memory_limit = '250MB';`.
- [x] Khởi tạo kết nối và ATTACH catalog DuckLake 1 lần duy nhất lúc start app.
- [x] Viết test gửi 100 task đồng thời vào Queue, kiểm tra worker xử lý tuần tự không bị tranh chấp lock hoặc rò rỉ bộ nhớ.
