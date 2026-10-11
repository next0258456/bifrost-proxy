# Master-01: Centralized Config & TableIdentifier Ubiquitous Refactor

**What to build:** Tạo package `config/config.go` quản lý tập trung toàn bộ biến môi trường S3/MinIO và PostgreSQL với các giá trị mặc định chuẩn xác (`minio:9000`, `postgres:5432/polaris`). Đưa struct `domain.TableIdentifier` vào thay thế toàn bộ tham số chuỗi thô `(namespace, table string)` ở mọi interface của `StorageService`, `CatalogStore`, và `QueueService`. Loại bỏ triệt để code smell Primitive Obsession và Config Duplication.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Tạo package `config` với struct `Config` và hàm `LoadConfig()` duy nhất.
- [ ] Refactor `main.go` và `engine/duckdb.go` sử dụng `config.LoadConfig()`.
- [ ] Cập nhật chữ ký hàm `storage.StorageService` nhận `domain.TableIdentifier`.
- [ ] Cập nhật chữ ký hàm `storage.CatalogStore` nhận `domain.TableIdentifier`.
- [ ] Cập nhật struct `engine.CommitTask` chứa `domain.TableIdentifier`.
