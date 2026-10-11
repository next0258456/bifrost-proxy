# Master-02: Typed Iceberg Commit Parser & HTTP API Hardening

**What to build:** Tạo typed parser `domain.UpdateTableRequest` trong package `domain` để tự động bóc tách `requirements` và `updates`, loại bỏ hoàn toàn việc bóc tách map thủ công (Feature Envy) trong HTTP handler. Bổ sung hàm tiện ích `writeError` chuẩn Iceberg để khử mã lỗi trùng lặp (Duplicated Code 404/500). Bổ sung bắt buộc header `Retry-After: 1` trên phản hồi HTTP 429.

**Blocked by:** Master-01: Centralized Config & TableIdentifier Ubiquitous Refactor

**Status:** ready-for-agent

- [ ] Tạo file `domain/iceberg.go` định nghĩa các struct `UpdateTableRequest`, `TableRequirement`, `TableUpdate` và hàm `ParseUpdateTableRequest(body []byte)`.
- [ ] Thêm hàm helper `writeError(c *fiber.Ctx, status int, errorType, message string)` trong `api/catalog.go`.
- [ ] Tích hợp parser vào `POST /v1/namespaces/:namespace/tables/:table`.
- [ ] Set header `c.Set("Retry-After", "1")` khi trả về mã lỗi HTTP 429.
