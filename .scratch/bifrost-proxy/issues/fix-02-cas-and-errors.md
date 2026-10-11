# Fix-02: Strict CAS Optimistic Concurrency & Error Propagation

**What to build:** Sửa câu lệnh SQL trong `SaveTablePointer` để kiểm tra điều kiện CAS: `WHERE (metadata_location = $2 OR $2 = '')`. Nếu không khớp phiên bản, trả về lỗi `ErrCommitConflict`. Trong `api/catalog.go`, bắt buộc kiểm tra lỗi trả về từ `SaveTablePointer`: nếu conflict trả `409 Conflict`, nếu database lỗi trả `500 Internal Server Error`, chấm dứt việc nuốt lỗi.

**Blocked by:** Fix-01: Repository Cleanup & Domain Types

**Status:** ready-for-agent

- [ ] Sửa `SaveTablePointer` trong `storage/postgres.go` có điều kiện CAS và kiểm tra `RowsAffected()`.
- [ ] Định nghĩa lỗi `ErrCommitConflict = errors.New("commit conflict")`.
- [ ] Cập nhật `POST /v1/namespaces/:namespace/tables/:table` trong `api/catalog.go` để trả về HTTP 409 khi xung đột CAS và HTTP 500 khi DB lỗi.
- [ ] Viết unit test xác nhận việc từ chối ghi đè khi previous pointer không khớp.
