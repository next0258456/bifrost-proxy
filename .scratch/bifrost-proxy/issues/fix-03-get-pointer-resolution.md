# Fix-03: Real Pointer Resolution on Table GET API

**What to build:** Endpoint `GET /v1/namespaces/{ns}/tables/{table}` bắt buộc sử dụng chuỗi `metadata_location` đọc từ Postgres để tải đúng file metadata JSON từ S3 MinIO. Nếu bảng không tồn tại trong Postgres, trả về ngay `404 NoSuchTableException`.

**Blocked by:** Fix-02: Strict CAS Optimistic Concurrency & Error Propagation

**Status:** ready-for-agent

- [ ] Lấy chính xác `metadataLocation` từ `catalogStore.GetTablePointer(ctx, ns, table)`.
- [ ] Thêm phương thức đọc file metadata theo đường dẫn cụ thể trong `StorageService` (`GetMetadataByLocation(ctx, location)`).
- [ ] Endpoint `GET` trong `api/catalog.go` đọc đúng file từ đường dẫn này thay vì đường dẫn hardcode.
- [ ] Viết unit test xác nhận `GET` trả về đúng nội dung của file được trỏ bởi Postgres.
