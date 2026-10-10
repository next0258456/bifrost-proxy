# 02: Table Metadata Retrieval (Catalog Read & S3)

**What to build:** Kết nối Proxy với hệ thống Object Storage (S3/MinIO). Triển khai endpoint `GET /v1/namespaces/{ns}/tables/{table}` để khi Redpanda hoặc Spark gửi yêu cầu đọc thông tin bảng, Proxy sẽ lấy file metadata JSON của Iceberg từ S3 và trả về cho client.

**Blocked by:** 01: Core Server & Auth Bypass

**Status:** ready-for-agent

- [ ] Tích hợp MinIO SDK / AWS SDK vào project Golang.
- [ ] Endpoint `GET /v1/namespaces/{ns}/tables/{table}` kết nối S3 lấy và trả về nội dung metadata.
- [ ] Trả về mã lỗi 404 NoSuchTable hợp lý nếu file trên S3 chưa tồn tại.
- [ ] Viết Integration test xác nhận việc móc nối vào Object Storage thành công.
