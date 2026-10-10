# 01: Core Server & Auth Bypass

**What to build:** Khởi tạo project Golang (Fiber hoặc Gin). Xây dựng một HTTP REST Server tối giản cung cấp endpoint `POST /v1/oauth/tokens`. Endpoint này không thực hiện auth thật mà chỉ trả về một Fake Token hợp lệ theo chuẩn Iceberg. Kết quả là Redpanda có thể ping và nhận token thành công.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Project Golang được khởi tạo thành công với các dependencies (Fiber/Gin).
- [ ] Endpoint `POST /v1/oauth/tokens` trả về mã 200 OK kèm payload JSON Fake Token theo chuẩn Iceberg.
- [ ] Có Unit Test / HTTP Mock Test xác nhận endpoint hoạt động với độ trễ tối thiểu (< 5ms).
