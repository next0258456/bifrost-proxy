# Master-06: Strict Blackbox Integration Tests & SLA Verification

**What to build:** Chuẩn hóa toàn bộ bộ test tuân thủ Single Test Seam: test CAS với chuỗi pointer lệch thật, test 429 kèm header `Retry-After: 1` bằng cách bơm tải bão hòa buffer channel thật qua REST API boundary. Chạy K6 benchmark xác nhận p(99) < 15ms dưới tải ghi 50 VUs.

**Blocked by:** Master-05: Real Delete v2 Mapping & Dynamic Schema Evolution

**Status:** ready-for-agent

- [ ] Cập nhật toàn bộ unit và integration test trong `api/catalog_test.go` chỉ kiểm tra qua HTTP boundary.
- [ ] Test trường hợp CAS conflict với payload chứa `previous-metadata-location` sai lệch hoặc requirement sai.
- [ ] Test trường hợp queue bão hòa: gửi request đồng thời vượt ngưỡng buffer và kiểm tra status 429 cùng header `Retry-After: 1`.
- [ ] Chạy K6 benchmark 50 VUs xác nhận p(99) < 15ms.
