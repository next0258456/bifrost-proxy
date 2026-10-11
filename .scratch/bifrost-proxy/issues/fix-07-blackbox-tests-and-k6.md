# Fix-07: Blackbox End-to-End Tests & Write-Heavy K6 Benchmark

**What to build:** Viết lại toàn bộ bài test tuân thủ Single Test Seam (chỉ assert qua HTTP REST response, không chọc biến private map). Cập nhật `benchmark.js` để bắn đồng thời 50 VUs vào endpoint `POST` commit nhằm đo đạc SLA < 15ms dưới tải ghi thực tế.

**Blocked by:** Fix-06: Delete v2 (Position Deletes) & Schema Evolution Mapping

**Status:** ready-for-agent

- [ ] Loại bỏ hoàn toàn việc assert map nội bộ trong `api/catalog_test.go`.
- [ ] Viết kịch bản K6 `benchmark.js` với 50 VUs gửi commit `POST /v1/namespaces/bronze/tables/fmcg_sales`.
- [ ] Chạy K6 benchmark xác nhận p(99) < 15ms dưới tải ghi đồng thời.
- [ ] Pass toàn bộ test suite.
