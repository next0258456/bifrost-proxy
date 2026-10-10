# 04: Production Readiness (Docker & SLA Tuning)

**What to build:** Đóng gói Proxy bằng Docker để chạy trực tiếp trên môi trường deploy có sẵn của Sếp. Thực hiện load test, ép tải và giới hạn bộ nhớ để hệ thống thỏa mãn toàn bộ SLA khắc nghiệt nhằm vượt mặt Apache Polaris.

**Blocked by:** 03: Ingestion Sync Pipeline (Commit API & DuckDB Core)

**Status:** ready-for-agent

- [ ] Viết Dockerfile đa bước (multi-stage) tối ưu dung lượng image.
- [ ] Thiết lập hard-limit `300MB RAM` cho container, đồng thời tune GOGC và DuckDB `memory_limit` để tránh OOM.
- [ ] Triển khai thành công lên môi trường Docker có sẵn.
- [ ] Viết/chạy Benchmark script (hey, k6) để verify hệ thống đáp ứng: Cold start < 15ms, Ping < 2ms, Tail Latency < 10ms.
