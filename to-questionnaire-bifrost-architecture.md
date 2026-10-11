# Bảng Quyết Định Kiến Trúc Bifrost Proxy (Architecture Questionnaire)

**Purpose:** Chốt dứt điểm các quyết định kiến trúc và tích hợp kỹ thuật cốt lõi để thông luồng Ingestion (Redpanda -> Proxy -> DuckLake) và triển khai trên môi trường Docker.

**From:** AI Pair Programmer, **To:** Sếp (Lead Architect / Owner), **How your answers will be used:** Dùng để hiện thực hóa trực tiếp vào mã nguồn `bifrost-proxy`, cấu hình `docker-compose.yml`, và thiết lập luồng kiểm thử End-to-End.

## Context

Bifrost Proxy đã hoàn thành phần lõi HTTP REST API, bypass Auth siêu tốc (< 5ms), và đóng gói Docker giới hạn 300MB RAM. Tuy nhiên, luồng Ingestion thực tế từ Redpanda sang DuckLake đang cần các quyết định then chốt về: cách Redpanda đẩy dữ liệu, nơi lưu con trỏ Catalog bền vững, và chiến lược "cán phẳng" Avro sang DuckLake.

## How to answer

Sếp có thể trả lời trực tiếp vào từng mục bên dưới hoặc gõ trả lời nhanh trong khung chat (ví dụ: `1A, 2B, 3A...`).

---

## 1. Cấu hình Upstream (Redpanda & Debezium)

### 1.1. Cơ chế Iceberg Sink trên Redpanda sẽ được kích hoạt như thế nào?

> [x] **A (Đã chọn):** Sử dụng tính năng Native Iceberg Tiering / Sink có sẵn của Redpanda, trỏ REST Catalog URL về `http://bifrost-proxy:8080`.

### 1.2. Topic và Tên bảng mục tiêu là gì?

> **Namespace:** `bronze`  
> **Table:** `fmcg_sales`

---

## 2. Lưu trữ Con trỏ Catalog (Persistence Layer)

### 2.1. Con trỏ trạng thái bảng (Catalog Pointer) nên được lưu ở đâu?

> [x] **A (Đã chọn):** Lưu vào Postgres có sẵn (dùng lại database `polaris` trong container `postgres:15` qua thư viện `pgx`).

---

## 3. Chiến lược "Cán phẳng" sang DuckLake (Sync Engine)

### 3.1. Phương án kỹ thuật nào để chuyển hóa 3 tầng Iceberg sang DuckLake?

> **Quyết định của Sếp:** **Chia nhỏ vấn đề** thành từng pha để kiểm soát rủi ro, không làm gộp:
> - **Pha 1:** Hoàn thiện Catalog Commit & lưu con trỏ vào Postgres (chưa cần đụng DuckLake).
> - **Pha 2:** Dựng Go Worker Pool (Channel Queue) để nhận `metadata-location`.
> - **Pha 3:** Chạy thử nghiệm cán phẳng bằng DuckDB C++ có khống chế RAM, benchmark đo đạc thực tế.
> - **Pha 4:** Nếu C++ bất ổn mới bóc nhỏ thành các bước Go Avro parser độc lập.

---

## 4. Ràng buộc Tài nguyên & SLA (Hard Limits)

### 4.1. Giới hạn RAM 300MB có được phép co giãn linh hoạt khi Sync không?

> [x] **B (Đã chọn):** **Flexible limit** - Cho phép peak lên tối đa 512MB RAM khi có đợt commit dồn dập, trạng thái bình thường giữ < 50MB.

---

## 5. Đầu ra Truy vấn (Superset & DuckLake)

### 5.1. Superset sẽ truy vấn dữ liệu DuckLake đã cán phẳng bằng cách nào?

> **Quyết định của Sếp:** **Hỗ trợ song song (Dual Mode):**
> - Hỗ trợ gọi chuẩn qua **Iceberg REST Catalog API** của Proxy.
> - Hỗ trợ gọi tốc độ cao trực tiếp qua **DuckLake format**.

---

## Anything else?

Sếp có yêu cầu đặc biệt nào về log, metrics (Prometheus), hoặc cách xử lý khi Redpanda commit bị lỗi không?

> *Ý kiến bổ sung:*
