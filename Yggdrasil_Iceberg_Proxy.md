# Bifrost Proxy (The Shape-Shifting Data Gateway)
*(Tên cũ: Yggdrasil Iceberg Proxy / Loki Catalog)*

**Tác giả:** Sếp
**Triết lý:** "Treo đầu dê Iceberg, bán thịt chó DuckLake"

---

## 1. Vấn đề & Giải pháp
- **Nỗi đau Ingestion:** Nếu dùng thuần 100% DuckLake, ta phải tự viết code tay (Kafka Consumers, Parquet Writers, Offset Managers) cực kỳ khổ sở và khó bảo trì.
- **Nỗi đau Truy vấn:** Nếu dùng thuần 100% Iceberg, tốc độ truy vấn sẽ bị cản trở bởi lớp Metadata JSON/Avro cồng kềnh.
- **Giải pháp (Bifrost Proxy):** Một con Proxy siêu nhẹ viết bằng **Golang**, đóng giả làm một Iceberg REST Catalog xịn để hứng Data tự động từ Redpanda, nhưng bên trong lén lút dùng Embedded DuckDB để chuyển hóa dữ liệu sang chuẩn DuckLake phục vụ truy vấn tốc độ cao. Cây cầu Bifrost nối liền 2 thế giới.

---

## 2. Kiến trúc Trạm Dịch Thuật Đa Ngữ (Rosetta Stone)

### Vòng ngoài: The Trickster (Lừa hệ sinh thái Ingestion)
Bifrost Proxy expose ra các API tuân thủ **chuẩn 100% Iceberg REST Protocol**:
- `POST /v1/oauth/tokens`: Trả về Fake Token (Bypass Auth phức tạp).
- `GET /v1/namespaces/{ns}/tables/{table}`: Load Iceberg Table.
- `POST /v1/namespaces/{ns}/tables/{table}`: Giao thức Commit (Compare-And-Swap) để nhận report từ hệ thống Ingestion.
👉 **Kết quả:** Thằng Redpanda Iceberg Sink hoàn toàn bị đánh lừa. Nó sung sướng tự động lo hết việc gom batch, ghi file Parquet, tự retry và vứt Metadata JSON vào Proxy.

### Vòng trong: The Core Engine (Embedded DuckDB)
Nhúng trực tiếp lõi **DuckDB (In-process)** vào bộ nhớ của Go API (qua thư viện `go-duckdb`).
Khi Redpanda gọi API `Commit`:
1. Proxy ném cục Metadata JSON cho Embedded DuckDB.
2. DuckDB dùng extension `iceberg` và `ducklake` để đọc, validate và đồng bộ (sync) trạng thái của bảng sang định dạng chuẩn của DuckLake.

### Đầu ra: Tự do Truy vấn (No Vendor Lock-in)
Proxy hỗ trợ trả về 2 luồng dữ liệu tùy khẩu vị của người dùng:
1. **Luồng Tiêu chuẩn (Standard Iceberg):** Trả về Iceberg JSON cho Spark, Superset.
2. **Luồng Cực Tốc (Native DuckLake):** DuckDB clients truy vấn thẳng vào định dạng DuckLake.

---

## 3. Bản thảo Triển khai (Golang Stack)
Lý do chọn Go thay vì Rust: Tập trung vào tốc độ phát triển API, parse JSON linh hoạt, và Goroutine xử lý concurrency I/O cực tốt mà không tốn công học Borrow Checker.

- **Web Framework:** Fiber hoặc Gin (Siêu nhẹ, routing HTTP cực nhanh).
- **Database Engine:** `go-duckdb` (CGO wrapper cho embedded in-memory catalog).
- **Storage:** MinIO / S3.
- *Phần code Go chi tiết sẽ được phát triển tiếp...*

---

## 4. Mục tiêu Hiệu năng (SLAs & KPIs)
Bifrost Proxy sinh ra để phế truất Apache Polaris, do đó nó bắt buộc phải thỏa mãn các giới hạn phần cứng và tốc độ sau đây:

### 1. Giới hạn Tài nguyên (Resource Quotas)
- **Hard Limit RAM:** `Max 300 MB` khi chạy trong Docker Container (Bao gồm cả bộ nhớ cấp phát cho Embedded DuckDB).

### 2. Yêu cầu Độ trễ tối thiểu (Phải đập chết Baseline của Polaris)
*Tham chiếu Benchmark thực tế của Polaris (Java/JVM) trên cùng hệ thống:*
> - [Auth] Lấy Token OAuth2: 123.79 ms
> - [Cold Start] Lần gọi API List Namespaces: 132.52 ms
> - Chậm nhất (Max): 26.40 ms
> - Nhanh nhất (Min): 10.30 ms
> - Trung bình (Avg): 13.48 ms

**Target của Bifrost Proxy (Golang):**
- **Auth (Fake Token):** Yêu cầu `< 5ms` (Bỏ xa 123ms của Polaris).
- **Cold Start:** Yêu cầu `< 15ms` (Bỏ xa 132ms).
- **Warm Average Ping:** Yêu cầu `< 2ms` (Vượt qua 13.48ms).
- **Max Tail Latency:** Yêu cầu `< 10ms` (Không bao giờ giật lag đến mức 26ms).
