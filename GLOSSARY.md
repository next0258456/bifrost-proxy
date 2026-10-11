# Glossary

- **Bifrost Proxy**: Một Gateway cực nhẹ viết bằng Go, giả lập API của Apache Polaris (Iceberg REST Catalog). Nhận metadata từ hệ thống Ingestion nhưng bên trong lén lút đồng bộ thành định dạng DuckLake để truy vấn siêu tốc.
- **DuckLake**: Định dạng lưu trữ tối ưu hóa cho DuckDB, được Bifrost Proxy gen ra từ Iceberg metadata.
- **Iceberg Sink**: Thành phần (thường nằm trên Redpanda) đẩy dữ liệu CDC từ streaming vào Data Lake thông qua giao tiếp với REST Catalog (Bifrost Proxy).
- **CDC (Change Data Capture)**: Luồng dữ liệu thay đổi theo thời gian thực. Trong hệ thống này, Debezium đọc CDC từ Postgres (`fmcg_source_db`) và ném vào Redpanda.
- **Fake Token**: Chuỗi mã thông báo "giả" do Bifrost Proxy sinh ra tức thì (latency < 5ms) để lừa các hệ thống yêu cầu OAuth2, bỏ qua khâu xác thực rườm rà.
