# Fix-01: Repository Cleanup & Domain Types

**What to build:** Dọn sạch toàn bộ file rác khỏi git repository (`diff.txt`, `full_diff.txt`, `ascii_diff.txt`, `diff_utf8.txt`, `.scratch/diff.txt`, `deepwiki.html`), chặn bằng `.gitignore`. Định nghĩa domain struct `TableIdentifier(Namespace, Table)`. Thống nhất hàm tiện ích `getEnv(key, fallback)` duy nhất, đồng bộ các giá trị mặc định cho S3 và Postgres.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [x] Chạy `git rm` xóa các file diff và html rác khỏi git index.
- [x] Cập nhật `.gitignore` để loại trừ vĩnh viễn các file `*.txt`, `*.html` rác trong root.
- [x] Định nghĩa `TableIdentifier` trong package `storage` hoặc `domain`.
- [x] Thống nhất hàm `getEnv` duy nhất, đồng bộ default S3 (`minio:9000` hoặc `localhost:9000`).
