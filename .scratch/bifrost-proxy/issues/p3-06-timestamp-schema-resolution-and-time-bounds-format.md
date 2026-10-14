# p3-06: Timestamp-based Schema Resolution and Time Bounds Formatting

**What to build:** Khắc phục 2 điểm sai lệch đặc tả cốt lõi:
1. Triển khai phân giải schema lịch sử theo mốc thời gian snapshot (`snap.TimestampMs`) trong `resolveSchemaForSnapshot`.
2. Định dạng chuẩn hóa microsecond `%02d:%02d:%02d.%06d` bắt buộc cho kiểu `time` trong `DecodeIcebergBinaryBound`.
3. Bổ sung unit tests cho schema resolution theo timestamp và time formatting.

**Blocked by:** p3-05: Fail-Fast Error Handling and Clean Code

**Status:** ready-for-agent

- [ ] Cập nhật `resolveSchemaForSnapshot` trong `engine/incremental.go` để so khớp `snap.TimestampMs` khi snapshot thiếu `schema-id`.
- [ ] Cập nhật `DecodeIcebergBinaryBound` trong `engine/manifest.go` để định dạng thời gian kiểu `time` luôn đủ 6 chữ số microsecond (`%02d:%02d:%02d.%06d`).
- [ ] Thêm unit test kiểm tra giải mã kiểu `time` với phần dư microsecond bằng 0 và khác 0.
- [ ] Thêm unit test kiểm thử phân giải schema lịch sử theo `snap.TimestampMs`.
