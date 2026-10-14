# p3-01: Iceberg Little-Endian Binary Bounds Decoder and Column Stats Enrichment

**What to build:** Implement a comprehensive binary decoder for Iceberg single-value serialization specs (v1 and v2) that decodes little-endian byte arrays for numeric, date, time, timestamp, boolean, and decimal lower/upper bounds into canonical string representations stored in `ducklake_file_column_stats` (`min_value`, `max_value`), enabling DuckDB to execute zone map file skipping / pruning on analytical queries.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Implement `DecodeIcebergBinaryBound(rawBytes []byte, icebergType string) (string, error)` supporting `int`, `long`, `float`, `double`, `boolean`, `date`, `time`, `timestamp`, `timestamptz`, `string`, `uuid`, and `decimal(P, S)`.
- [ ] Connect column type lookup from active schema fields so bounds are parsed using their actual column types rather than raw UTF-8 string casting.
- [ ] Ingest decoded `min_value` and `max_value` into `ducklake_file_column_stats`.
- [ ] Add unit tests in `engine/manifest_test.go` covering binary bounds fixtures for all supported types (int32, int64, float32, float64, date, timestamp, boolean, string).
- [ ] Verify that non-string bounds are no longer stored as NULL in `ducklake_file_column_stats`.
