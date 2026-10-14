# p2-01: DuckLake Catalog Invariants and Column Bounds Statistics

**What to build:** Align DuckLake catalog mutations with reference specification invariants: dynamically increment counters in `ducklake_snapshot` (`schema_version`, `next_catalog_id`, `next_file_id`), tokenize `changes_made` string with standard comma-separated DuckLake tokens, and populate `ducklake_file_column_stats` with min/max bounds and null counts to activate DuckDB parquet pruning.

**Blocked by:** p1-01: Transactional Delta Sync and Fail-Fast Startup

**Status:** ready-for-agent

- [ ] Eliminate hardcoded `(1, 100, 1000)` in `ducklake_snapshot`: query previous snapshot counters via `SELECT COALESCE(MAX(schema_version), 1), COALESCE(MAX(next_catalog_id), 1), COALESCE(MAX(next_file_id), 1) FROM ducklake_catalog.ducklake_snapshot;` and advance them monotonically based on new entities created in the transaction.
- [ ] Format `changes_made` in `ducklake_snapshot_changes` as a standard token list: `created_table:"<table_uuid>"`, `inserted_into_table:<table_id>`, `deleted_from_table:<table_id>`, joined by comma.
- [ ] Extract column value bounds and null counts from manifest entries and ingest into `ducklake_file_column_stats` (or populate basic column stats per file).
- [ ] Unit tests verify monotonic counter progression and standardized `changes_made` token formats across consecutive snapshots.
