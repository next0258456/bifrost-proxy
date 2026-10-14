# p3-02: Catalog Counter Invariants and Entity ID Synchronization

**What to build:** Synchronize monotonic snapshot counter invariants (`next_catalog_id`, `next_file_id`, `schema_version`) in `ducklake_snapshot` with real-time entity identifier allocation for schemas, tables, data files, and delete files, ensuring DuckLake catalog invariants never drift across consecutive snapshots.

**Blocked by:** p3-01: Iceberg Little-Endian Binary Bounds Decoder and Column Stats Enrichment

**Status:** ready-for-agent

- [ ] Define struct `SnapshotCounters` encapsulating `SchemaVersion`, `NextCatalogID`, and `NextFileID`.
- [ ] Query base counters from preceding `ducklake_snapshot` and assign sequential IDs for new catalog entities (`schema_id`, `table_id`, `data_file_id`, `delete_file_id`) within the delta sync transaction.
- [ ] Update `ducklake_snapshot` insertion to record advanced monotonic counters reflecting the true next available identifiers.
- [ ] Add unit tests verifying counter invariants over 3 consecutive snapshot commits containing multiple data and delete files.
- [ ] Confirm no ID drift exists between `ducklake_snapshot` counters and entity table MAX identifiers.
