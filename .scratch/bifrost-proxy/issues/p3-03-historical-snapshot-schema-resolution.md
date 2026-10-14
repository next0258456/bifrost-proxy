# p3-03: Historical Snapshot Schema Resolution in Self-Healing Catch-up

**What to build:** Ensure that historical snapshots replayed during self-healing catch-up re-sync resolve and apply their originating schema definitions (via `schema-id` or historical timeline) rather than blindly inheriting the latest `CurrentSchemaID`, preserving exact historical column evolution in `ducklake_column`.

**Blocked by:** p3-02: Catalog Counter Invariants and Entity ID Synchronization

**Status:** ready-for-agent

- [ ] Implement `resolveSchemaForSnapshot(meta *IcebergMetadata, snap *SnapshotEntry) *SchemaEntry` to match a snapshot to its true originating schema.
- [ ] Update `SyncDeltaIncremental` and `syncSnapshotInternal` to use the resolved historical schema for column evolution tracking, column stats fallback, and column ID mapping.
- [ ] Add unit test with multi-schema progression (Schema 0 with 2 columns, Schema 1 with 4 columns) verifying that catch-up re-sync creates historical column entries matching Schema 0 at Snapshot 1 and Schema 1 at Snapshot 2.
- [ ] Confirm no future columns leak into historical snapshot DuckLake column records.
