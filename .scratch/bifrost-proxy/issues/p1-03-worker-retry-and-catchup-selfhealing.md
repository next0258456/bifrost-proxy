# p1-03: Ingestion Worker Transient Retry and History Catch-up Self-Healing

**What to build:** Equip `IngestionQueue` worker with exponential backoff retries for transient errors (network hiccups, temporary DB lock contention), and implement an automatic catch-up mechanism that detects missing snapshots in DuckLake and replays missing deltas from Iceberg's metadata history before applying new snapshots, ensuring zero desync drift.

**Blocked by:** p1-01: Transactional Delta Sync and Fail-Fast Startup

**Status:** ready-for-agent

- [ ] Worker retries failed `SyncDeltaIncremental` execution up to 3 times with exponential backoff (e.g., 100ms, 500ms, 1500ms) before logging an error.
- [ ] Before synchronizing a new snapshot for a table, the engine checks the latest recorded snapshot for that table in DuckLake.
- [ ] If a gap is detected (DuckLake is behind the previous snapshot of the current commit), the engine traverses `meta.Snapshots` history to replay and synchronize missing intermediate snapshots sequentially.
- [ ] Unit test verifies worker transient retry behavior on simulated temporary network errors.
- [ ] Unit test verifies automatic catch-up replay when a snapshot gap is present in the table's metadata history.
