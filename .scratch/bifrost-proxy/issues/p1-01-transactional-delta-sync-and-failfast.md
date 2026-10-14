# p1-01: Transactional Delta Sync and Fail-Fast Startup

**What to build:** Ensure atomic execution of all DuckLake metadata updates within `SyncDeltaIncremental` using a database transaction (`BeginTx` / `Commit` with `defer Rollback`), eliminating partial writes when step failures occur. Enforce strict error propagation on active column query in Step 6 to prevent duplicate column inserts on DB issues. Enforce hard crash (`log.Fatalf`) on DuckDB sync engine initialization failure in `main.go` to prevent silent task dropping.

**Blocked by:** None (can start immediately)

**Status:** completed

- [x] `SyncDeltaIncremental` opens an atomic database transaction (`e.db.BeginTx(ctx, nil)`) and executes all DuckLake table updates (schema, table, snapshot, data files, delete files, columns, snapshot changes) through `*sql.Tx`.
- [x] A deferred rollback (`defer tx.Rollback()`) ensures that any failure at any step cleanly rolls back all catalog modifications for that snapshot.
- [x] `allocateNextID` helper is updated to accept an `execerQueryer` or `*sql.Tx` so ID allocations run within the transaction context.
- [x] Step 6 active column query (`ducklake_column`) checks and returns error (`fmt.Errorf`) instead of swallowing `err == nil`.
- [x] `main.go` calls `log.Fatalf` if `engine.NewSyncEngine` fails to initialize DuckDB, preventing the proxy from starting in a degraded state that silently drops tasks.
- [x] Unit test verifies transactional rollback on simulated failure without leaving orphaned records in DuckLake catalog tables.
