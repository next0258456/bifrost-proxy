# p3-04: Type Mapper Safe Fallback, Code Smells Hardening and Full Verification

**What to build:** Complete remaining code review hardening points by enforcing safe fallback to `varchar` in `MapIcebergTypeToDuckLake`, refactoring `allocateNextID` to eliminate Feature Envy, unifying struct field delimiter parsing, eliminating swallowed error in `ParseMetadataFile`, and running full E2E & benchmark verification.

**Blocked by:** p3-01: Iceberg Little-Endian Binary Bounds Decoder and Column Stats Enrichment, p3-02: Catalog Counter Invariants and Entity ID Synchronization

**Status:** ready-for-agent

- [ ] Update `MapIcebergTypeToDuckLake` `default:` branch to return `"varchar"` with a warning log for unmapped types.
- [ ] Refactor `allocateNextID` into a clean package function taking `tx *sql.Tx` directly, removing unused `(e *SyncEngine)` receiver.
- [ ] Extract `parseStructField(part string)` to eliminate duplicated delimiter matching (`:` vs ` `) in struct type parsing.
- [ ] Ensure `ParseMetadataFile` wraps local read errors using `%w` and never swallows error values.
- [ ] Update `test_e2e_pyiceberg_ducklake.py` to assert decoded min/max bounds in `ducklake_file_column_stats` and run query with range filtering (`WHERE amount > ...`).
- [ ] Run full unit test suite, E2E test, and k6 benchmark verifying 100% checks and SLA compliance.
