# p1-02: Recursive Type Mapper for Iceberg Complex Types

**What to build:** Build a recursive type mapping parser `MapIcebergTypeToDuckLake` that converts primitive, nested, and complex Iceberg data types into DuckDB-compatible syntax, preventing DuckDB reader engine crashes when querying tables with collections, timezone-aware timestamps, or nested structures.

**Blocked by:** None (can start immediately)

**Status:** completed

- [x] `MapIcebergTypeToDuckLake` supports timezone-aware timestamps: `timestamptz`, `timestamp with time zone`, and `timestamp_ns` mapping to `timestamptz`.
- [x] Recursively unwraps `list<T>` and `array<T>` into DuckDB array notation `T[]` (e.g., `list<string>` -> `varchar[]`, `list<list<int>>` -> `int32[][]`).
- [x] Maps `map<K, V>` into DuckDB map notation `MAP(K, V)` with recursive conversion of key and value types.
- [x] Handles `struct<...>` and `json` types cleanly.
- [x] Provides a safe fallback to `varchar` or sanitized string for unrecognized types, preventing invalid SQL tokens from reaching `ducklake_column.column_type`.
- [x] Comprehensive unit tests cover all primitive types, nested lists, maps, timezone timestamps, and fallback cases.
