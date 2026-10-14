# p2-02: Zero Direct Access RBAC Role and Analytics Client Hardening

**What to build:** Enforce the Zero Direct Access architecture by establishing a dedicated PostgreSQL read-only role `ducklake_reader` restricted to SELECT operations on catalog tables, and configure query engines (DuckDB, Superset) and test harnesses to connect exclusively using this role with read-only flags.

**Blocked by:** p1-01: Transactional Delta Sync and Fail-Fast Startup, p2-01: DuckLake Catalog Invariants and Column Bounds Statistics

**Status:** ready-for-agent

- [ ] Create database migration / initialization script for role `ducklake_reader` with `LOGIN PASSWORD 'reader_password'`.
- [ ] Grant `USAGE` on schema `ducklake_catalog`, `SELECT` on all tables in `ducklake_catalog`, and `SELECT` on `catalog_tables`.
- [ ] Configure `ALTER DEFAULT PRIVILEGES IN SCHEMA ducklake_catalog GRANT SELECT ON TABLES TO ducklake_reader;` for future catalog tables.
- [ ] Update `test_e2e_pyiceberg_ducklake.py` to connect via `user=ducklake_reader password=reader_password` with `READ_ONLY true`.
- [ ] Update documentation (`README.md`, `docs/ARCHITECTURE.md`) reflecting connection strings and security architecture.
- [ ] Verify that `ducklake_reader` can read catalog data successfully and is denied permission on write/drop commands.
