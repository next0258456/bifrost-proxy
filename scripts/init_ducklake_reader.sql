-- Zero Direct Access RBAC Role: ducklake_reader
-- Grants read-only access to DuckLake metadata catalog tables in PostgreSQL

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ducklake_reader') THEN
        CREATE ROLE ducklake_reader WITH LOGIN PASSWORD 'reader_password';
    END IF;
END $$;

-- Grant read-only privileges on public schema (where ducklake catalog tables reside in polaris DB)
GRANT USAGE ON SCHEMA public TO ducklake_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ducklake_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ducklake_reader;

-- Grant read-only privileges on ducklake_catalog schema if created
CREATE SCHEMA IF NOT EXISTS ducklake_catalog;
GRANT USAGE ON SCHEMA ducklake_catalog TO ducklake_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ducklake_catalog TO ducklake_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ducklake_catalog GRANT SELECT ON TABLES TO ducklake_reader;
