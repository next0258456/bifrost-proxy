"""
E2E Integration Test:
1. Ghi dữ liệu Iceberg qua PyIceberg (Catalog SqlCatalog trỏ đến MinIO/S3).
2. Gửi commit POST metadata tới Bifrost Proxy (xác nhận giao thức Iceberg REST & CAS).
3. Đọc dữ liệu Iceberg qua DuckDB Iceberg engine và Native DuckLake catalog.
"""

import os
import tempfile
import pathlib
import requests
import pyarrow as pa
import duckdb
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType


# Cấu hình môi trường S3 / MinIO & Bifrost Proxy
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
BIFROST_PROXY_URL = os.getenv("BIFROST_PROXY_URL", "http://localhost:8182")


def test_e2e_write_pyiceberg_read_ducklake():
    print("======================================================================")
    print("START E2E TEST: WRITE WITH PYICEBERG -> BIFROST PROXY -> READ DUCKLAKE")
    print("======================================================================\n")

    # -------------------------------------------------------------------------
    # STEP 1: Initialize PyIceberg Catalog pointing to MinIO S3 Warehouse
    # -------------------------------------------------------------------------
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "pyiceberg_catalog.db").replace("\\", "/")

    print(f"[1/4] Initializing PyIceberg Catalog (Warehouse: s3://{S3_BUCKET}/e2e_test)...")
    iceberg_catalog = SqlCatalog(
        "bifrost_e2e",
        uri=f"sqlite:///{db_path}",
        warehouse=f"s3://{S3_BUCKET}/e2e_test",
        **{
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
        },
    )

    namespace = "bronze"
    table_name = "fmcg_orders"
    full_table_name = f"{namespace}.{table_name}"

    try:
        iceberg_catalog.create_namespace(namespace)
    except Exception:
        pass

    # Schema definition
    schema = Schema(
        NestedField(field_id=1, name="order_id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="customer_name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="amount", field_type=DoubleType(), required=False),
    )

    try:
        iceberg_catalog.drop_table(full_table_name)
    except Exception:
        pass

    table = iceberg_catalog.create_table(full_table_name, schema=schema)
    print(f"  -> Created Iceberg table successfully: {full_table_name}")
    print(f"  -> Initial Metadata Location: {table.metadata_location}")

    # -------------------------------------------------------------------------
    # STEP 2: Write batch records via PyIceberg (generates real Parquet on S3)
    # -------------------------------------------------------------------------
    print("\n[2/4] Appending 3 order records via PyIceberg (Parquet data files)...")
    records = pa.Table.from_arrays(
        [
            pa.array([101, 102, 103], type=pa.int64()),
            pa.array(["CongAnh", "Bifrost", "DuckLake"], type=pa.string()),
            pa.array([250.50, 999.99, 12.00], type=pa.float64()),
        ],
        schema=schema.as_arrow(),
    )
    table.append(records)
    table.refresh()

    current_snap = table.current_snapshot()
    metadata_location = table.metadata_location
    snapshot_id = current_snap.snapshot_id

    print(f"  -> Append complete!")
    print(f"  -> Snapshot ID: {snapshot_id}")
    print(f"  -> New Metadata Location: {metadata_location}")

    # -------------------------------------------------------------------------
    # STEP 3: Send HTTP Commit Request to Bifrost Proxy (Iceberg REST API)
    # -------------------------------------------------------------------------
    print(f"\n[3/4] Sending Commit POST to Bifrost Proxy: {BIFROST_PROXY_URL}...")
    commit_payload = {
        "requirements": [],
        "updates": [
            {"action": "set-current-snapshot", "snapshot-id": snapshot_id}
        ],
        "metadata-location": metadata_location,
    }

    commit_url = f"{BIFROST_PROXY_URL}/v1/namespaces/{namespace}/tables/{table_name}"
    resp = requests.post(commit_url, json=commit_payload)

    print(f"  -> HTTP Response Code: {resp.status_code}")
    print(f"  -> Response Body: {resp.text}")
    assert resp.status_code == 200, f"Bifrost Proxy commit failed: {resp.text}"
    print("  -> Commit through Bifrost Proxy verified successfully!")

    # -------------------------------------------------------------------------
    # STEP 4: Query data via DuckDB & Native DuckLake
    # -------------------------------------------------------------------------
    print("\n[4/4] Initializing DuckDB with DuckLake extension...")
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("INSTALL httpfs; LOAD httpfs;")
    duckdb_conn.execute("INSTALL iceberg; LOAD iceberg;")
    duckdb_conn.execute("INSTALL ducklake; LOAD ducklake;")

    # S3 Secret pointing to MinIO
    duckdb_conn.execute(f"""
        CREATE SECRET s3_e2e (
            TYPE S3,
            KEY_ID '{S3_ACCESS_KEY}',
            SECRET '{S3_SECRET_KEY}',
            REGION 'us-east-1',
            ENDPOINT 'localhost:9000',
            URL_STYLE 'path',
            USE_SSL 'false'
        );
    """)

    # 4.1 Read data directly from Iceberg via DuckDB iceberg_scan
    print("\n  [4.1] Reading Iceberg metadata via DuckDB `iceberg_scan`:")
    iceberg_query = f"SELECT order_id, customer_name, amount FROM iceberg_scan('{metadata_location}') ORDER BY order_id;"
    iceberg_rows = duckdb_conn.execute(iceberg_query).fetchall()
    for row in iceberg_rows:
        print(f"    - Order #{row[0]}: {row[1]} | Price: ${row[2]}")

    assert len(iceberg_rows) == 3, f"Unexpected Iceberg row count: {len(iceberg_rows)}"
    assert iceberg_rows[0] == (101, "CongAnh", 250.50)

    # 4.2 Attach Native DuckLake Catalog and query
    ducklake_cat_path = os.path.join(temp_dir, "e2e_ducklake.ducklake").replace("\\", "/")
    print(f"\n  [4.2] Attaching Native DuckLake Catalog ({ducklake_cat_path}):")
    duckdb_conn.execute(f"ATTACH 'ducklake:{ducklake_cat_path}' AS e2e_lake;")
    
    # Create DuckLake table from the synchronized Iceberg state
    duckdb_conn.execute(f"""
        CREATE TABLE e2e_lake.{table_name} AS 
        SELECT * FROM iceberg_scan('{metadata_location}');
    """)

    ducklake_rows = duckdb_conn.execute(f"SELECT * FROM e2e_lake.{table_name} ORDER BY order_id;").fetchall()
    print("  -> Query result from Native DuckLake:")
    for row in ducklake_rows:
        print(f"    - Order #{row[0]}: {row[1]} | Price: ${row[2]}")

    assert len(ducklake_rows) == 3, f"Unexpected DuckLake row count: {len(ducklake_rows)}"
    assert ducklake_rows == iceberg_rows, "Data between Iceberg and DuckLake does not match!"

    print("\n======================================================================")
    print("SUCCESS: 100% DATA CONSISTENCY BETWEEN PYICEBERG AND DUCKLAKE!")
    print("======================================================================\n")


if __name__ == "__main__":
    test_e2e_write_pyiceberg_read_ducklake()
