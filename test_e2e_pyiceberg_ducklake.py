"""
E2E Integration Test:
1. Ghi dữ liệu Iceberg qua PyIceberg (Catalog SqlCatalog trỏ đến MinIO/S3).
2. Gửi commit POST metadata tới Bifrost Proxy (xác nhận giao thức Iceberg REST & CAS).
3. Đọc dữ liệu Iceberg qua DuckDB Iceberg engine và Native DuckLake catalog.
"""

import os
import time
import tempfile
from decimal import Decimal
import requests
import pyarrow as pa
import duckdb
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType, TimestampType, DecimalType


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
    run_id = int(time.time())
    table_name = f"fmcg_orders_{run_id}"
    full_table_name = f"{namespace}.{table_name}"

    try:
        iceberg_catalog.create_namespace(namespace)
    except Exception:
        pass

    # Schema definition with rich types (including Timestamp and Decimal)
    schema = Schema(
        NestedField(field_id=1, name="order_id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="customer_name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="amount", field_type=DoubleType(), required=False),
        NestedField(field_id=4, name="created_at", field_type=TimestampType(), required=False),
        NestedField(field_id=5, name="tax", field_type=DecimalType(precision=10, scale=2), required=False),
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
            pa.array([1700000000000000, 1700000001000000, 1700000002000000], type=pa.timestamp('us')),
            pa.array([Decimal("25.05"), Decimal("99.99"), Decimal("1.20")], type=pa.decimal128(10, 2)),
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
    # STEP 4: Contrast & Verify: Reading via Iceberg vs Reading via Pure DuckLake
    # -------------------------------------------------------------------------
    print("\n[4/4] Comparing Query Paths:")
    
    # 4.1. Reading via Iceberg Engine (iceberg_scan)
    print("\n  [4.1] Query Path 1: Iceberg Protocol (`iceberg_scan` from MinIO):")
    iceberg_conn = duckdb.connect()
    iceberg_conn.execute("INSTALL httpfs; LOAD httpfs;")
    iceberg_conn.execute("INSTALL iceberg; LOAD iceberg;")
    iceberg_conn.execute(f"""
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
    iceberg_rows = iceberg_conn.execute(
        f"SELECT order_id, customer_name, amount, tax, created_at FROM iceberg_scan('{metadata_location}') ORDER BY order_id;"
    ).fetchall()
    for row in iceberg_rows:
        print(f"    [Iceberg] Order #{row[0]}: {row[1]} | Price: ${row[2]} | Tax: ${row[3]} | Time: {row[4]}")
    iceberg_conn.close()

    # 4.2. Query Path 2: PURE NATIVE DUCKLAKE (ATTACH ducklake:postgres & direct table query, NO iceberg_scan)
    print(f"\n  [4.2] Query Path 2: Pure Native DuckLake (`ATTACH ducklake:postgres...`):")
    print("        -> Reading DuckLake metadata directly streamed into PostgreSQL by Bifrost Proxy...")

    # Dedicated PURE DuckLake Reader connection:
    pure_ducklake_reader = duckdb.connect()
    pure_ducklake_reader.execute("INSTALL ducklake; LOAD ducklake;")
    pure_ducklake_reader.execute("INSTALL postgres; LOAD postgres;")
    pure_ducklake_reader.execute("INSTALL httpfs; LOAD httpfs;")
    pure_ducklake_reader.execute(f"""
        CREATE SECRET s3_e2e_ducklake (
            TYPE S3,
            KEY_ID '{S3_ACCESS_KEY}',
            SECRET '{S3_SECRET_KEY}',
            REGION 'us-east-1',
            ENDPOINT 'localhost:9000',
            URL_STYLE 'path',
            USE_SSL 'false'
        );
    """)

    # Attach DuckLake from Postgres backend!
    pure_ducklake_reader.execute(f"""
        ATTACH 'ducklake:postgres:dbname=polaris host=localhost port=5432 user=admin password=password' 
        AS prod_lake (DATA_PATH 's3://lakehouse/{namespace}/', OVERRIDE_DATA_PATH true);
    """)

    # Deterministic polling: poll until table is visible and populated by async worker
    print(f"        -> Deterministic polling for prod_lake.{namespace}.{table_name}...")
    ducklake_rows = []
    max_retries = 30
    for attempt in range(max_retries):
        try:
            ducklake_rows = pure_ducklake_reader.execute(
                f"SELECT order_id, customer_name, amount, tax, created_at FROM prod_lake.{namespace}.{table_name} ORDER BY order_id;"
            ).fetchall()
            if len(ducklake_rows) == len(iceberg_rows):
                print(f"        -> Table query resolved successfully on attempt {attempt+1}!")
                break
        except Exception:
            pass
        time.sleep(0.1)

    for row in ducklake_rows:
        print(f"    [DuckLake] Order #{row[0]}: {row[1]} | Price: ${row[2]} | Tax: ${row[3]} | Time: {row[4]}")
    pure_ducklake_reader.close()

    assert len(ducklake_rows) == 3, f"Unexpected DuckLake row count: {len(ducklake_rows)}"
    assert ducklake_rows == iceberg_rows, "Data between Iceberg and Pure DuckLake does not match!"

    print("\n======================================================================")
    print("SUCCESS: PURE DUCKLAKE QUERY VERIFIED (NO ICEBERG SCAN NEEDED)!")
    print("======================================================================\n")


if __name__ == "__main__":
    test_e2e_write_pyiceberg_read_ducklake()
