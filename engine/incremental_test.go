package engine_test

import (
	"context"
	"database/sql"
	"testing"

	"bifrost-proxy/domain"
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStorageService struct {
	files map[string][]byte
}

func (m *mockStorageService) GetMetadataByLocation(ctx context.Context, location string) ([]byte, error) {
	if data, ok := m.files[location]; ok {
		return data, nil
	}
	return nil, storage.ErrTableNotFound
}

func (m *mockStorageService) GetTableMetadata(ctx context.Context, id domain.TableIdentifier) ([]byte, error) {
	return m.GetMetadataByLocation(ctx, id.String())
}

func (m *mockStorageService) SaveTableMetadata(ctx context.Context, id domain.TableIdentifier, data []byte) error {
	return nil
}

func TestParseMetadataJSONAndExtractRealSnapshot(t *testing.T) {
	sampleJSON := []byte(`{
		"format-version": 2,
		"table-uuid": "test-uuid-555",
		"current-snapshot-id": 1001,
		"last-sequence-number": 1,
		"schemas": [
			{
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "order_id", "required": true, "type": "bigint"},
					{"id": 2, "name": "amount", "required": false, "type": "double"}
				]
			}
		],
		"snapshots": [
			{
				"snapshot-id": 1001,
				"timestamp-ms": 1699999999000,
				"manifest-list": "s3://lakehouse/bronze/sales/metadata/snap-1001.avro",
				"summary": {
					"operation": "append",
					"added-data-files": "2",
					"added-records": "500",
					"deleted-data-files": "0"
				},
				"data-files": [
					{
						"file_path": "s3://lakehouse/bronze/sales/data/00000-1-real.parquet",
						"file_size_in_bytes": 10240,
						"record_count": 250,
						"content": 0
					},
					{
						"file_path": "s3://lakehouse/bronze/sales/data/00001-1-real.parquet",
						"file_size_in_bytes": 10480,
						"record_count": 250,
						"content": 0
					}
				]
			}
		]
	}`)

	meta, err := engine.ParseMetadataJSON(sampleJSON)
	require.NoError(t, err)
	assert.Equal(t, 2, meta.FormatVersion)
	assert.Equal(t, "test-uuid-555", meta.TableUUID)
	assert.Equal(t, int64(1001), meta.CurrentSnapshotID)
	require.Len(t, meta.Snapshots, 1)

	snap := meta.Snapshots[0]
	assert.Equal(t, int64(1001), snap.SnapshotID)
	assert.Equal(t, "append", snap.Summary.Operation)
	assert.Equal(t, "500", snap.Summary.AddedRecords)
	require.Len(t, snap.DataFiles, 2)
	assert.Equal(t, "s3://lakehouse/bronze/sales/data/00000-1-real.parquet", snap.DataFiles[0].Path)
	assert.Equal(t, int64(250), snap.DataFiles[0].RecordCount)
	assert.Equal(t, int64(10240), snap.DataFiles[0].FileSizeInBytes)
	assert.NotContains(t, snap.DataFiles[0].Path, "snapshot-%d.parquet")
}

func TestDeleteV2AndDynamicSchemaEvolution(t *testing.T) {
	deleteV2JSON := []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-v2-evolution",
		"current-snapshot-id": 2002,
		"last-sequence-number": 2,
		"schemas": [
			{
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "order_id", "required": true, "type": "bigint"},
					{"id": 2, "name": "sku", "required": true, "type": "varchar"},
					{"id": 3, "name": "discount_pct", "required": false, "type": "float"}
				]
			}
		],
		"snapshots": [
			{
				"snapshot-id": 2002,
				"timestamp-ms": 1700000000000,
				"manifest-list": "s3://lakehouse/bronze/sales/metadata/snap-2002.avro",
				"summary": {
					"operation": "delete",
					"added-data-files": "0",
					"deleted-data-files": "0"
				},
				"data-files": [
					{
						"file_path": "s3://lakehouse/bronze/sales/data/delete-pos-001.parquet",
						"file_size_in_bytes": 2048,
						"record_count": 5,
						"content": 1,
						"referenced_path": "s3://lakehouse/bronze/sales/data/00000-1-real.parquet"
					}
				]
			}
		]
	}`)

	meta, err := engine.ParseMetadataJSON(deleteV2JSON)
	require.NoError(t, err)

	// Verify Schema Evolution
	require.Len(t, meta.Schemas, 1)
	fields := meta.Schemas[0].Fields
	require.Len(t, fields, 3)
	assert.Equal(t, "order_id", fields[0].Name)
	assert.Equal(t, "sku", fields[1].Name)
	assert.Equal(t, "discount_pct", fields[2].Name)

	// Verify Delete v2
	require.Len(t, meta.Snapshots, 1)
	snap := meta.Snapshots[0]
	assert.Equal(t, "delete", snap.Summary.Operation)
	require.Len(t, snap.DataFiles, 1)

	delFile := snap.DataFiles[0]
	assert.Equal(t, 1, delFile.Content)
	assert.Equal(t, "s3://lakehouse/bronze/sales/data/delete-pos-001.parquet", delFile.Path)
	assert.Equal(t, "s3://lakehouse/bronze/sales/data/00000-1-real.parquet", delFile.ReferencedPath)
}

func TestSyncDeltaIncrementalDirectExecution(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Initialize mock DuckLake catalog schema and tables
	_, err = db.Exec(`
		CREATE SCHEMA IF NOT EXISTS ducklake_catalog;
		CREATE TABLE ducklake_catalog.ducklake_schema (schema_id BIGINT, schema_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
		CREATE TABLE ducklake_catalog.ducklake_table (table_id BIGINT, table_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
		CREATE TABLE ducklake_catalog.ducklake_snapshot (snapshot_id BIGINT, snapshot_time VARCHAR, schema_version INT, next_catalog_id INT, next_file_id INT);
		CREATE TABLE ducklake_catalog.ducklake_data_file (data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, partition_id INT);
		CREATE TABLE ducklake_catalog.ducklake_delete_file (delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT);
		CREATE TABLE ducklake_catalog.ducklake_column (column_id INT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order INT, column_name VARCHAR, column_type VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, default_value_type VARCHAR, default_value_dialect VARCHAR);
		CREATE TABLE ducklake_catalog.ducklake_snapshot_changes (snapshot_id BIGINT, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
		INSERT INTO ducklake_catalog.ducklake_schema VALUES (1, '00000000-0000-0000-0000-000000000000', 0, NULL, 'bronze', 'bronze/', true);
		INSERT INTO ducklake_catalog.ducklake_table VALUES (1, 'uuid-sales', 0, NULL, 1, 'sales', 'sales/', true);
	`)
	require.NoError(t, err)

	metaJSON1 := []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-sales",
		"current-snapshot-id": 5001,
		"current-schema-id": 0,
		"last-sequence-number": 1,
		"schemas": [
			{
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "order_id", "required": true, "type": "bigint"},
					{"id": 2, "name": "amount", "required": false, "type": "double"}
				]
			}
		],
		"snapshots": [
			{
				"snapshot-id": 5001,
				"timestamp-ms": 1700000000000,
				"summary": {"operation": "append"},
				"data-files": [
					{
						"file_path": "s3://lakehouse/data/part-01.parquet",
						"file_size_in_bytes": 5000,
						"record_count": 100,
						"content": 0
					},
					{
						"file_path": "s3://lakehouse/data/del-01.parquet",
						"file_size_in_bytes": 1000,
						"record_count": 2,
						"content": 1,
						"referenced_path": "s3://lakehouse/data/part-01.parquet"
					}
				]
			}
		]
	}`)

	mockStore := &mockStorageService{
		files: map[string][]byte{
			"s3://lakehouse/metadata/v1.json": metaJSON1,
		},
	}

	syncEng := engine.NewTestSyncEngine(db, mockStore)

	task1 := engine.CommitTask{
		Table:            domain.NewTableIdentifier("bronze", "sales"),
		MetadataLocation: "s3://lakehouse/metadata/v1.json",
		SnapshotID:       5001,
	}

	err = syncEng.SyncDeltaIncremental(task1)
	require.NoError(t, err)

	// Verify Data File was inserted with real path and stats
	var dfPath string
	var dfRecords, dfBytes int64
	err = db.QueryRow("SELECT path, record_count, file_size_bytes FROM ducklake_catalog.ducklake_data_file WHERE data_file_id = 1;").Scan(&dfPath, &dfRecords, &dfBytes)
	require.NoError(t, err)
	assert.Equal(t, "s3://lakehouse/data/part-01.parquet", dfPath)
	assert.Equal(t, int64(100), dfRecords)
	assert.Equal(t, int64(5000), dfBytes)

	// Verify Delete File linked to data_file_id 1 via ReferencedPath
	var delPath string
	var refDataID int64
	err = db.QueryRow("SELECT path, data_file_id FROM ducklake_catalog.ducklake_delete_file WHERE delete_file_id = 1;").Scan(&delPath, &refDataID)
	require.NoError(t, err)
	assert.Equal(t, "s3://lakehouse/data/del-01.parquet", delPath)
	assert.Equal(t, int64(1), refDataID)

	// Verify Schema Columns
	var colCount int
	err = db.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_column WHERE table_id = 1 AND end_snapshot IS NULL;").Scan(&colCount)
	require.NoError(t, err)
	assert.Equal(t, 2, colCount)

	// Commit 2: Schema Evolution - drop 'amount' (ID 2), add 'customer_id' (ID 3)
	metaJSON2 := []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-sales",
		"current-snapshot-id": 5002,
		"current-schema-id": 1,
		"last-sequence-number": 2,
		"schemas": [
			{
				"schema-id": 1,
				"fields": [
					{"id": 1, "name": "order_id", "required": true, "type": "bigint"},
					{"id": 3, "name": "customer_id", "required": true, "type": "varchar"}
				]
			}
		],
		"snapshots": [
			{
				"snapshot-id": 5002,
				"timestamp-ms": 1700000001000,
				"summary": {"operation": "append"}
			}
		]
	}`)
	mockStore.files["s3://lakehouse/metadata/v2.json"] = metaJSON2

	task2 := engine.CommitTask{
		Table:            domain.NewTableIdentifier("bronze", "sales"),
		MetadataLocation: "s3://lakehouse/metadata/v2.json",
		SnapshotID:       5002,
	}

	err = syncEng.SyncDeltaIncremental(task2)
	require.NoError(t, err)

	// Verify dropped column (ID 2) now has end_snapshot = 5002
	var endSnap *int64
	err = db.QueryRow("SELECT end_snapshot FROM ducklake_catalog.ducklake_column WHERE table_id = 1 AND column_id = 2;").Scan(&endSnap)
	require.NoError(t, err)
	require.NotNil(t, endSnap)
	assert.Equal(t, int64(5002), *endSnap)

	// Verify new column (ID 3) has begin_snapshot = 5002, end_snapshot = NULL
	var beginSnap int64
	err = db.QueryRow("SELECT begin_snapshot FROM ducklake_catalog.ducklake_column WHERE table_id = 1 AND column_id = 3 AND end_snapshot IS NULL;").Scan(&beginSnap)
	require.NoError(t, err)
	assert.Equal(t, int64(5002), beginSnap)

	// Verify Audit info in ducklake_snapshot_changes
	var author, changesMade, commitMsg string
	err = db.QueryRow("SELECT author, changes_made, commit_message FROM ducklake_catalog.ducklake_snapshot_changes WHERE snapshot_id = 5002;").Scan(&author, &changesMade, &commitMsg)
	require.NoError(t, err)
	assert.Equal(t, "bifrost-proxy", author)
	assert.Equal(t, "append", changesMade)
	assert.Contains(t, commitMsg, "Commit snapshot 5002")
}

func TestMapIcebergTypeToDuckLake(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"long", "int64"},
		{"bigint", "int64"},
		{"int", "int32"},
		{"integer", "int32"},
		{"short", "int16"},
		{"string", "varchar"},
		{"varchar", "varchar"},
		{"double", "float64"},
		{"float", "float32"},
		{"boolean", "boolean"},
		{"date", "date"},
		{"time", "time"},
		{"timestamp", "timestamp"},
		{"timestamptz", "timestamptz"},
		{"timestamp with time zone", "timestamptz"},
		{"timestamp_tz", "timestamptz"},
		{"uuid", "uuid"},
		{"binary", "blob"},
		{"fixed", "blob"},
		{"fixed[16]", "blob"},
		{"fixed[32]", "blob"},
		{"decimal(18, 4)", "decimal(18, 4)"},
		{"json", "json"},
		{"list<string>", "varchar[]"},
		{"list<int>", "int32[]"},
		{"array<long>", "int64[]"},
		{"list<list<double>>", "float64[][]"},
		{"map<string, int>", "MAP(varchar, int32)"},
		{"map<string, list<long>>", "MAP(varchar, int64[])"},
		{"struct<id: int, name: string>", "STRUCT(id int32, name varchar)"},
		{"struct<user_id: string, tags: list<string>>", "STRUCT(user_id varchar, tags varchar[])"},
		{"", "varchar"},
		{"custom_type", "custom_type"},
	}

	for _, tc := range testCases {
		actual := engine.MapIcebergTypeToDuckLake(tc.input)
		assert.Equal(t, tc.expected, actual, "Type mapping mismatch for: %s", tc.input)
	}
}

func TestSyncDeltaPositionDeleteFailsOnMissingTargetFile(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Initialize mock DuckLake catalog schema and tables
	_, err = db.Exec(`
		CREATE SCHEMA IF NOT EXISTS ducklake_catalog;
		CREATE TABLE ducklake_catalog.ducklake_schema (schema_id BIGINT, schema_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
		CREATE TABLE ducklake_catalog.ducklake_table (table_id BIGINT, table_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
		CREATE TABLE ducklake_catalog.ducklake_snapshot (snapshot_id BIGINT, snapshot_time VARCHAR, schema_version INT, next_catalog_id INT, next_file_id INT);
		CREATE TABLE ducklake_catalog.ducklake_data_file (data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, partition_id INT);
		CREATE TABLE ducklake_catalog.ducklake_delete_file (delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT);
		CREATE TABLE ducklake_catalog.ducklake_column (column_id INT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order INT, column_name VARCHAR, column_type VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, default_value_type VARCHAR, default_value_dialect VARCHAR);
		CREATE TABLE ducklake_catalog.ducklake_snapshot_changes (snapshot_id BIGINT, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
		INSERT INTO ducklake_catalog.ducklake_schema VALUES (1, '00000000-0000-0000-0000-000000000000', 0, NULL, 'bronze', 'bronze/', true);
		INSERT INTO ducklake_catalog.ducklake_table VALUES (1, 'uuid-sales', 0, NULL, 1, 'sales', 'sales/', true);
	`)
	require.NoError(t, err)

	// Snapshot containing a position delete referencing non-existent data file
	deleteMetaJSON := []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-sales",
		"current-snapshot-id": 6001,
		"current-schema-id": 0,
		"last-sequence-number": 1,
		"schemas": [
			{
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "order_id", "required": true, "type": "bigint"}
				]
			}
		],
		"snapshots": [
			{
				"snapshot-id": 6001,
				"timestamp-ms": 1700000000000,
				"summary": {"operation": "delete"},
				"data-files": [
					{
						"file_path": "s3://lakehouse/data/orphan_delete.parquet",
						"file_size_in_bytes": 1000,
						"record_count": 2,
						"content": 1,
						"referenced_path": "s3://lakehouse/data/non_existent.parquet"
					}
				]
			}
		]
	}`)

	mockStore := &mockStorageService{
		files: map[string][]byte{
			"s3://lakehouse/metadata/v6.json": deleteMetaJSON,
		},
	}

	syncEng := engine.NewTestSyncEngine(db, mockStore)

	task := engine.CommitTask{
		Table:            domain.NewTableIdentifier("bronze", "sales"),
		MetadataLocation: "s3://lakehouse/metadata/v6.json",
		SnapshotID:       6001,
	}

	err = syncEng.SyncDeltaIncremental(task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve active referenced data file ID")

	// Verify Transactional Rollback: snapshot 6001 was inserted before the error, but MUST be rolled back
	var snapCount int
	err = db.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_snapshot WHERE snapshot_id = 6001;").Scan(&snapCount)
	require.NoError(t, err)
	assert.Equal(t, 0, snapCount, "Snapshot 6001 must be rolled back completely")
}

func TestSyncDeltaTransactionalRollbackOnNewTableFailure(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Initialize mock DuckLake catalog schema and tables
	_, err = db.Exec(`
		CREATE SCHEMA IF NOT EXISTS ducklake_catalog;
		CREATE TABLE ducklake_catalog.ducklake_schema (schema_id BIGINT, schema_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
		CREATE TABLE ducklake_catalog.ducklake_table (table_id BIGINT, table_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
		CREATE TABLE ducklake_catalog.ducklake_snapshot (snapshot_id BIGINT, snapshot_time VARCHAR, schema_version INT, next_catalog_id INT, next_file_id INT);
		CREATE TABLE ducklake_catalog.ducklake_data_file (data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, partition_id INT);
		CREATE TABLE ducklake_catalog.ducklake_delete_file (delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT);
		CREATE TABLE ducklake_catalog.ducklake_column (column_id INT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order INT, column_name VARCHAR, column_type VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, default_value_type VARCHAR, default_value_dialect VARCHAR);
		CREATE TABLE ducklake_catalog.ducklake_snapshot_changes (snapshot_id BIGINT, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
	`)
	require.NoError(t, err)

	// New table 'finance.invoices' with an invalid delete file referencing a non-existent file
	metaJSON := []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-invoices",
		"current-snapshot-id": 7001,
		"current-schema-id": 0,
		"last-sequence-number": 1,
		"schemas": [
			{
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "inv_id", "required": true, "type": "bigint"}
				]
			}
		],
		"snapshots": [
			{
				"snapshot-id": 7001,
				"timestamp-ms": 1700000000000,
				"summary": {"operation": "delete"},
				"data-files": [
					{
						"file_path": "s3://lakehouse/data/bad_delete.parquet",
						"file_size_in_bytes": 1000,
						"record_count": 2,
						"content": 1,
						"referenced_path": "s3://lakehouse/data/missing_inv.parquet"
					}
				]
			}
		]
	}`)

	mockStore := &mockStorageService{
		files: map[string][]byte{
			"s3://lakehouse/metadata/v7.json": metaJSON,
		},
	}

	syncEng := engine.NewTestSyncEngine(db, mockStore)

	task := engine.CommitTask{
		Table:            domain.NewTableIdentifier("finance", "invoices"),
		MetadataLocation: "s3://lakehouse/metadata/v7.json",
		SnapshotID:       7001,
	}

	err = syncEng.SyncDeltaIncremental(task)
	require.Error(t, err)

	// Ensure neither schema 'finance', table 'invoices', nor snapshot 7001 exist
	var schemaCount, tableCount, snapCount int
	err = db.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_schema WHERE schema_name = 'finance';").Scan(&schemaCount)
	require.NoError(t, err)
	assert.Equal(t, 0, schemaCount, "Schema 'finance' should be rolled back")

	err = db.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_table WHERE table_name = 'invoices';").Scan(&tableCount)
	require.NoError(t, err)
	assert.Equal(t, 0, tableCount, "Table 'invoices' should be rolled back")

	err = db.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_snapshot WHERE snapshot_id = 7001;").Scan(&snapCount)
	require.NoError(t, err)
	assert.Equal(t, 0, snapCount, "Snapshot 7001 should be rolled back")
}





