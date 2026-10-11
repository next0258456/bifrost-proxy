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
		CREATE TABLE ducklake_catalog.ducklake_table (table_id BIGINT, table_name VARCHAR);
		CREATE TABLE ducklake_catalog.ducklake_snapshot (snapshot_id BIGINT, snapshot_time VARCHAR, schema_version INT, next_catalog_id INT, next_file_id INT);
		CREATE TABLE ducklake_catalog.ducklake_data_file (data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, path VARCHAR, record_count BIGINT, file_size_in_bytes BIGINT, partition_id INT);
		CREATE TABLE ducklake_catalog.ducklake_delete_file (delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, record_count BIGINT, file_size_in_bytes BIGINT);
		CREATE TABLE ducklake_catalog.ducklake_column (column_id INT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order INT, column_name VARCHAR, column_type VARCHAR);
		CREATE TABLE ducklake_catalog.ducklake_snapshot_changes (snapshot_id BIGINT, operation VARCHAR, c3 VARCHAR, c4 VARCHAR, c5 VARCHAR);
		INSERT INTO ducklake_catalog.ducklake_table VALUES (1, 'sales');
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
	err = db.QueryRow("SELECT path, record_count, file_size_in_bytes FROM ducklake_catalog.ducklake_data_file WHERE data_file_id = 1;").Scan(&dfPath, &dfRecords, &dfBytes)
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
}


