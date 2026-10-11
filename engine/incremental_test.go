package engine_test

import (
	"context"
	"testing"

	"bifrost-proxy/domain"
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

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

