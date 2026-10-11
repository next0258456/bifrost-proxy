package engine_test

import (
	"testing"

	"bifrost-proxy/engine"

	"github.com/stretchr/testify/assert"
)

func TestParseMetadataJSON(t *testing.T) {
	sampleJSON := []byte(`{
		"format-version": 2,
		"table-uuid": "test-uuid-456",
		"current-snapshot-id": 123456789,
		"last-sequence-number": 5,
		"schemas": [
			{
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "product_name", "required": false, "type": "string"}
				]
			}
		],
		"snapshots": [
			{
				"snapshot-id": 123456789,
				"timestamp-ms": 1700000000000,
				"manifest-list": "s3://lakehouse/metadata/snap-123.avro",
				"summary": {
					"operation": "append",
					"added-data-files": "1",
					"added-records": "50"
				}
			}
		]
	}`)

	meta, err := engine.ParseMetadataJSON(sampleJSON)
	assert.NoError(t, err)
	assert.Equal(t, int64(123456789), meta.CurrentSnapshotID)
	assert.Equal(t, "test-uuid-456", meta.TableUUID)
	assert.Len(t, meta.Schemas[0].Fields, 2)
	assert.Equal(t, "product_name", meta.Schemas[0].Fields[1].Name)
	assert.Equal(t, "append", meta.Snapshots[0].Summary.Operation)
}
