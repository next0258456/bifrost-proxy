package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnwrapAvroUnion(t *testing.T) {
	assert.Nil(t, unwrapAvroUnion(nil))
	assert.Equal(t, int64(123), unwrapAvroUnion(int64(123)))
	assert.Equal(t, "hello", unwrapAvroUnion("hello"))

	// Map representation from goavro
	unionLong := map[string]interface{}{"long": int64(456)}
	assert.Equal(t, int64(456), unwrapAvroUnion(unionLong))

	unionBytes := map[string]interface{}{"bytes": []byte("test-data")}
	assert.Equal(t, []byte("test-data"), unwrapAvroUnion(unionBytes))
}

func TestUnwrapInt64(t *testing.T) {
	v, ok := unwrapInt64(int64(100))
	assert.True(t, ok)
	assert.Equal(t, int64(100), v)

	v, ok = unwrapInt64(int(50))
	assert.True(t, ok)
	assert.Equal(t, int64(50), v)

	v, ok = unwrapInt64(int32(25))
	assert.True(t, ok)
	assert.Equal(t, int64(25), v)

	v, ok = unwrapInt64(float64(75))
	assert.True(t, ok)
	assert.Equal(t, int64(75), v)

	// In union
	v, ok = unwrapInt64(map[string]interface{}{"long": int64(999)})
	assert.True(t, ok)
	assert.Equal(t, int64(999), v)

	// Non-numbers
	_, ok = unwrapInt64("not a number")
	assert.False(t, ok)

	_, ok = unwrapInt64(nil)
	assert.False(t, ok)
}

func TestReadManifestListWithAvroUnion(t *testing.T) {
	manifestListSchema := `{
		"type": "record",
		"name": "manifest_file",
		"fields": [
			{"name": "manifest_path", "type": "string"},
			{"name": "manifest_length", "type": "long"},
			{"name": "partition_spec_id", "type": "int"},
			{"name": "added_snapshot_id", "type": ["null", "long"], "default": null}
		]
	}`

	var buf bytes.Buffer
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: manifestListSchema,
	})
	require.NoError(t, err)

	records := []map[string]interface{}{
		{
			"manifest_path":     "s3://lakehouse/metadata/snap1-m0.avro",
			"manifest_length":   int64(1024),
			"partition_spec_id": 0,
			"added_snapshot_id": map[string]interface{}{"long": int64(1001)},
		},
		{
			"manifest_path":     "s3://lakehouse/metadata/snap2-m1.avro",
			"manifest_length":   int64(2048),
			"partition_spec_id": 0,
			"added_snapshot_id": map[string]interface{}{"long": int64(1002)},
		},
	}
	err = w.Append(records)
	require.NoError(t, err)

	// Query for snapshot 1002 only
	paths, err := ReadManifestList(buf.Bytes(), 1002)
	require.NoError(t, err)
	assert.Equal(t, []string{"s3://lakehouse/metadata/snap2-m1.avro"}, paths)

	// Query all (target 0)
	allPaths, err := ReadManifestList(buf.Bytes(), 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(allPaths))
}

func TestReadManifestFileFilteringAndBounds(t *testing.T) {
	manifestFileSchema := `{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int"},
			{"name": "snapshot_id", "type": ["null", "long"], "default": null},
			{
				"name": "data_file",
				"type": {
					"type": "record",
					"name": "r102",
					"fields": [
						{"name": "file_path", "type": "string"},
						{"name": "file_format", "type": "string"},
						{"name": "record_count", "type": "long"},
						{"name": "file_size_in_bytes", "type": "long"},
						{"name": "content", "type": "int", "default": 0},
						{
							"name": "lower_bounds",
							"type": [
								"null",
								{"type": "map", "values": "bytes"}
							],
							"default": null
						},
						{
							"name": "upper_bounds",
							"type": [
								"null",
								{"type": "map", "values": "bytes"}
							],
							"default": null
						}
					]
				}
			}
		]
	}`

	var buf bytes.Buffer
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: manifestFileSchema,
	})
	require.NoError(t, err)

	entries := []map[string]interface{}{
		// Entry 1: status 1 (ADDED) data file
		{
			"status":      1,
			"snapshot_id": map[string]interface{}{"long": int64(1001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/added1.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(100),
				"file_size_in_bytes": int64(5000),
				"content":            0,
				"lower_bounds":       nil,
				"upper_bounds":       nil,
			},
		},
		// Entry 2: status 0 (EXISTING) - must be ignored!
		{
			"status":      0,
			"snapshot_id": map[string]interface{}{"long": int64(1000)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/existing.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(50),
				"file_size_in_bytes": int64(2500),
				"content":            0,
				"lower_bounds":       nil,
				"upper_bounds":       nil,
			},
		},
		// Entry 3: status 2 (DELETED) - must be added to deletedFiles!
		{
			"status":      2,
			"snapshot_id": map[string]interface{}{"long": int64(1001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/deleted1.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(30),
				"file_size_in_bytes": int64(1500),
				"content":            0,
				"lower_bounds":       nil,
				"upper_bounds":       nil,
			},
		},
		// Entry 4: status 1 (ADDED) Position Delete file with lower_bounds field 2147483546
		{
			"status":      1,
			"snapshot_id": map[string]interface{}{"long": int64(1001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/delete1.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(1),
				"file_size_in_bytes": int64(800),
				"content":            1,
				"lower_bounds": map[string]interface{}{
					"map": map[string]interface{}{
						"2147483546": []byte("s3://lakehouse/data/added1.parquet"),
					},
				},
				"upper_bounds": nil,
			},
		},
		// Entry 5: status 99 (UNKNOWN) - must be ignored!
		{
			"status":      99,
			"snapshot_id": map[string]interface{}{"long": int64(1001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/unknown.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(10),
				"file_size_in_bytes": int64(500),
				"content":            0,
				"lower_bounds":       nil,
				"upper_bounds":       nil,
			},
		},
	}

	err = w.Append(entries)
	require.NoError(t, err)

	added, deleted, err := ReadManifestFile(buf.Bytes())
	require.NoError(t, err)

	// Verify only status 1 entries are in added
	assert.Equal(t, 2, len(added), "Only status 1 entries should be added")
	assert.Equal(t, "s3://lakehouse/data/added1.parquet", added[0].Path)
	assert.Equal(t, 0, added[0].Content)

	assert.Equal(t, "s3://lakehouse/data/delete1.parquet", added[1].Path)
	assert.Equal(t, 1, added[1].Content)
	assert.Equal(t, "s3://lakehouse/data/added1.parquet", added[1].ReferencedPath, "Position delete must extract referenced data file path")

	// Verify status 2 is in deleted
	assert.Equal(t, []string{"s3://lakehouse/data/deleted1.parquet"}, deleted)
}

func TestReadManifestFileWithArrayBounds(t *testing.T) {
	manifestFileSchema := `{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int"},
			{"name": "snapshot_id", "type": ["null", "long"], "default": null},
			{
				"name": "data_file",
				"type": {
					"type": "record",
					"name": "r103",
					"fields": [
						{"name": "file_path", "type": "string"},
						{"name": "file_format", "type": "string"},
						{"name": "record_count", "type": "long"},
						{"name": "file_size_in_bytes", "type": "long"},
						{"name": "content", "type": "int", "default": 0},
						{
							"name": "lower_bounds",
							"type": [
								"null",
								{
									"type": "array",
									"items": {
										"type": "record",
										"name": "k_v",
										"fields": [
											{"name": "key", "type": "int"},
											{"name": "value", "type": "bytes"}
										]
									}
								}
							],
							"default": null
						},
						{
							"name": "upper_bounds",
							"type": "null",
							"default": null
						}
					]
				}
			}
		]
	}`

	var buf bytes.Buffer
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: manifestFileSchema,
	})
	require.NoError(t, err)

	entries := []map[string]interface{}{
		{
			"status":      1,
			"snapshot_id": map[string]interface{}{"long": int64(2001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/delete_arr.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(2),
				"file_size_in_bytes": int64(1200),
				"content":            1,
				"lower_bounds": map[string]interface{}{
					"array": []interface{}{
						map[string]interface{}{
							"key":   int(IcebergReferencedDataFilePathFieldID),
							"value": []byte("s3://lakehouse/data/target.parquet"),
						},
					},
				},
				"upper_bounds": nil,
			},
		},
	}

	err = w.Append(entries)
	require.NoError(t, err)

	added, _, err := ReadManifestFile(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1, len(added))
	assert.Equal(t, "s3://lakehouse/data/target.parquet", added[0].ReferencedPath)
}

func TestReadManifestFileWithMapUnionEnvelope(t *testing.T) {
	manifestFileSchema := `{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int"},
			{"name": "snapshot_id", "type": ["null", "long"], "default": null},
			{
				"name": "data_file",
				"type": {
					"type": "record",
					"name": "r104",
					"fields": [
						{"name": "file_path", "type": "string"},
						{"name": "file_format", "type": "string"},
						{"name": "record_count", "type": "long"},
						{"name": "file_size_in_bytes", "type": "long"},
						{"name": "content", "type": "int", "default": 0},
						{
							"name": "lower_bounds",
							"type": [
								"null",
								{
									"type": "map",
									"values": ["null", "bytes"]
								}
							],
							"default": null
						},
						{
							"name": "upper_bounds",
							"type": "null",
							"default": null
						}
					]
				}
			}
		]
	}`

	var buf bytes.Buffer
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: manifestFileSchema,
	})
	require.NoError(t, err)

	targetKey := fmt.Sprintf("%d", IcebergReferencedDataFilePathFieldID)
	entries := []map[string]interface{}{
		{
			"status":      1,
			"snapshot_id": map[string]interface{}{"long": int64(3001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/delete_map_union.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(5),
				"file_size_in_bytes": int64(1500),
				"content":            1,
				"lower_bounds": map[string]interface{}{
					"map": map[string]interface{}{
						targetKey: map[string]interface{}{
							"bytes": []byte("s3://lakehouse/data/map_target.parquet"),
						},
					},
				},
				"upper_bounds": nil,
			},
		},
	}

	err = w.Append(entries)
	require.NoError(t, err)

	added, _, err := ReadManifestFile(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1, len(added))
	assert.Equal(t, "s3://lakehouse/data/map_target.parquet", added[0].ReferencedPath)
}

func TestExtractColumnBoundsStats(t *testing.T) {
	manifestSchema := `{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int"},
			{"name": "snapshot_id", "type": ["null", "long"], "default": null},
			{
				"name": "data_file",
				"type": {
					"type": "record",
					"name": "r102",
					"fields": [
						{"name": "file_path", "type": "string"},
						{"name": "file_format", "type": "string"},
						{"name": "record_count", "type": "long"},
						{"name": "file_size_in_bytes", "type": "long"},
						{"name": "content", "type": "int", "default": 0},
						{"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
						{"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
						{"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
						{"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
						{"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
						{"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null}
					]
				}
			}
		]
	}`

	codec, err := goavro.NewCodec(manifestSchema)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     buf,
		Codec: codec,
	})
	require.NoError(t, err)

	entries := []map[string]interface{}{
		{
			"status":      1,
			"snapshot_id": map[string]interface{}{"long": int64(4001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/stats_test.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(100),
				"file_size_in_bytes": int64(4096),
				"content":            0,
				"column_sizes": map[string]interface{}{
					"map": map[string]interface{}{
						"1": int64(1024),
						"2": int64(2048),
					},
				},
				"value_counts": map[string]interface{}{
					"map": map[string]interface{}{
						"1": int64(100),
						"2": int64(100),
					},
				},
				"null_value_counts": map[string]interface{}{
					"map": map[string]interface{}{
						"1": int64(0),
						"2": int64(5),
					},
				},
				"nan_value_counts": map[string]interface{}{
					"map": map[string]interface{}{
						"1": int64(0),
					},
				},
				"lower_bounds": map[string]interface{}{
					"map": map[string]interface{}{
						"1": []byte("101"),
						"2": []byte("Alice"),
					},
				},
				"upper_bounds": map[string]interface{}{
					"map": map[string]interface{}{
						"1": []byte("200"),
						"2": []byte("Zach"),
					},
				},
			},
		},
	}

	err = w.Append(entries)
	require.NoError(t, err)

	added, _, err := ReadManifestFile(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1, len(added))

	df := added[0]
	assert.Equal(t, "s3://lakehouse/data/stats_test.parquet", df.Path)
	assert.Equal(t, int64(100), df.RecordCount)
	assert.Equal(t, 2, len(df.ColumnStats))

	// Find stats for col 1 and col 2
	statsMap := make(map[int64]ManifestColumnStats)
	for _, cs := range df.ColumnStats {
		statsMap[cs.ColumnID] = cs
	}

	col1, ok1 := statsMap[1]
	require.True(t, ok1)
	assert.Equal(t, int64(1024), *col1.ColumnSizeBytes)
	assert.Equal(t, int64(100), *col1.ValueCount)
	assert.Equal(t, int64(0), *col1.NullCount)
	assert.Equal(t, "101", *col1.MinValue)
	assert.Equal(t, "200", *col1.MaxValue)
	require.NotNil(t, col1.ContainsNaN)
	assert.False(t, *col1.ContainsNaN)

	col2, ok2 := statsMap[2]
	require.True(t, ok2)
	assert.Equal(t, int64(2048), *col2.ColumnSizeBytes)
	assert.Equal(t, int64(100), *col2.ValueCount)
	assert.Equal(t, int64(5), *col2.NullCount)
	assert.Equal(t, "Alice", *col2.MinValue)
	assert.Equal(t, "Zach", *col2.MaxValue)
}

func TestDecodeIcebergBinaryBound(t *testing.T) {
	// Boolean
	res, err := DecodeIcebergBinaryBound([]byte{0x01}, "boolean")
	require.NoError(t, err)
	assert.Equal(t, "true", res)

	res, err = DecodeIcebergBinaryBound([]byte{0x00}, "boolean")
	require.NoError(t, err)
	assert.Equal(t, "false", res)

	// Int32
	intBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(intBuf, uint32(101))
	res, err = DecodeIcebergBinaryBound(intBuf, "int")
	require.NoError(t, err)
	assert.Equal(t, "101", res)

	// Int64 / Long
	longBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(longBuf, uint64(10000000000))
	res, err = DecodeIcebergBinaryBound(longBuf, "long")
	require.NoError(t, err)
	assert.Equal(t, "10000000000", res)

	// Float32
	f32Buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(f32Buf, math.Float32bits(250.5))
	res, err = DecodeIcebergBinaryBound(f32Buf, "float")
	require.NoError(t, err)
	assert.Equal(t, "250.5", res)

	// Float64 / Double
	f64Buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(f64Buf, math.Float64bits(999.99))
	res, err = DecodeIcebergBinaryBound(f64Buf, "double")
	require.NoError(t, err)
	assert.Equal(t, "999.99", res)

	// Date (days from 1970-01-01)
	dateBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(dateBuf, uint32(19680)) // 2023-11-19
	res, err = DecodeIcebergBinaryBound(dateBuf, "date")
	require.NoError(t, err)
	assert.Equal(t, "2023-11-19", res)

	// Timestamp (microseconds from epoch)
	tsBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBuf, uint64(1700000000000000))
	res, err = DecodeIcebergBinaryBound(tsBuf, "timestamp")
	require.NoError(t, err)
	assert.Equal(t, "2023-11-14 22:13:20.000000", res)

	// String
	res, err = DecodeIcebergBinaryBound([]byte("CongAnh"), "string")
	require.NoError(t, err)
	assert.Equal(t, "CongAnh", res)

	// Decimal(18, 2)
	// 1250 cents = 12.50
	decBytes := []byte{0x04, 0xe2} // 1250 in big-endian
	res, err = DecodeIcebergBinaryBound(decBytes, "decimal(18, 2)")
	require.NoError(t, err)
	assert.Equal(t, "12.50", res)
}

func TestReadManifestFileWithColTypesAndBinaryBounds(t *testing.T) {
	manifestSchema := `{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int"},
			{"name": "snapshot_id", "type": ["null", "long"], "default": null},
			{
				"name": "data_file",
				"type": {
					"type": "record",
					"name": "data_file_entry",
					"fields": [
						{"name": "file_path", "type": "string"},
						{"name": "file_format", "type": "string"},
						{"name": "record_count", "type": "long"},
						{"name": "file_size_in_bytes", "type": "long"},
						{"name": "content", "type": "int", "default": 0},
						{
							"name": "lower_bounds",
							"type": [
								"null",
								{"type": "map", "values": "bytes"}
							],
							"default": null
						},
						{
							"name": "upper_bounds",
							"type": [
								"null",
								{"type": "map", "values": "bytes"}
							],
							"default": null
						}
					]
				}
			}
		]
	}`

	buf := new(bytes.Buffer)
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      buf,
		Schema: manifestSchema,
	})
	require.NoError(t, err)

	// Col 1 = long (order_id: 101 .. 105)
	minIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(minIDBytes, 101)
	maxIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(maxIDBytes, 105)

	// Col 2 = double (amount: 12.0 .. 999.99)
	minAmtBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(minAmtBytes, math.Float64bits(12.0))
	maxAmtBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(maxAmtBytes, math.Float64bits(999.99))

	entries := []map[string]interface{}{
		{
			"status":      1,
			"snapshot_id": map[string]interface{}{"long": int64(1001)},
			"data_file": map[string]interface{}{
				"file_path":          "s3://lakehouse/data/orders_1.parquet",
				"file_format":        "PARQUET",
				"record_count":       int64(5),
				"file_size_in_bytes": int64(4096),
				"content":            0,
				"lower_bounds": map[string]interface{}{
					"map": map[string]interface{}{
						"1": minIDBytes,
						"2": minAmtBytes,
					},
				},
				"upper_bounds": map[string]interface{}{
					"map": map[string]interface{}{
						"1": maxIDBytes,
						"2": maxAmtBytes,
					},
				},
			},
		},
	}

	err = w.Append(entries)
	require.NoError(t, err)

	colTypes := map[int64]string{
		1: "long",
		2: "double",
	}

	added, _, err := ReadManifestFileWithColTypes(buf.Bytes(), colTypes)
	require.NoError(t, err)
	require.Equal(t, 1, len(added))

	df := added[0]
	require.Equal(t, 2, len(df.ColumnStats))

	statsMap := make(map[int64]ManifestColumnStats)
	for _, cs := range df.ColumnStats {
		statsMap[cs.ColumnID] = cs
	}

	col1, ok1 := statsMap[1]
	require.True(t, ok1)
	require.NotNil(t, col1.MinValue)
	require.NotNil(t, col1.MaxValue)
	assert.Equal(t, "101", *col1.MinValue)
	assert.Equal(t, "105", *col1.MaxValue)

	col2, ok2 := statsMap[2]
	require.True(t, ok2)
	require.NotNil(t, col2.MinValue)
	require.NotNil(t, col2.MaxValue)
	assert.Equal(t, "12", *col2.MinValue)
	assert.Equal(t, "999.99", *col2.MaxValue)
}

