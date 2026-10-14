package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"bifrost-proxy/storage"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
)

type ManifestColumnStats struct {
	ColumnID        int64
	ColumnSizeBytes *int64
	ValueCount      *int64
	NullCount       *int64
	MinValue        *string
	MaxValue        *string
	ContainsNaN     *bool
}

type ManifestDataFile struct {
	Path            string
	FileSizeInBytes int64
	RecordCount     int64
	Content         int    // 0 = data, 1 = position deletes, 2 = equality deletes
	ReferencedPath  string // field 2147483546 if delete file
	ColumnStats     []ManifestColumnStats
}

// DecodeIcebergBinaryBound deserializes raw bytes into a canonical string value
// according to Iceberg Single-Value Serialization Spec.
func DecodeIcebergBinaryBound(rawBytes []byte, icebergType string) (string, error) {
	if len(rawBytes) == 0 {
		return "", nil
	}
	t := strings.ToLower(strings.TrimSpace(icebergType))
	switch {
	case t == "boolean":
		if rawBytes[0] == 0x01 {
			return "true", nil
		}
		return "false", nil

	case t == "int" || t == "integer" || t == "int32":
		if len(rawBytes) < 4 {
			return "", fmt.Errorf("invalid byte length for int: %d", len(rawBytes))
		}
		val := int32(binary.LittleEndian.Uint32(rawBytes))
		return strconv.FormatInt(int64(val), 10), nil

	case t == "long" || t == "bigint" || t == "int64":
		if len(rawBytes) < 8 {
			return "", fmt.Errorf("invalid byte length for long: %d", len(rawBytes))
		}
		val := int64(binary.LittleEndian.Uint64(rawBytes))
		return strconv.FormatInt(val, 10), nil

	case t == "float" || t == "float32":
		if len(rawBytes) < 4 {
			return "", fmt.Errorf("invalid byte length for float: %d", len(rawBytes))
		}
		bits := binary.LittleEndian.Uint32(rawBytes)
		val := math.Float32frombits(bits)
		return strconv.FormatFloat(float64(val), 'g', -1, 32), nil

	case t == "double" || t == "float64":
		if len(rawBytes) < 8 {
			return "", fmt.Errorf("invalid byte length for double: %d", len(rawBytes))
		}
		bits := binary.LittleEndian.Uint64(rawBytes)
		val := math.Float64frombits(bits)
		return strconv.FormatFloat(val, 'g', -1, 64), nil

	case t == "date":
		if len(rawBytes) < 4 {
			return "", fmt.Errorf("invalid byte length for date: %d", len(rawBytes))
		}
		days := int32(binary.LittleEndian.Uint32(rawBytes))
		d := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days))
		return d.Format("2006-01-02"), nil

	case t == "time":
		if len(rawBytes) < 8 {
			return "", fmt.Errorf("invalid byte length for time: %d", len(rawBytes))
		}
		us := int64(binary.LittleEndian.Uint64(rawBytes))
		if us < 0 || us >= 86400000000 {
			return "", fmt.Errorf("time microseconds %d out of daily range [0, 86400000000)", us)
		}
		totalSec := us / 1000000
		remUS := us % 1000000
		hr := totalSec / 3600
		min := (totalSec % 3600) / 60
		sec := totalSec % 60
		return fmt.Sprintf("%02d:%02d:%02d.%06d", hr, min, sec, remUS), nil

	case strings.HasPrefix(t, "timestamp"):
		if len(rawBytes) < 8 {
			return "", fmt.Errorf("invalid byte length for timestamp: %d", len(rawBytes))
		}
		us := int64(binary.LittleEndian.Uint64(rawBytes))
		tm := time.UnixMicro(us).UTC()
		return tm.Format("2006-01-02 15:04:05.000000"), nil

	case t == "string" || t == "varchar":
		if utf8.Valid(rawBytes) {
			return string(rawBytes), nil
		}
		return "", fmt.Errorf("invalid utf8 sequence for string")

	case t == "uuid":
		if len(rawBytes) == 16 {
			u, err := uuid.FromBytes(rawBytes)
			if err != nil {
				return "", fmt.Errorf("failed to parse uuid bytes: %w", err)
			}
			return u.String(), nil
		}
		return "", fmt.Errorf("invalid byte length for uuid: %d", len(rawBytes))

	case strings.HasPrefix(t, "decimal"):
		unscaled := new(big.Int).SetBytes(rawBytes)
		if len(rawBytes) > 0 && (rawBytes[0]&0x80) != 0 {
			limit := new(big.Int).Lsh(big.NewInt(1), uint(8*len(rawBytes)))
			unscaled.Sub(unscaled, limit)
		}
		scale := 0
		if idx := strings.Index(t, ","); idx > 0 {
			scaleStr := strings.TrimRight(strings.TrimSpace(t[idx+1:]), ")")
			var err error
			scale, err = strconv.Atoi(scaleStr)
			if err != nil {
				return "", fmt.Errorf("invalid decimal scale in %q: %w", icebergType, err)
			}
		}
		if scale <= 0 {
			return unscaled.String(), nil
		}
		sign := ""
		if unscaled.Sign() < 0 {
			sign = "-"
			unscaled.Abs(unscaled)
		}
		str := unscaled.String()
		if len(str) <= scale {
			str = strings.Repeat("0", scale-len(str)+1) + str
		}
		intPart := str[:len(str)-scale]
		fracPart := str[len(str)-scale:]
		return sign + intPart + "." + fracPart, nil

	case t == "binary" || strings.HasPrefix(t, "fixed"):
		return hex.EncodeToString(rawBytes), nil

	default:
		if utf8.Valid(rawBytes) {
			return string(rawBytes), nil
		}
		return hex.EncodeToString(rawBytes), nil
	}
}

// extractMapOrArray parses Avro maps or arrays of key/value pairs into map[int64]interface{}
func extractMapOrArray(raw interface{}) map[int64]interface{} {
	result := make(map[int64]interface{})
	if raw == nil {
		return result
	}
	unwrapped := unwrapAvroUnion(raw)
	if m, ok := unwrapped.(map[string]interface{}); ok {
		for k, v := range m {
			if n, err := strconv.ParseInt(k, 10, 64); err == nil {
				result[n] = unwrapAvroUnion(v)
			}
		}
	} else if arr, ok := unwrapped.([]interface{}); ok {
		for _, item := range arr {
			if entryMap, ok := item.(map[string]interface{}); ok {
				if k, ok := unwrapInt64(entryMap["key"]); ok {
					result[k] = unwrapAvroUnion(entryMap["value"])
				}
			}
		}
	}
	return result
}

// collectUniqueKeys aggregates all unique int64 keys across multiple maps
func collectUniqueKeys(maps ...map[int64]interface{}) map[int64]bool {
	res := make(map[int64]bool)
	for _, m := range maps {
		for k := range m {
			res[k] = true
		}
	}
	return res
}

// unwrapAvroUnion extracts the inner concrete value from a goavro union representation (map[string]interface{})
func unwrapAvroUnion(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	if m, ok := val.(map[string]interface{}); ok {
		for _, inner := range m {
			return inner
		}
	}
	return val
}

// unwrapInt64 safely converts unwrapAvroUnion result to int64
func unwrapInt64(val interface{}) (int64, bool) {
	v := unwrapAvroUnion(val)
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

// ReadManifestList parses an Avro manifest list file and returns entries added in targetSnapshotID.
func ReadManifestList(data []byte, targetSnapshotID int64) ([]string, error) {
	ocf, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF reader for manifest list: %w", err)
	}

	var manifestPaths []string
	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest list record: %w", err)
		}
		record, ok := datum.(map[string]interface{})
		if !ok {
			continue
		}

		path, _ := record["manifest_path"].(string)
		if path == "" {
			continue
		}

		// If targetSnapshotID is 0, take all manifests
		if targetSnapshotID == 0 {
			manifestPaths = append(manifestPaths, path)
			continue
		}

		if addedSnap, ok := unwrapInt64(record["added_snapshot_id"]); ok {
			if addedSnap == targetSnapshotID {
				manifestPaths = append(manifestPaths, path)
			}
		}
	}
	return manifestPaths, nil
}

const IcebergReferencedDataFilePathFieldID int64 = 2147483546

// ReadManifestFile parses an Iceberg manifest Avro file and returns added files and deleted paths.
func ReadManifestFile(data []byte) (added []ManifestDataFile, deleted []string, err error) {
	return ReadManifestFileWithColTypes(data, nil)
}

// ReadManifestFileWithColTypes parses an Iceberg manifest Avro file with column types for bounds decoding.
func ReadManifestFileWithColTypes(data []byte, colTypes map[int64]string) (added []ManifestDataFile, deleted []string, err error) {
	ocf, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create OCF reader for manifest: %w", err)
	}

	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read manifest record: %w", err)
		}
		record, ok := datum.(map[string]interface{})
		if !ok {
			continue
		}

		// Iceberg manifest entry status: 0 = EXISTING, 1 = ADDED, 2 = DELETED
		statusVal, _ := unwrapInt64(record["status"])
		status := int32(statusVal)

		dataFileMap, ok := record["data_file"].(map[string]interface{})
		if !ok {
			continue
		}

		filePath, _ := dataFileMap["file_path"].(string)
		if filePath == "" {
			continue
		}

		if status == 0 {
			// EXISTING: We do not process existing files in delta commit
			continue
		} else if status == 2 {
			// DELETED: Record path to close active end_snapshot
			deleted = append(deleted, filePath)
			continue
		} else if status != 1 {
			// Ignore any unknown status
			continue
		}

		// status == 1 (ADDED)
		var manifestDataFile ManifestDataFile
		manifestDataFile.Path = filePath
		if s, ok := unwrapInt64(dataFileMap["file_size_in_bytes"]); ok {
			manifestDataFile.FileSizeInBytes = s
		}
		if r, ok := unwrapInt64(dataFileMap["record_count"]); ok {
			manifestDataFile.RecordCount = r
		}
		if c, ok := unwrapInt64(dataFileMap["content"]); ok {
			manifestDataFile.Content = int(c)
		}

		// Extract field 2147483546 (referenced data file path in position deletes)
		// Check lower_bounds & upper_bounds which can be map or array of key-value pairs
		extractRefPath := func(bounds interface{}) string {
			if bounds == nil {
				return ""
			}
			bVal := unwrapAvroUnion(bounds)
			// Map representation: {"2147483546": "s3://..." or {"bytes": [...]}}
			if m, ok := bVal.(map[string]interface{}); ok {
				var rawVal interface{}
				fieldKey := fmt.Sprintf("%d", IcebergReferencedDataFilePathFieldID)
				if val, exists := m[fieldKey]; exists {
					rawVal = val
				}
				if rawVal != nil {
					unwrapped := unwrapAvroUnion(rawVal)
					if b, ok := unwrapped.([]byte); ok {
						return string(b)
					}
					if s, ok := unwrapped.(string); ok {
						return s
					}
				}
			}
			// Array of key/value pairs: [{"key": 2147483546, "value": [...]}]
			if arr, ok := bVal.([]interface{}); ok {
				for _, item := range arr {
					if entryMap, ok := item.(map[string]interface{}); ok {
						k, _ := unwrapInt64(entryMap["key"])
						if k == IcebergReferencedDataFilePathFieldID {
							val := unwrapAvroUnion(entryMap["value"])
							if b, ok := val.([]byte); ok {
								return string(b)
							}
							if s, ok := val.(string); ok {
								return s
							}
						}
					}
				}
			}
			return ""
		}

		lowerRef := extractRefPath(dataFileMap["lower_bounds"])
		upperRef := extractRefPath(dataFileMap["upper_bounds"])
		if lowerRef != "" {
			manifestDataFile.ReferencedPath = lowerRef
		} else if upperRef != "" {
			manifestDataFile.ReferencedPath = upperRef
		}

		if manifestDataFile.ReferencedPath == "" {
			if ref, ok := dataFileMap["referenced_data_file"].(string); ok {
				manifestDataFile.ReferencedPath = ref
			}
		}

		// Extract column statistics (bounds, nulls, value counts, sizes)
		colSizes := extractMapOrArray(dataFileMap["column_sizes"])
		valCounts := extractMapOrArray(dataFileMap["value_counts"])
		nullCounts := extractMapOrArray(dataFileMap["null_value_counts"])
		nanCounts := extractMapOrArray(dataFileMap["nan_value_counts"])
		lowerBounds := extractMapOrArray(dataFileMap["lower_bounds"])
		upperBounds := extractMapOrArray(dataFileMap["upper_bounds"])

		allColIDs := collectUniqueKeys(colSizes, valCounts, nullCounts, nanCounts, lowerBounds, upperBounds)

		formatBound := func(v interface{}, colID int64) *string {
			if v == nil {
				return nil
			}
			unwrapped := unwrapAvroUnion(v)
			if b, ok := unwrapped.([]byte); ok {
				if len(b) == 0 {
					return nil
				}
				var colType string
				if colTypes != nil {
					colType = colTypes[colID]
				}
				decoded, err := DecodeIcebergBinaryBound(b, colType)
				if err == nil && decoded != "" {
					return &decoded
				}
				if colType != "" {
					if err != nil {
						log.Printf("Warning: failed to decode binary bound for column %d (%s): %v", colID, colType, err)
					}
					return nil
				}
				if utf8.Valid(b) {
					s := string(b)
					return &s
				}
				return nil
			}
			if s, ok := unwrapped.(string); ok {
				if utf8.ValidString(s) {
					return &s
				}
				return nil
			}
			s := fmt.Sprintf("%v", unwrapped)
			if utf8.ValidString(s) {
				return &s
			}
			return nil
		}

		for colID := range allColIDs {
			if colID == IcebergReferencedDataFilePathFieldID {
				continue
			}
			var stat ManifestColumnStats
			stat.ColumnID = colID
			if sz, ok := unwrapInt64(colSizes[colID]); ok {
				stat.ColumnSizeBytes = &sz
			}
			if vc, ok := unwrapInt64(valCounts[colID]); ok {
				stat.ValueCount = &vc
			}
			if nc, ok := unwrapInt64(nullCounts[colID]); ok {
				stat.NullCount = &nc
			}
			if nanVal, exists := nanCounts[colID]; exists {
				if nanCount, ok := unwrapInt64(nanVal); ok {
					hasNan := nanCount > 0
					stat.ContainsNaN = &hasNan
				}
			}
			stat.MinValue = formatBound(lowerBounds[colID], colID)
			stat.MaxValue = formatBound(upperBounds[colID], colID)

			manifestDataFile.ColumnStats = append(manifestDataFile.ColumnStats, stat)
		}

		added = append(added, manifestDataFile)
	}
	return added, deleted, nil
}

// ExtractDataFilesFromStorage traverses manifest list and manifests via StorageService.
func ExtractDataFilesFromStorage(ctx context.Context, store storage.StorageService, manifestListLocation string, snapshotID int64) (added []ManifestDataFile, deleted []string, err error) {
	return ExtractDataFilesFromStorageWithColTypes(ctx, store, manifestListLocation, snapshotID, nil)
}

// ExtractDataFilesFromStorageWithColTypes traverses manifest list and manifests using column types for bounds decoding.
func ExtractDataFilesFromStorageWithColTypes(ctx context.Context, store storage.StorageService, manifestListLocation string, snapshotID int64, colTypes map[int64]string) (added []ManifestDataFile, deleted []string, err error) {
	if store == nil || manifestListLocation == "" {
		return nil, nil, nil
	}

	listBytes, err := store.GetMetadataByLocation(ctx, manifestListLocation)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read manifest list from %s: %w", manifestListLocation, err)
	}

	manifestPaths, err := ReadManifestList(listBytes, snapshotID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode manifest list Avro from %s: %w", manifestListLocation, err)
	}

	for _, mPath := range manifestPaths {
		mBytes, err := store.GetMetadataByLocation(ctx, mPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read manifest file %s: %w", mPath, err)
		}
		addedFiles, deletedFiles, err := ReadManifestFileWithColTypes(mBytes, colTypes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse manifest file %s: %w", mPath, err)
		}
		added = append(added, addedFiles...)
		deleted = append(deleted, deletedFiles...)
	}
	return added, deleted, nil
}

