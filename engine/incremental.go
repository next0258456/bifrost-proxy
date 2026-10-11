package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// IcebergMetadata represents the essential Iceberg table metadata fields
type IcebergMetadata struct {
	FormatVersion      int             `json:"format-version"`
	TableUUID          string          `json:"table-uuid"`
	CurrentSnapshotID  int64           `json:"current-snapshot-id"`
	CurrentSchemaID    int             `json:"current-schema-id"`
	LastSequenceNumber int64           `json:"last-sequence-number"`
	Schemas            []SchemaEntry   `json:"schemas"`
	Snapshots          []SnapshotEntry `json:"snapshots"`
}

type SchemaEntry struct {
	SchemaID int          `json:"schema-id"`
	Fields   []FieldEntry `json:"fields"`
}

type FieldEntry struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

type SnapshotEntry struct {
	SnapshotID   int64           `json:"snapshot-id"`
	TimestampMS  int64           `json:"timestamp-ms"`
	ManifestList string          `json:"manifest-list"`
	Summary      SnapshotSummary `json:"summary"`
	DataFiles    []DataFileEntry `json:"data-files,omitempty"`
}

type SnapshotSummary struct {
	Operation        string `json:"operation"`
	AddedDataFiles   string `json:"added-data-files"`
	AddedRecords     string `json:"added-records"`
	DeletedDataFiles string `json:"deleted-data-files"`
}

type DataFileEntry struct {
	Path            string `json:"file_path"`
	FileSizeInBytes int64  `json:"file_size_in_bytes"`
	RecordCount     int64  `json:"record_count"`
	Content         int    `json:"content"` // 0 = data, 1 = position deletes
	ReferencedPath  string `json:"referenced_path,omitempty"` // field 2147483546
}

// SyncDeltaIncremental synchronises one committed Iceberg snapshot into DuckLake.
func (e *SyncEngine) SyncDeltaIncremental(task CommitTask) error {
	log.Printf("Starting delta sync for %s (snapshot: %d)", task.Table, task.SnapshotID)

	// 1. Tải metadata.json thật qua StorageService hoặc fallback
	var meta *IcebergMetadata
	if e.store != nil {
		data, err := e.store.GetMetadataByLocation(context.Background(), task.MetadataLocation)
		if err == nil {
			meta, err = ParseMetadataJSON(data)
			if err != nil {
				log.Printf("Warning: failed to parse metadata JSON from storage: %v", err)
			}
		} else {
			log.Printf("Warning: failed to load metadata from storage for %s: %v", task.MetadataLocation, err)
		}
	}
	if meta == nil {
		meta, _ = ParseMetadataFile(task.MetadataLocation)
	}

	// 2. Trích xuất đúng Snapshot và Summary delta thật
	snapshotID := task.SnapshotID
	var targetSnap *SnapshotEntry

	if meta != nil {
		for i := range meta.Snapshots {
			s := &meta.Snapshots[i]
			if snapshotID != 0 && s.SnapshotID == snapshotID {
				targetSnap = s
				break
			}
			if snapshotID == 0 && s.SnapshotID == meta.CurrentSnapshotID {
				targetSnap = s
				snapshotID = s.SnapshotID
				break
			}
		}
		if targetSnap == nil && len(meta.Snapshots) > 0 {
			targetSnap = &meta.Snapshots[len(meta.Snapshots)-1]
			if snapshotID == 0 {
				snapshotID = targetSnap.SnapshotID
			}
		}
	}
	if snapshotID == 0 {
		snapshotID = time.Now().UnixMilli()
	}

	operation := "append"
	if targetSnap != nil && targetSnap.Summary.Operation != "" {
		operation = targetSnap.Summary.Operation
	}

	// 3. Kiểm tra xem bảng đã tồn tại trong DuckLake chưa
	var tableCount int
	var tableID int64 = 1
	checkQuery := "SELECT count(*), COALESCE(MAX(table_id), 1) FROM ducklake_catalog.ducklake_table WHERE table_name = $1;"
	err := e.db.QueryRow(checkQuery, task.Table.Table).Scan(&tableCount, &tableID)

	// Bảng chưa tồn tại trong DuckLake: bootstrap đăng ký table_id mới
	if err != nil || tableCount == 0 {
		log.Printf("Table %s chưa có trong DuckLake, đăng ký table metadata vào DuckLake...", task.Table)
		var nextTblID int64 = 1
		_ = e.db.QueryRow("SELECT COALESCE(MAX(table_id), 0) + 1 FROM ducklake_catalog.ducklake_table;").Scan(&nextTblID)
		tableID = nextTblID

		tableUUID := "00000000-0000-0000-0000-000000000000"
		if meta != nil && meta.TableUUID != "" {
			tableUUID = meta.TableUUID
		}

		insertTableSQL := `
			INSERT INTO ducklake_catalog.ducklake_table 
			(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)
			VALUES ($1, $2, $3, NULL, 0, $4, $5, true);
		`
		tablePath := fmt.Sprintf("%s/", task.Table.Table)
		if _, err := e.db.Exec(insertTableSQL, tableID, tableUUID, snapshotID, task.Table.Table, tablePath); err != nil {
			log.Printf("Warning: failed to insert ducklake_table: %v", err)
		} else {
			log.Printf("Đăng ký thành công table %s (table_id: %d) vào DuckLake PostgreSQL catalog", task.Table, tableID)
		}
	}

	// 4. Ghi nhận snapshot mới vào ducklake_catalog.ducklake_snapshot
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	var snapExists int
	_ = e.db.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_snapshot WHERE snapshot_id = $1;", snapshotID).Scan(&snapExists)
	if snapExists == 0 {
		snapshotSQL := `
			INSERT INTO ducklake_catalog.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
			VALUES ($1, $2, 1, 100, 1000);
		`
		if _, err := e.db.Exec(snapshotSQL, snapshotID, now); err != nil {
			return fmt.Errorf("failed to insert snapshot into ducklake: %w", err)
		}
	}

	// 5. Trích xuất file Parquet THẬT từ metadata / Avro manifest list & manifests
	var dataFiles []ManifestDataFile

	if targetSnap != nil && targetSnap.ManifestList != "" && e.store != nil {
		mFiles, err := ExtractDataFilesFromStorage(context.Background(), e.store, targetSnap.ManifestList, snapshotID)
		if err != nil {
			log.Printf("Warning: error reading Avro manifest files: %v", err)
		} else {
			dataFiles = append(dataFiles, mFiles...)
		}
	}

	if len(dataFiles) == 0 && targetSnap != nil && len(targetSnap.DataFiles) > 0 {
		for _, f := range targetSnap.DataFiles {
			dataFiles = append(dataFiles, ManifestDataFile{
				Path:            f.Path,
				FileSizeInBytes: f.FileSizeInBytes,
				RecordCount:     f.RecordCount,
				Content:         f.Content,
				ReferencedPath:  f.ReferencedPath,
			})
		}
	}

	var nextFileID int64 = 1
	_ = e.db.QueryRow("SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_catalog.ducklake_data_file;").Scan(&nextFileID)

	var nextDeleteFileID int64 = 1
	_ = e.db.QueryRow("SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_catalog.ducklake_delete_file;").Scan(&nextDeleteFileID)

	for _, df := range dataFiles {
		if df.Content == 0 { // 0 = Data file
			dataFileSQL := `
				INSERT INTO ducklake_catalog.ducklake_data_file 
				(data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, partition_id)
				VALUES ($1, $2, $3, NULL, 0, $4, false, 'parquet', $5, $6, 0);
			`
			if _, err := e.db.Exec(dataFileSQL, nextFileID, tableID, snapshotID, df.Path, df.RecordCount, df.FileSizeInBytes); err != nil {
				return fmt.Errorf("failed to insert data file %s into ducklake: %w", df.Path, err)
			}
			nextFileID++
		} else if df.Content == 1 { // 1 = Delete v2 (Position Deletes)
			var refDataFileID int64 = 1
			if df.ReferencedPath != "" {
				_ = e.db.QueryRow("SELECT data_file_id FROM ducklake_catalog.ducklake_data_file WHERE path = $1 LIMIT 1;", df.ReferencedPath).Scan(&refDataFileID)
			} else {
				_ = e.db.QueryRow("SELECT COALESCE(MAX(data_file_id), 1) FROM ducklake_catalog.ducklake_data_file WHERE table_id = $1;", tableID).Scan(&refDataFileID)
			}

			deleteFileSQL := `
				INSERT INTO ducklake_catalog.ducklake_delete_file 
				(delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes)
				VALUES ($1, $2, $3, NULL, $4, $5, false, 'parquet', $6, $7);
			`
			if _, err := e.db.Exec(deleteFileSQL, nextDeleteFileID, tableID, snapshotID, refDataFileID, df.Path, df.RecordCount, df.FileSizeInBytes); err != nil {
				return fmt.Errorf("failed to insert delete file %s into ducklake: %w", df.Path, err)
			}
			nextDeleteFileID++
		}
	}

	// 6. Xử lý Schema Evolution động: đối chiếu danh sách cột và cập nhật begin/end_snapshot
	if meta != nil && len(meta.Schemas) > 0 {
		var activeSchema *SchemaEntry
		for i := range meta.Schemas {
			if meta.Schemas[i].SchemaID == meta.CurrentSchemaID {
				activeSchema = &meta.Schemas[i]
				break
			}
		}
		if activeSchema == nil {
			activeSchema = &meta.Schemas[len(meta.Schemas)-1]
		}

		// Truy vấn danh sách cột đang active trong DuckLake
		activeCols := make(map[int]bool)
		rows, err := e.db.Query("SELECT column_id FROM ducklake_catalog.ducklake_column WHERE table_id = $1 AND end_snapshot IS NULL;", tableID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var cid int
				if err := rows.Scan(&cid); err == nil {
					activeCols[cid] = true
				}
			}
		}

		newCols := make(map[int]bool)
		for orderIdx, f := range activeSchema.Fields {
			newCols[f.ID] = true
			if !activeCols[f.ID] {
				// Chuẩn hóa kiểu dữ liệu từ Iceberg sang DuckLake / DuckDB type
				duckType := strings.ToLower(f.Type)
				switch duckType {
				case "long":
					duckType = "int64"
				case "string":
					duckType = "varchar"
				case "double":
					duckType = "float64"
				case "int", "integer":
					duckType = "int32"
				case "float":
					duckType = "float32"
				case "boolean":
					duckType = "boolean"
				}

				columnSQL := `
					INSERT INTO ducklake_catalog.ducklake_column 
					(column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, default_value, nulls_allowed, default_value_type, default_value_dialect)
					VALUES ($1, $2, NULL, $3, $4, $5, $6, 'NULL', true, 'literal', 'duckdb');
				`
				if _, err := e.db.Exec(columnSQL, f.ID, snapshotID, tableID, orderIdx+1, f.Name, duckType); err != nil {
					return fmt.Errorf("failed to insert column %s into ducklake: %w", f.Name, err)
				}
			}
		}

		// Đánh dấu end_snapshot cho các cột đã bị xóa
		for cid := range activeCols {
			if !newCols[cid] {
				dropSQL := `
					UPDATE ducklake_catalog.ducklake_column 
					SET end_snapshot = $1 
					WHERE table_id = $2 AND column_id = $3 AND end_snapshot IS NULL;
				`
				if _, err := e.db.Exec(dropSQL, snapshotID, tableID, cid); err != nil {
					return fmt.Errorf("failed to update dropped column %d end_snapshot: %w", cid, err)
				}
			}
		}
	}

	// 6. Ghi nhận sự kiện delta thật vào ducklake_catalog.ducklake_snapshot_changes (5 cột)
	changeSQL := `
		INSERT INTO ducklake_catalog.ducklake_snapshot_changes 
		VALUES ($1, $2, NULL, NULL, NULL);
	`
	if _, err := e.db.Exec(changeSQL, snapshotID, operation); err != nil {
		return fmt.Errorf("failed to insert snapshot change into ducklake: %w", err)
	}

	log.Printf("Hoàn tất delta sync thật cho %s (snapshot: %d, files: %d)", task.Table, snapshotID, len(dataFiles))
	return nil
}

// ParseMetadataJSON parses Iceberg metadata JSON bytes
func ParseMetadataJSON(data []byte) (*IcebergMetadata, error) {
	var meta IcebergMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// ParseMetadataFile reads and parses a local Iceberg metadata file
func ParseMetadataFile(filePath string) (*IcebergMetadata, error) {
	if strings.HasPrefix(filePath, "s3://") {
		return nil, fmt.Errorf("use storage client for S3 uri: %s", filePath)
	}
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return ParseMetadataJSON(bytes)
}
