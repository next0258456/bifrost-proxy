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

	// 2. Kiểm tra xem bảng đã tồn tại trong DuckLake chưa
	var tableCount int
	var tableID int64 = 1
	checkQuery := "SELECT count(*), COALESCE(MAX(table_id), 1) FROM ducklake_catalog.ducklake_table WHERE table_name = $1;"
	err := e.db.QueryRow(checkQuery, task.Table.Table).Scan(&tableCount, &tableID)

	// Bảng chưa tồn tại trong DuckLake: bootstrap lần đầu
	if err != nil || tableCount == 0 {
		log.Printf("Table %s chưa có trong DuckLake, bootstrap lần đầu...", task.Table)
		bootstrapQuery := fmt.Sprintf(`CALL iceberg_to_ducklake('%s', 'ducklake_catalog');`, task.MetadataLocation)
		if _, err := e.db.Exec(bootstrapQuery); err != nil {
			log.Printf("Warning: bootstrap iceberg_to_ducklake: %v", err)
			return err
		}
		log.Printf("Bootstrap DuckLake cho %s thành công", task.Table)
		return nil
	}

	// 3. Trích xuất đúng Snapshot và Summary delta thật
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

	// 4. Ghi nhận snapshot mới vào ducklake_catalog.ducklake_snapshot
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	snapshotSQL := fmt.Sprintf(`
		INSERT INTO ducklake_catalog.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
		VALUES (%d, '%s', 1, 100, 1000)
		ON CONFLICT DO NOTHING;
	`, snapshotID, now)
	if _, err := e.db.Exec(snapshotSQL); err != nil {
		log.Printf("Incremental note: snapshot insert: %v", err)
	}

	// 5. Trích xuất file Parquet THẬT từ metadata / manifest
	type RealDataFile struct {
		Path            string
		FileSizeInBytes int64
		RecordCount     int64
		Content         int
	}

	var dataFiles []RealDataFile

	if e.db != nil {
		query := `
			SELECT file_path, file_size_in_bytes, record_count, COALESCE(content, 0)
			FROM iceberg_metadata($1)
			WHERE snapshot_id = $2;
		`
		rows, err := e.db.Query(query, task.MetadataLocation, snapshotID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var f RealDataFile
				if err := rows.Scan(&f.Path, &f.FileSizeInBytes, &f.RecordCount, &f.Content); err == nil {
					dataFiles = append(dataFiles, f)
				}
			}
		}
	}

	if len(dataFiles) == 0 && targetSnap != nil && len(targetSnap.DataFiles) > 0 {
		for _, f := range targetSnap.DataFiles {
			dataFiles = append(dataFiles, RealDataFile{
				Path:            f.Path,
				FileSizeInBytes: f.FileSizeInBytes,
				RecordCount:     f.RecordCount,
				Content:         f.Content,
			})
		}
	}

	var nextFileID int64 = 1
	_ = e.db.QueryRow("SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_catalog.ducklake_data_file;").Scan(&nextFileID)

	for _, df := range dataFiles {
		if df.Content == 0 {
			dataFileSQL := fmt.Sprintf(`
				INSERT INTO ducklake_catalog.ducklake_data_file 
				(data_file_id, table_id, begin_snapshot, end_snapshot, path, record_count, file_size_in_bytes, partition_id)
				VALUES (%d, %d, %d, NULL, '%s', %d, %d, 0);
			`, nextFileID, tableID, snapshotID, df.Path, df.RecordCount, df.FileSizeInBytes)
			if _, err := e.db.Exec(dataFileSQL); err != nil {
				log.Printf("Incremental note: data_file insert error: %v", err)
			}
			nextFileID++
		}
	}

	// 6. Ghi nhận sự kiện delta thật vào ducklake_catalog.ducklake_snapshot_changes
	changeSQL := fmt.Sprintf(`
		INSERT INTO ducklake_catalog.ducklake_snapshot_changes 
		VALUES (%d, '%s', CURRENT_TIMESTAMP);
	`, snapshotID, operation)
	if _, err := e.db.Exec(changeSQL); err != nil {
		log.Printf("Incremental note: snapshot_changes insert error: %v", err)
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
