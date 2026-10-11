package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

// IcebergMetadata represents the essential Iceberg table metadata fields
type IcebergMetadata struct {
	FormatVersion      int    `json:"format-version"`
	TableUUID          string `json:"table-uuid"`
	CurrentSnapshotID  int64  `json:"current-snapshot-id"`
	LastSequenceNumber int64  `json:"last-sequence-number"`
	Schemas            []struct {
		SchemaID int `json:"schema-id"`
		Fields   []struct {
			ID       int    `json:"id"`
			Name     string `json:"name"`
			Required bool   `json:"required"`
			Type     string `json:"type"`
		} `json:"fields"`
	} `json:"schemas"`
	Snapshots []struct {
		SnapshotID   int64  `json:"snapshot-id"`
		TimestampMS  int64  `json:"timestamp-ms"`
		ManifestList string `json:"manifest-list"`
		Summary      struct {
			Operation        string `json:"operation"`
			AddedDataFiles   string `json:"added-data-files"`
			AddedRecords     string `json:"added-records"`
			DeletedDataFiles string `json:"deleted-data-files"`
		} `json:"summary"`
	} `json:"snapshots"`
}

// SyncDeltaIncremental synchronises one committed Iceberg snapshot into DuckLake.
func (e *SyncEngine) SyncDeltaIncremental(task CommitTask) error {
	log.Printf("Starting delta sync for %s (snapshot: %d)", task.Table, task.SnapshotID)

	var tableCount int
	checkQuery := "SELECT count(*) FROM ducklake_catalog.ducklake_table WHERE table_name = $1;"
	err := e.db.QueryRow(checkQuery, task.Table.Table).Scan(&tableCount)

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

	// Incremental delta thật được hiện thực ở Ticket Master-04.
	log.Printf("Table %s đã tồn tại trong DuckLake; chờ delta sync thật (Master-04)", task.Table)
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
