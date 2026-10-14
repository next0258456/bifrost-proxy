package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
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
	SchemaID     *int            `json:"schema-id,omitempty"`
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

// MapIcebergTypeToDuckLake converts standard Iceberg schema data types to DuckLake SQL types
func MapIcebergTypeToDuckLake(icebergType string) string {
	raw := strings.ToLower(strings.TrimSpace(icebergType))
	if raw == "" {
		return "varchar"
	}

	// 1. Timezone-aware timestamps
	if raw == "timestamptz" || strings.Contains(raw, "with time zone") || strings.HasPrefix(raw, "timestamp_tz") {
		return "timestamptz"
	}

	// 2. Complex & Nested Types: list<T> or array<T>
	if (strings.HasPrefix(raw, "list<") || strings.HasPrefix(raw, "array<")) && strings.HasSuffix(raw, ">") {
		prefixLen := 5
		if strings.HasPrefix(raw, "array<") {
			prefixLen = 6
		}
		inner := strings.TrimSpace(raw[prefixLen : len(raw)-1])
		return MapIcebergTypeToDuckLake(inner) + "[]"
	}

	// 3. Complex Types: map<K, V>
	if strings.HasPrefix(raw, "map<") && strings.HasSuffix(raw, ">") {
		inner := strings.TrimSpace(raw[4 : len(raw)-1])
		parts := splitTopLevelComma(inner)
		if len(parts) == 2 {
			keyType := MapIcebergTypeToDuckLake(parts[0])
			valType := MapIcebergTypeToDuckLake(parts[1])
			return fmt.Sprintf("MAP(%s, %s)", keyType, valType)
		}
	}

	// 4. Complex Types: struct<f1: t1, f2: t2>
	if strings.HasPrefix(raw, "struct<") && strings.HasSuffix(raw, ">") {
		inner := strings.TrimSpace(raw[7 : len(raw)-1])
		parts := splitTopLevelComma(inner)
		var structFields []string
		for _, part := range parts {
			if name, typ, ok := parseStructField(part); ok {
				duckType := MapIcebergTypeToDuckLake(typ)
				structFields = append(structFields, fmt.Sprintf("%s %s", name, duckType))
			} else if part != "" {
				structFields = append(structFields, part)
			}
		}
		return fmt.Sprintf("STRUCT(%s)", strings.Join(structFields, ", "))
	}

	// 5. Primitive Types
	switch {
	case raw == "long" || raw == "bigint":
		return "int64"
	case raw == "int" || raw == "integer":
		return "int32"
	case raw == "short":
		return "int16"
	case raw == "string" || raw == "varchar":
		return "varchar"
	case raw == "double":
		return "float64"
	case raw == "float":
		return "float32"
	case raw == "boolean":
		return "boolean"
	case raw == "date":
		return "date"
	case raw == "time":
		return "time"
	case strings.HasPrefix(raw, "timestamp"):
		return "timestamp"
	case raw == "uuid":
		return "uuid"
	case raw == "binary" || strings.HasPrefix(raw, "fixed"):
		return "blob"
	case strings.HasPrefix(raw, "decimal"):
		return raw
	case raw == "json":
		return "json"
	default:
		log.Printf("Warning: unmapped Iceberg type %q, safely falling back to 'varchar'", raw)
		return "varchar"
	}
}

// parseStructField extracts the field name and field type from a struct field definition,
// supporting either ':' (e.g. "f1: int") or ' ' (e.g. "f1 int") delimiters.
func parseStructField(part string) (string, string, bool) {
	part = strings.TrimSpace(part)
	delimiterIdx := strings.IndexAny(part, ": ")
	if delimiterIdx > 0 {
		fieldName := strings.TrimSpace(part[:delimiterIdx])
		fieldType := strings.TrimSpace(part[delimiterIdx+1:])
		return fieldName, fieldType, true
	}
	return part, "", false
}

// splitTopLevelComma splits a string on commas that are not nested within <>, (), or []
func splitTopLevelComma(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, r := range s {
		switch r {
		case '<', '(', '[':
			depth++
		case '>', ')', ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(s) {
		parts = append(parts, strings.TrimSpace(s[start:]))
	}
	return parts
}

// ptrToAny converts a typed pointer to an untyped interface{} value (or untyped nil if pointer is nil)
func ptrToAny[T any](p *T) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

// SnapshotCounters tracks monotonic catalog, file, and schema version counters in ducklake_snapshot.
type SnapshotCounters struct {
	SchemaVersion int64
	NextCatalogID int64
	NextFileID    int64
}

// allocateNextID generates the next incremental identifier for a catalog table within a transaction.
func allocateNextID(tx *sql.Tx, table, idCol string) (int64, error) {
	var nextID int64 = 1
	query := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) + 1 FROM ducklake_catalog.%s;", idCol, table)
	if err := tx.QueryRow(query).Scan(&nextID); err != nil {
		return 0, fmt.Errorf("failed to allocate next %s for %s: %w", idCol, table, err)
	}
	return nextID, nil
}

// resolveSchemaForSnapshot finds the originating schema corresponding to a specific snapshot.
func resolveSchemaForSnapshot(meta *IcebergMetadata, snap *SnapshotEntry) *SchemaEntry {
	if meta == nil || len(meta.Schemas) == 0 {
		return nil
	}

	// 1. Explicit schema-id on snapshot
	if snap != nil && snap.SchemaID != nil {
		targetID := *snap.SchemaID
		for i := range meta.Schemas {
			if meta.Schemas[i].SchemaID == targetID {
				return &meta.Schemas[i]
			}
		}
	}

	// 2. If target snap is the current snapshot, use CurrentSchemaID
	if snap != nil && snap.SnapshotID != 0 && snap.SnapshotID == meta.CurrentSnapshotID {
		for i := range meta.Schemas {
			if meta.Schemas[i].SchemaID == meta.CurrentSchemaID {
				return &meta.Schemas[i]
			}
		}
	}

	// 3. Temporal resolution using snap.TimestampMS:
	// Search meta.Snapshots for the closest snapshot with a known SchemaID at or before snap.TimestampMS
	if snap != nil && snap.TimestampMS > 0 && len(meta.Snapshots) > 0 {
		var closestSchemaID *int
		var closestTimestamp int64 = -1

		for _, s := range meta.Snapshots {
			if s.SchemaID != nil && s.TimestampMS <= snap.TimestampMS {
				if s.TimestampMS >= closestTimestamp {
					closestTimestamp = s.TimestampMS
					id := *s.SchemaID
					closestSchemaID = &id
				}
			}
		}

		if closestSchemaID != nil {
			for i := range meta.Schemas {
				if meta.Schemas[i].SchemaID == *closestSchemaID {
					return &meta.Schemas[i]
				}
			}
		}
	}

	// 4. If only 1 schema exists, return it
	if len(meta.Schemas) == 1 {
		return &meta.Schemas[0]
	}

	// 5. If target snap is historical, resolve to the earliest schema (e.g. SchemaID 0 or first schema)
	if snap != nil && snap.SnapshotID != 0 && snap.SnapshotID != meta.CurrentSnapshotID {
		for i := range meta.Schemas {
			if meta.Schemas[i].SchemaID == 0 {
				return &meta.Schemas[i]
			}
		}
		return &meta.Schemas[0]
	}

	// 6. Default fallback to CurrentSchemaID or first schema
	for i := range meta.Schemas {
		if meta.Schemas[i].SchemaID == meta.CurrentSchemaID {
			return &meta.Schemas[i]
		}
	}
	return &meta.Schemas[0]
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
		var localErr error
		meta, localErr = ParseMetadataFile(task.MetadataLocation)
		if localErr != nil {
			log.Printf("Warning: failed to load local metadata file for %s: %v", task.MetadataLocation, localErr)
		}
	}
	if meta == nil {
		return fmt.Errorf("cannot sync table %s: metadata file could not be loaded from %s", task.Table, task.MetadataLocation)
	}

	// 2. Trích xuất đúng Snapshot và Summary delta thật
	snapshotID := task.SnapshotID
	var targetSnap *SnapshotEntry
	targetIdx := -1

	if meta != nil {
		for i := range meta.Snapshots {
			s := &meta.Snapshots[i]
			if snapshotID != 0 && s.SnapshotID == snapshotID {
				targetSnap = s
				targetIdx = i
				break
			}
			if snapshotID == 0 && s.SnapshotID == meta.CurrentSnapshotID {
				targetSnap = s
				snapshotID = s.SnapshotID
				targetIdx = i
				break
			}
		}
		if targetSnap == nil && len(meta.Snapshots) > 0 {
			targetIdx = len(meta.Snapshots) - 1
			targetSnap = &meta.Snapshots[targetIdx]
			if snapshotID == 0 {
				snapshotID = targetSnap.SnapshotID
			}
		}
	}
	if snapshotID == 0 {
		snapshotID = time.Now().UnixMilli()
	}

	// Auto Catch-up Re-sync: Đồng bộ bù tất cả snapshot trong lịch sử trước targetSnap nếu chưa tồn tại trong DuckLake
	if meta != nil && targetIdx > 0 {
		for i := 0; i < targetIdx; i++ {
			prevSnap := &meta.Snapshots[i]
			var snapExists int
			if err := e.db.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_snapshot WHERE snapshot_id = $1;", prevSnap.SnapshotID).Scan(&snapExists); err != nil {
				return fmt.Errorf("failed to check snapshot %d existence during catch-up: %w", prevSnap.SnapshotID, err)
			}
			if snapExists == 0 {
				log.Printf("IngestionQueue: Catching up missing snapshot %d for %s", prevSnap.SnapshotID, task.Table)
				catchUpTask := CommitTask{
					Table:            task.Table,
					MetadataLocation: task.MetadataLocation,
					SnapshotID:       prevSnap.SnapshotID,
				}
				if err := e.syncSnapshotInternal(catchUpTask, meta, prevSnap); err != nil {
					return fmt.Errorf("failed during catch-up sync for snapshot %d: %w", prevSnap.SnapshotID, err)
				}
			}
		}
	}

	return e.syncSnapshotInternal(task, meta, targetSnap)
}

func (e *SyncEngine) syncSnapshotInternal(task CommitTask, meta *IcebergMetadata, targetSnap *SnapshotEntry) error {
	snapshotID := task.SnapshotID
	if targetSnap != nil && targetSnap.SnapshotID != 0 {
		snapshotID = targetSnap.SnapshotID
	}
	if snapshotID == 0 {
		snapshotID = time.Now().UnixMilli()
	}

	operation := "append"
	if targetSnap != nil && targetSnap.Summary.Operation != "" {
		operation = targetSnap.Summary.Operation
	}

	// 3. Mở database transaction nguyên khối cho DuckLake catalog
	ctx := context.Background()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin ducklake sync transaction: %w", err)
	}
	defer tx.Rollback()

	// Kiểm tra xem bảng đã tồn tại trong DuckLake chưa (theo namespace schema)
	schemaID := int64(0)
	schemaName := task.Table.Namespace
	if schemaName == "" {
		schemaName = "main"
	}

	// Đảm bảo ducklake_schema đã tồn tại cho namespace
	var schemaCount int
	if err := tx.QueryRow("SELECT count(*), COALESCE(MAX(schema_id), 0) FROM ducklake_catalog.ducklake_schema WHERE schema_name = $1;", schemaName).Scan(&schemaCount, &schemaID); err != nil {
		return fmt.Errorf("failed to query ducklake_schema for namespace %s: %w", schemaName, err)
	}
	if schemaCount == 0 {
		nextSchemaID, err := allocateNextID(tx, "ducklake_schema", "schema_id")
		if err != nil {
			return err
		}
		schemaID = nextSchemaID
		schemaUUID := uuid.New().String()
		insertSchemaSQL := `
			INSERT INTO ducklake_catalog.ducklake_schema 
			(schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative)
			VALUES ($1, $2, $3, NULL, $4, $5, true);
		`
		schemaPath := fmt.Sprintf("%s/", schemaName)
		if _, err := tx.Exec(insertSchemaSQL, schemaID, schemaUUID, snapshotID, schemaName, schemaPath); err != nil {
			return fmt.Errorf("failed to insert ducklake_schema for %s: %w", schemaName, err)
		}
	}

	var tableCount int
	var tableID int64 = 1
	checkQuery := "SELECT count(*), COALESCE(MAX(table_id), 1) FROM ducklake_catalog.ducklake_table WHERE schema_id = $1 AND table_name = $2;"
	err = tx.QueryRow(checkQuery, schemaID, task.Table.Table).Scan(&tableCount, &tableID)
	if err != nil {
		return fmt.Errorf("failed to query ducklake_table for %s: %w", task.Table, err)
	}

	// Bảng chưa tồn tại trong DuckLake: bootstrap đăng ký table_id mới
	if tableCount == 0 {
		log.Printf("Table %s chưa có trong DuckLake, đăng ký table metadata vào DuckLake...", task.Table)
		nextTblID, err := allocateNextID(tx, "ducklake_table", "table_id")
		if err != nil {
			return err
		}
		tableID = nextTblID

		tableUUID := uuid.New().String()
		if meta != nil && meta.TableUUID != "" {
			tableUUID = meta.TableUUID
		}

		insertTableSQL := `
			INSERT INTO ducklake_catalog.ducklake_table 
			(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)
			VALUES ($1, $2, $3, NULL, $4, $5, $6, true);
		`
		tablePath := fmt.Sprintf("%s/", task.Table.Table)
		if _, err := tx.Exec(insertTableSQL, tableID, tableUUID, snapshotID, schemaID, task.Table.Table, tablePath); err != nil {
			return fmt.Errorf("failed to insert ducklake_table: %w", err)
		} else {
			log.Printf("Đăng ký thành công table %s (table_id: %d, schema_id: %d) vào DuckLake PostgreSQL catalog", task.Table, tableID, schemaID)
		}
	}

	newSchemaCreated := schemaCount == 0
	newTableCreated := tableCount == 0
	tableUUID := uuid.New().String()
	if meta != nil && meta.TableUUID != "" {
		tableUUID = meta.TableUUID
	} else if !newTableCreated {
		if err := tx.QueryRow("SELECT table_uuid FROM ducklake_catalog.ducklake_table WHERE table_id = $1;", tableID).Scan(&tableUUID); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to query table_uuid for table_id %d: %w", tableID, err)
		}
	}

	// Resolve originating historical schema for this snapshot (preserving exact schema version)
	activeSchema := resolveSchemaForSnapshot(meta, targetSnap)

	// 4. Trích xuất file Parquet THẬT từ metadata / Avro manifest list & manifests
	var dataFiles []ManifestDataFile
	var deletedPaths []string

	if targetSnap != nil && targetSnap.ManifestList != "" && e.store != nil {
		var colTypes map[int64]string
		if activeSchema != nil {
			colTypes = make(map[int64]string)
			for _, f := range activeSchema.Fields {
				colTypes[int64(f.ID)] = f.Type
			}
		}
		extractedDataFiles, extractedDeletedPaths, err := ExtractDataFilesFromStorageWithColTypes(context.Background(), e.store, targetSnap.ManifestList, snapshotID, colTypes)
		if err != nil {
			return fmt.Errorf("failed to extract manifest files from storage for %s: %w", targetSnap.ManifestList, err)
		}
		dataFiles = append(dataFiles, extractedDataFiles...)
		deletedPaths = append(deletedPaths, extractedDeletedPaths...)
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

	// Đóng end_snapshot cho các file dữ liệu đã bị xóa (Compaction / Overwrite)
	for _, delPath := range deletedPaths {
		closeDataFileSQL := `
			UPDATE ducklake_catalog.ducklake_data_file 
			SET end_snapshot = $1 
			WHERE table_id = $2 AND path = $3 AND end_snapshot IS NULL;
		`
		if _, err := tx.Exec(closeDataFileSQL, snapshotID, tableID, delPath); err != nil {
			return fmt.Errorf("failed to close end_snapshot for deleted data file %s: %w", delPath, err)
		}
		closeDeleteFileSQL := `
			UPDATE ducklake_catalog.ducklake_delete_file 
			SET end_snapshot = $1 
			WHERE table_id = $2 AND path = $3 AND end_snapshot IS NULL;
		`
		if _, err := tx.Exec(closeDeleteFileSQL, snapshotID, tableID, delPath); err != nil {
			return fmt.Errorf("failed to close end_snapshot for deleted delete file %s: %w", delPath, err)
		}
	}

	nextFileID, err := allocateNextID(tx, "ducklake_data_file", "data_file_id")
	if err != nil {
		return err
	}

	nextDeleteFileID, err := allocateNextID(tx, "ducklake_delete_file", "delete_file_id")
	if err != nil {
		return err
	}

	colStatSQL := `
		INSERT INTO ducklake_catalog.ducklake_file_column_stats 
		(data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL);
	`

	for _, df := range dataFiles {
		if df.Content == 0 { // 0 = Data file
			dataFileSQL := `
				INSERT INTO ducklake_catalog.ducklake_data_file 
				(data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, partition_id)
				VALUES ($1, $2, $3, NULL, 0, $4, false, 'parquet', $5, $6, 0);
			`
			if _, err := tx.Exec(dataFileSQL, nextFileID, tableID, snapshotID, df.Path, df.RecordCount, df.FileSizeInBytes); err != nil {
				return fmt.Errorf("failed to insert data file %s into ducklake: %w", df.Path, err)
			}

			// Ingest column bounds & stats into ducklake_file_column_stats when present in manifest
			if len(df.ColumnStats) > 0 {
				for _, cs := range df.ColumnStats {
					if _, err := tx.Exec(colStatSQL, nextFileID, tableID, cs.ColumnID, ptrToAny(cs.ColumnSizeBytes), ptrToAny(cs.ValueCount), ptrToAny(cs.NullCount), ptrToAny(cs.MinValue), ptrToAny(cs.MaxValue), ptrToAny(cs.ContainsNaN)); err != nil {
						return fmt.Errorf("failed to insert file column stats for data file %d (col %d): %w", nextFileID, cs.ColumnID, err)
					}
				}
			} else if activeSchema != nil && len(activeSchema.Fields) > 0 {
				for _, f := range activeSchema.Fields {
					var defaultNullCount int64 = 0
					var defaultContainsNaN bool = false
					if _, err := tx.Exec(colStatSQL, nextFileID, tableID, f.ID, nil, df.RecordCount, defaultNullCount, nil, nil, defaultContainsNaN); err != nil {
						return fmt.Errorf("failed to insert default file column stats for data file %d (col %d): %w", nextFileID, f.ID, err)
					}
				}
			}

			nextFileID++
		} else if df.Content == 1 { // 1 = Delete v2 (Position Deletes)
			if df.ReferencedPath == "" {
				return fmt.Errorf("position delete file %s has no referenced data file path", df.Path)
			}
			var refDataFileID int64
			err := tx.QueryRow("SELECT data_file_id FROM ducklake_catalog.ducklake_data_file WHERE path = $1 AND table_id = $2 AND end_snapshot IS NULL LIMIT 1;", df.ReferencedPath, tableID).Scan(&refDataFileID)
			if err != nil {
				return fmt.Errorf("failed to resolve active referenced data file ID for position delete %s in table %d (path: %s): %w", df.Path, tableID, df.ReferencedPath, err)
			}

			deleteFileSQL := `
				INSERT INTO ducklake_catalog.ducklake_delete_file 
				(delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes)
				VALUES ($1, $2, $3, NULL, $4, $5, false, 'parquet', $6, $7);
			`
			if _, err := tx.Exec(deleteFileSQL, nextDeleteFileID, tableID, snapshotID, refDataFileID, df.Path, df.RecordCount, df.FileSizeInBytes); err != nil {
				return fmt.Errorf("failed to insert delete file %s into ducklake: %w", df.Path, err)
			}
			nextDeleteFileID++
		}
	}

	// 5. Xử lý Schema Evolution động: đối chiếu danh sách cột và cập nhật begin/end_snapshot
	columnsAltered := false
	if activeSchema != nil {
		// Truy vấn danh sách cột đang active trong DuckLake
		activeCols := make(map[int]bool)
		rows, err := tx.Query("SELECT column_id FROM ducklake_catalog.ducklake_column WHERE table_id = $1 AND end_snapshot IS NULL;", tableID)
		if err != nil {
			return fmt.Errorf("failed to query active columns for table %d: %w", tableID, err)
		}
		defer rows.Close()
		for rows.Next() {
			var cid int
			if err := rows.Scan(&cid); err != nil {
				return fmt.Errorf("failed to scan active column for table %d: %w", tableID, err)
			}
			activeCols[cid] = true
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating active columns for table %d: %w", tableID, err)
		}

		newCols := make(map[int]bool)
		for orderIdx, f := range activeSchema.Fields {
			newCols[f.ID] = true
			if !activeCols[f.ID] {
				columnsAltered = true
				duckType := MapIcebergTypeToDuckLake(f.Type)

				columnSQL := `
					INSERT INTO ducklake_catalog.ducklake_column 
					(column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, default_value, nulls_allowed, default_value_type, default_value_dialect)
					VALUES ($1, $2, NULL, $3, $4, $5, $6, 'NULL', true, 'literal', 'duckdb');
				`
				if _, err := tx.Exec(columnSQL, f.ID, snapshotID, tableID, orderIdx+1, f.Name, duckType); err != nil {
					return fmt.Errorf("failed to insert column %s into ducklake: %w", f.Name, err)
				}
			}
		}

		// Đánh dấu end_snapshot cho các cột đã bị xóa
		for cid := range activeCols {
			if !newCols[cid] {
				columnsAltered = true
				dropSQL := `
					UPDATE ducklake_catalog.ducklake_column 
					SET end_snapshot = $1 
					WHERE table_id = $2 AND column_id = $3 AND end_snapshot IS NULL;
				`
				if _, err := tx.Exec(dropSQL, snapshotID, tableID, cid); err != nil {
					return fmt.Errorf("failed to update dropped column %d end_snapshot: %w", cid, err)
				}
			}
		}
	}

	// 6. Ghi nhận snapshot mới vào ducklake_catalog.ducklake_snapshot với bộ đếm tăng tiến đơn điệu (Invariants)
	var prevCounters SnapshotCounters
	err = tx.QueryRow(`
		SELECT COALESCE(MAX(schema_version), 0), COALESCE(MAX(next_catalog_id), 0), COALESCE(MAX(next_file_id), 0)
		FROM ducklake_catalog.ducklake_snapshot;
	`).Scan(&prevCounters.SchemaVersion, &prevCounters.NextCatalogID, &prevCounters.NextFileID)
	if err != nil {
		return fmt.Errorf("failed to query previous snapshot counters: %w", err)
	}

	catalogAdditions := int64(0)
	if newSchemaCreated {
		catalogAdditions++
	}
	if newTableCreated {
		catalogAdditions++
	}

	var schemaVersion int64
	if prevCounters.SchemaVersion == 0 {
		schemaVersion = 1
	} else {
		schemaVersion = prevCounters.SchemaVersion
		if newSchemaCreated || newTableCreated || columnsAltered {
			schemaVersion++
		}
	}

	var nextCatalogID int64
	if prevCounters.NextCatalogID == 0 {
		nextCatalogID = 1 + catalogAdditions
	} else {
		nextCatalogID = prevCounters.NextCatalogID + catalogAdditions
	}

	var nextFileIDCounter int64
	if prevCounters.NextFileID == 0 {
		nextFileIDCounter = 1 + int64(len(dataFiles))
	} else {
		nextFileIDCounter = prevCounters.NextFileID + int64(len(dataFiles))
	}

	// Đảm bảo invariant đơn điệu chặt chẽ: next_catalog_id > max(catalog_entities), next_file_id > max(file_entities)
	var maxCatalogID, maxFileID int64
	if err := tx.QueryRow(`
		SELECT GREATEST(
			COALESCE((SELECT MAX(schema_id) FROM ducklake_catalog.ducklake_schema), 0),
			COALESCE((SELECT MAX(table_id) FROM ducklake_catalog.ducklake_table), 0)
		);
	`).Scan(&maxCatalogID); err != nil {
		return fmt.Errorf("failed to query max catalog entity id for invariants: %w", err)
	}
	if nextCatalogID <= maxCatalogID {
		nextCatalogID = maxCatalogID + 1
	}

	if err := tx.QueryRow(`
		SELECT GREATEST(
			COALESCE((SELECT MAX(data_file_id) FROM ducklake_catalog.ducklake_data_file), 0),
			COALESCE((SELECT MAX(delete_file_id) FROM ducklake_catalog.ducklake_delete_file), 0)
		);
	`).Scan(&maxFileID); err != nil {
		return fmt.Errorf("failed to query max file entity id for invariants: %w", err)
	}
	if nextFileIDCounter <= maxFileID {
		nextFileIDCounter = maxFileID + 1
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	var snapExists int
	if err := tx.QueryRow("SELECT count(*) FROM ducklake_catalog.ducklake_snapshot WHERE snapshot_id = $1;", snapshotID).Scan(&snapExists); err != nil {
		return fmt.Errorf("failed to check ducklake_snapshot existence for %d: %w", snapshotID, err)
	}
	if snapExists == 0 {
		snapshotSQL := `
			INSERT INTO ducklake_catalog.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
			VALUES ($1, $2, $3, $4, $5);
		`
		if _, err := tx.Exec(snapshotSQL, snapshotID, now, schemaVersion, nextCatalogID, nextFileIDCounter); err != nil {
			return fmt.Errorf("failed to insert snapshot into ducklake: %w", err)
		}

		// 7. Ghi nhận sự kiện delta chuẩn hóa tokens vào ducklake_catalog.ducklake_snapshot_changes
		hasDataFiles := false
		hasDeleteFiles := false
		for _, df := range dataFiles {
			if df.Content == 0 {
				hasDataFiles = true
			} else {
				hasDeleteFiles = true
			}
		}

		var changes []string
		if newSchemaCreated {
			changes = append(changes, fmt.Sprintf("created_schema:\"%s\"", schemaName))
		}
		if newTableCreated {
			changes = append(changes, fmt.Sprintf("created_table:\"%s\"", tableUUID))
		}
		if hasDataFiles {
			changes = append(changes, fmt.Sprintf("inserted_into_table:%d", tableID))
		}
		if len(deletedPaths) > 0 || hasDeleteFiles {
			changes = append(changes, fmt.Sprintf("deleted_from_table:%d", tableID))
		}
		if columnsAltered && !newTableCreated {
			changes = append(changes, fmt.Sprintf("altered_table:%d", tableID))
		}
		if len(changes) == 0 {
			changes = append(changes, operation)
		}
		changesMade := strings.Join(changes, ",")

		commitMsg := fmt.Sprintf("Commit snapshot %d (%s) for table %s", snapshotID, operation, task.Table)
		changeSQL := `
			INSERT INTO ducklake_catalog.ducklake_snapshot_changes 
			(snapshot_id, changes_made, author, commit_message, commit_extra_info)
			VALUES ($1, $2, 'bifrost-proxy', $3, NULL);
		`
		if _, err := tx.Exec(changeSQL, snapshotID, changesMade, commitMsg); err != nil {
			return fmt.Errorf("failed to insert snapshot change into ducklake: %w", err)
		}
	}

	// 7. Commit toàn bộ transaction nguyên khối
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit ducklake sync transaction: %w", err)
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
		return nil, fmt.Errorf("failed to read local metadata file %s: %w", filePath, err)
	}
	meta, err := ParseMetadataJSON(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON from %s: %w", filePath, err)
	}
	return meta, nil
}
