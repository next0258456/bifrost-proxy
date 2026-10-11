package engine

import (
	"database/sql"
	"fmt"
	"log"

	"bifrost-proxy/config"
	"bifrost-proxy/storage"

	_ "github.com/marcboeker/go-duckdb"
)

type SyncEngine struct {
	db    *sql.DB
	store storage.StorageService
}

// NewSyncEngine initializes the embedded DuckDB connection
func NewSyncEngine(cfg *config.Config, store storage.StorageService) (*SyncEngine, error) {
	db, err := sql.Open("duckdb", "?access_mode=read_write")
	if err != nil {
		return nil, err
	}

	// Cấu hình tài nguyên khắt khe (Ticket Sync-02)
	db.Exec("PRAGMA threads = 1;")
	db.Exec("PRAGMA memory_limit = '250MB';")
	db.Exec("PRAGMA preserve_insertion_order = false;")

	// Load extensions cần thiết để parse Avro, Iceberg tree và kết nối PostgreSQL Catalog
	db.Exec("INSTALL httpfs; LOAD httpfs;")
	db.Exec("INSTALL iceberg; LOAD iceberg;")
	db.Exec("INSTALL postgres; LOAD postgres;")
	db.Exec("INSTALL ducklake; LOAD ducklake;")

	// Khởi tạo Secret S3 để đọc file Avro/Parquet và metadata JSON
	secretSQL := fmt.Sprintf(`
		CREATE SECRET IF NOT EXISTS s3_bifrost (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION 'us-east-1',
			ENDPOINT '%s',
			URL_STYLE 'path',
			USE_SSL 'false'
		);
	`, cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.S3Endpoint)
	
	_, err = db.Exec(secretSQL)
	if err != nil {
		log.Printf("Warning: failed to create S3 secret: %v", err)
	}

	// Đính kèm trực tiếp DuckLake Catalog vào PostgreSQL làm single source of truth cho metadata!
	// Cú pháp: ATTACH 'ducklake:postgres:...' AS ducklake_catalog
	pgCatalogSQL := fmt.Sprintf(
		"ATTACH 'dbname=polaris host=postgres port=5432 user=admin password=password' AS ducklake_catalog (TYPE POSTGRES);",
	)
	_, err = db.Exec(pgCatalogSQL)
	if err != nil {
		log.Printf("Warning: failed to attach PostgreSQL as ducklake_catalog: %v", err)
	} else {
		log.Println("DuckDB SyncEngine successfully attached PostgreSQL as ducklake_catalog!")
	}

	return &SyncEngine{db: db, store: store}, nil
}

func (e *SyncEngine) SetStorage(store storage.StorageService) {
	e.store = store
}

// NewTestSyncEngine creates a SyncEngine wrapping an existing database connection for unit testing.
func NewTestSyncEngine(db *sql.DB, store storage.StorageService) *SyncEngine {
	return &SyncEngine{db: db, store: store}
}

func (e *SyncEngine) Close() {
	if e.db != nil {
		e.db.Close()
	}
}

// SyncToDuckLake triggers the C++ iceberg_to_ducklake routine.
// Cực kỳ cẩn thận: Hàm này sẽ dùng sức mạnh của DuckDB Iceberg extension để parse toàn bộ
// Manifest List, Manifest (định dạng Avro) và quét các file Parquet để tạo cấu trúc DuckLake tối ưu nhất.
func (e *SyncEngine) SyncToDuckLake(metadataLocation string) error {
	log.Printf("DuckDB is starting deep sync via iceberg_to_ducklake from: %s", metadataLocation)
	
	// Thực thi table function `iceberg_to_ducklake(iceberg_metadata, target_ducklake_catalog)`
	query := fmt.Sprintf(`CALL iceberg_to_ducklake('%s', 'ducklake_catalog');`, metadataLocation)
	_, err := e.db.Exec(query)
	if err != nil {
		log.Printf("Error: SyncToDuckLake failed during Avro mapping: %v", err)
		return err
	}
	
	log.Println("DuckDB successfully synced entire Iceberg Avro tree to DuckLake!")
	return nil
}
