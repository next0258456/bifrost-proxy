package engine

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

type SyncEngine struct {
	db *sql.DB
}

// NewSyncEngine initializes the embedded DuckDB connection
func NewSyncEngine() (*SyncEngine, error) {
	db, err := sql.Open("duckdb", "?access_mode=read_write")
	if err != nil {
		return nil, err
	}

	// Load extensions cần thiết để parse Avro và Iceberg tree
	db.Exec("INSTALL httpfs; LOAD httpfs;")
	db.Exec("INSTALL iceberg; LOAD iceberg;")
	db.Exec("INSTALL ducklake; LOAD ducklake;")

	// Khởi tạo Secret S3 để đọc file Avro/Parquet và metadata JSON
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "minio:9000"
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = "admin"
	}
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = "password"
	}

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
	`, accessKey, secretKey, endpoint)
	
	_, err = db.Exec(secretSQL)
	if err != nil {
		log.Printf("Warning: failed to create S3 secret: %v", err)
	}

	// Tạo schema và attach DuckLake Catalog để xuất file .ducklake
	_, err = db.Exec("ATTACH 's3://lakehouse/bifrost_catalog' AS ducklake_catalog (TYPE DUCKLAKE);")
	if err != nil {
		log.Printf("Warning: failed to attach ducklake catalog: %v", err)
	}

	return &SyncEngine{db: db}, nil
}

func (e *SyncEngine) Close() {
	e.db.Close()
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
