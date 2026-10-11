package main

import (
	"context"
	"log"
	"os"

	"bifrost-proxy/api"
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
)

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	ctx := context.Background()
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// 1. MinIO / S3 Storage setup
	endpoint := getEnv("S3_ENDPOINT", "localhost:9000")
	accessKey := getEnv("AWS_ACCESS_KEY_ID", "admin")
	secretKey := getEnv("AWS_SECRET_ACCESS_KEY", "password")
	bucket := getEnv("S3_BUCKET", "lakehouse")

	store, err := storage.NewMinioStorage(endpoint, accessKey, secretKey, bucket)
	if err != nil {
		log.Printf("Warning: Failed to init MinIO storage: %v", err)
	}

	// 2. PostgreSQL Catalog Pointer Store (Sync-01)
	pgConn := getEnv("PG_CONN_STRING", "postgres://admin:password@postgres:5432/polaris?sslmode=disable")
	catalogStore, err := storage.NewPostgresCatalogStore(ctx, pgConn)
	if err != nil {
		log.Printf("Warning: Failed to init PostgreSQL Catalog store: %v", err)
	} else {
		defer catalogStore.Close()
	}

	// 3. DuckDB Sync Engine setup
	syncEngine, err := engine.NewSyncEngine()
	if err != nil {
		log.Printf("Warning: Failed to init DuckDB engine: %v", err)
	} else {
		defer syncEngine.Close()
	}

	// 4. Mount Routes
	api.SetupAuthRoutes(app)
	api.SetupCatalogRoutes(app, store, catalogStore, syncEngine)

	log.Println("Bifrost Proxy is starting on :8080...")
	log.Fatal(app.Listen(":8080"))
}
