package main

import (
	"context"
	"log"

	"bifrost-proxy/api"
	"bifrost-proxy/config"
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
)

func main() {
	ctx := context.Background()
	cfg := config.LoadConfig()

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// 1. MinIO / S3 Storage
	store, err := storage.NewMinioStorage(cfg.S3Endpoint, cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.S3Bucket)
	if err != nil {
		log.Fatalf("Failed to init MinIO storage: %v", err)
	}

	// 2. PostgreSQL Catalog Pointer Store
	catalogStore, err := storage.NewPostgresCatalogStore(ctx, cfg.PGConnString)
	if err != nil {
		log.Fatalf("Failed to init PostgreSQL Catalog store: %v", err)
	}
	defer catalogStore.Close()

	// 3. DuckDB Sync Engine & Ingestion Queue
	syncEngine, err := engine.NewSyncEngine(cfg, store)
	if err != nil {
		log.Fatalf("Fatal: failed to initialize DuckDB sync engine: %v", err)
	}
	defer syncEngine.Close()

	queue := engine.NewIngestionQueue(syncEngine, 50000)
	defer queue.Stop()

	// 4. Mount Routes
	api.SetupAuthRoutes(app)
	api.SetupCatalogRoutes(app, store, catalogStore, queue)

	log.Println("Bifrost Proxy is starting on :8080...")
	log.Fatal(app.Listen(":8080"))
}
