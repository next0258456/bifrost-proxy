package main

import (
	"log"
	"os"

	"bifrost-proxy/api"
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New(fiber.Config{
		// Optimize for SLA
		DisableStartupMessage: true,
	})

	// 1. Storage setup
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	}
	
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = "admin"
	}
	
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = "password"
	}

	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		bucket = "lakehouse"
	}

	store, err := storage.NewMinioStorage(endpoint, accessKey, secretKey, bucket)
	if err != nil {
		log.Printf("Warning: Failed to init real S3 storage: %v", err)
	}

	// 2. DuckDB Engine setup
	syncEngine, err := engine.NewSyncEngine()
	if err != nil {
		log.Printf("Warning: Failed to init DuckDB engine: %v", err)
	} else {
		defer syncEngine.Close()
	}

	// 3. Mount Routes
	api.SetupAuthRoutes(app)
	api.SetupCatalogRoutes(app, store, syncEngine)

	log.Println("Bifrost Proxy is starting on :8080...")
	log.Fatal(app.Listen(":8080"))
}
