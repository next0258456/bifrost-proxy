package api

import (
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
)

func SetupCatalogRoutes(app *fiber.App, store storage.StorageService, catalogStore storage.CatalogStore, syncEngine *engine.SyncEngine) {
	// Ticket 02 & Sync-01: Catalog Read API
	app.Get("/v1/namespaces/:namespace/tables/:table", func(c *fiber.Ctx) error {
		ns := c.Params("namespace")
		table := c.Params("table")

		// 1. Kiểm tra con trỏ bảng trong PostgreSQL trước (nếu có catalogStore)
		if catalogStore != nil {
			_, err := catalogStore.GetTablePointer(c.Context(), ns, table)
			if err != nil && err == storage.ErrTableNotFound {
				return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
					"error": fiber.Map{
						"message": "Table does not exist",
						"type":    "NoSuchTableException",
						"code":    404,
					},
				})
			}
		}

		// 2. Lấy nội dung metadata JSON từ S3 MinIO
		data, err := store.GetTableMetadata(c.Context(), ns, table)
		if err != nil {
			if err == storage.ErrTableNotFound {
				return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
					"error": fiber.Map{
						"message": "Table does not exist",
						"type":    "NoSuchTableException",
						"code":    404,
					},
				})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		c.Set("Content-Type", "application/json")
		return c.Send(data)
	})

	// Ticket 03 & Sync-01: Ingestion Commit API
	app.Post("/v1/namespaces/:namespace/tables/:table", func(c *fiber.Ctx) error {
		ns := c.Params("namespace")
		table := c.Params("table")

		var payload map[string]interface{}
		_ = c.BodyParser(&payload)

		metadataPath, ok := payload["metadata-location"].(string)
		if !ok || metadataPath == "" {
			metadataPath = "s3://lakehouse/" + ns + "/" + table + "/metadata/current.json"
			_ = store.SaveTableMetadata(c.Context(), ns, table, c.Body())
		}

		// Bước 1: GHI ĐỒNG BỘ VÀO POSTGRES (< 2ms)
		if catalogStore != nil {
			_ = catalogStore.SaveTablePointer(c.Context(), ns, table, metadataPath, "")
		}

		// Bước 2: KÍCH HOẠT ĐỒNG BỘ DUCKLAKE BẤT ĐỒNG BỘ (Background)
		if syncEngine != nil {
			go syncEngine.SyncToDuckLake(metadataPath)
		}
		
		// Bước 3: TRẢ VỀ 200 OK NGAY CHO REDPANDA (< 15ms SLA)
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"metadata-location": metadataPath,
			"table-uuid":        "uuid-123",
		})
	})
}
