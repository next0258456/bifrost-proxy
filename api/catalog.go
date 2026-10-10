package api

import (
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
)

func SetupCatalogRoutes(app *fiber.App, store storage.StorageService, syncEngine *engine.SyncEngine) {
	// Ticket 02: Catalog Read API
	app.Get("/v1/namespaces/:namespace/tables/:table", func(c *fiber.Ctx) error {
		ns := c.Params("namespace")
		table := c.Params("table")

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

	// Ticket 03: Ingestion Sync Pipeline
	app.Post("/v1/namespaces/:namespace/tables/:table", func(c *fiber.Ctx) error {
		// Nhận payload commit từ Redpanda (Iceberg Sink)
		// Yêu cầu DuckDB đồng bộ metadata này sang DuckLake
		if syncEngine != nil {
			go syncEngine.SyncToDuckLake(c.Body()) // Chạy async để đảm bảo SLA response < 15ms
		}
		
		// Giả lập trả về cho client Redpanda rằng commit đã thành công
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"metadata-location": "s3://bifrost/metadata/new.json",
			"table-uuid": "uuid-123",
		})
	})
}
