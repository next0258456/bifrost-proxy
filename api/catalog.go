package api

import (
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
)

func SetupCatalogRoutes(app *fiber.App, store storage.StorageService, catalogStore storage.CatalogStore, queue engine.QueueService) {
	// Ticket 02 & Sync-01: Catalog Read API
	app.Get("/v1/namespaces/:namespace/tables/:table", func(c *fiber.Ctx) error {
		ns := c.Params("namespace")
		table := c.Params("table")

		// 1. Kiểm tra con trỏ bảng trong PostgreSQL trước (nếu có catalogStore)
		var metadataPath string
		if catalogStore != nil {
			var err error
			metadataPath, err = catalogStore.GetTablePointer(c.Context(), ns, table)
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
		}

		// 2. Lấy nội dung metadata JSON từ S3 MinIO bằng đúng con trỏ tìm được
		var data []byte
		var err error
		if metadataPath != "" {
			data, err = store.GetMetadataByLocation(c.Context(), metadataPath)
		} else {
			data, err = store.GetTableMetadata(c.Context(), ns, table)
		}

		if err != nil {
			if err == storage.ErrTableNotFound {
				return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
					"error": fiber.Map{
						"message": "Table metadata does not exist",
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

	// Ticket 03 & Sync-01 & Sync-02: Ingestion Commit API
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

		previousLocation, _ := payload["previous-metadata-location"].(string)

		// Bước 1: GHI ĐỒNG BỘ VÀO POSTGRES CÓ KIỂM TRA CAS (< 2ms)
		if catalogStore != nil {
			if err := catalogStore.SaveTablePointer(c.Context(), ns, table, metadataPath, previousLocation); err != nil {
				if err == storage.ErrCommitConflict {
					return c.Status(fiber.StatusConflict).JSON(fiber.Map{
						"error": fiber.Map{
							"message": "Commit failed: table pointer conflict or concurrent update",
							"type":    "CommitFailedException",
							"code":    409,
						},
					})
				}
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": fiber.Map{
						"message": err.Error(),
						"type":    "InternalServerError",
						"code":    500,
					},
				})
			}
		}

		// Trích xuất snapshot-id thật từ payload updates (Ticket Fix-04)
		var snapshotID int64
		if updates, ok := payload["updates"].([]interface{}); ok {
			for _, u := range updates {
				if updateMap, ok := u.(map[string]interface{}); ok {
					if sid, exists := updateMap["snapshot-id"].(float64); exists {
						snapshotID = int64(sid)
						break
					}
				}
			}
		}

		// Bước 2: KÍCH HOẠT ĐỒNG BỘ DUCKLAKE QUA QUEUE VỚI CƠ CHẾ BACKPRESSURE (Ticket Fix-04)
		if queue != nil {
			ok := queue.Submit(engine.CommitTask{
				Namespace:        ns,
				TableName:        table,
				MetadataLocation: metadataPath,
				SnapshotID:       snapshotID,
			})
			if !ok {
				return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
					"error": fiber.Map{
						"message": "Ingestion queue is saturated. Please retry with exponential backoff.",
						"type":    "QueueSaturatedException",
						"code":    429,
					},
				})
			}
		}
		
		// Bước 3: TRẢ VỀ 200 OK NGAY CHO REDPANDA (< 15ms SLA)
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"metadata-location": metadataPath,
			"table-uuid":        "uuid-123",
		})
	})
}
