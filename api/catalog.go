package api

import (
	"bifrost-proxy/domain"
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
)

// writeError renders the Iceberg REST error envelope.
func writeError(c *fiber.Ctx, status int, errorType, message string) error {
	return c.Status(status).JSON(fiber.Map{
		"error": fiber.Map{
			"message": message,
			"type":    errorType,
			"code":    status,
		},
	})
}

func SetupCatalogRoutes(app *fiber.App, store storage.StorageService, catalogStore storage.CatalogStore, queue engine.QueueService) {
	// Load table: resolve the pointer in the catalog store, then stream that exact metadata file.
	app.Get("/v1/namespaces/:namespace/tables/:table", func(c *fiber.Ctx) error {
		id := domain.NewTableIdentifier(c.Params("namespace"), c.Params("table"))

		metadataPath, err := catalogStore.GetTablePointer(c.Context(), id)
		if err == nil {
			var data []byte
			data, err = store.GetMetadataByLocation(c.Context(), metadataPath)
			if err == nil {
				c.Set("Content-Type", "application/json")
				return c.Send(data)
			}
		}
		if err == storage.ErrTableNotFound {
			return writeError(c, fiber.StatusNotFound, "NoSuchTableException", "Table does not exist")
		}
		return writeError(c, fiber.StatusInternalServerError, "InternalServerError", err.Error())
	})

	// Commit table: write the pointer synchronously, then hand the delta to the async queue.
	app.Post("/v1/namespaces/:namespace/tables/:table", func(c *fiber.Ctx) error {
		id := domain.NewTableIdentifier(c.Params("namespace"), c.Params("table"))

		req, err := domain.ParseUpdateTableRequest(c.Body())
		if err != nil {
			return writeError(c, fiber.StatusBadRequest, "BadRequestException", err.Error())
		}
		if req.MetadataLocation == "" {
			return writeError(c, fiber.StatusBadRequest, "BadRequestException", "metadata-location is required")
		}

		if err := catalogStore.SaveTablePointer(c.Context(), id, req.MetadataLocation, req.PreviousMetadataLocation); err != nil {
			if err == storage.ErrCommitConflict {
				return writeError(c, fiber.StatusConflict, "CommitFailedException", "Commit failed: table pointer conflict or concurrent update")
			}
			return writeError(c, fiber.StatusInternalServerError, "InternalServerError", err.Error())
		}

		if !queue.Submit(engine.CommitTask{
			Table:            id,
			MetadataLocation: req.MetadataLocation,
			SnapshotID:       req.CommittedSnapshotID(),
		}) {
			c.Set("Retry-After", "1")
			return writeError(c, fiber.StatusTooManyRequests, "QueueSaturatedException", "Ingestion queue is saturated. Please retry with backoff.")
		}

		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"metadata-location": req.MetadataLocation,
		})
	})
}
