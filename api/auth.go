package api

import (
	"github.com/gofiber/fiber/v2"
)

// SetupAuthRoutes mounts the Iceberg standard auth endpoints
func SetupAuthRoutes(app *fiber.App) {
	app.Post("/v1/oauth/tokens", func(c *fiber.Ctx) error {
		// Just return a fake token to bypass auth.
		// Iceberg standard expects access_token, token_type, expires_in.
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"access_token": "bifrost-fake-token-bypass",
			"token_type":   "bearer",
			"expires_in":   86400,
		})
	})
}
