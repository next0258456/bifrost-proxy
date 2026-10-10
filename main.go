package main

import (
	"log"

	"bifrost-proxy/api"

	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New(fiber.Config{
		// Optimize for SLA
		DisableStartupMessage: true,
	})

	api.SetupAuthRoutes(app)

	log.Println("Bifrost Proxy is starting on :8080...")
	log.Fatal(app.Listen(":8080"))
}
