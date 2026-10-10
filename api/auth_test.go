package api_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"bifrost-proxy/api"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

func TestAuthFakeToken(t *testing.T) {
	app := fiber.New()
	api.SetupAuthRoutes(app)

	// Create a mock POST request to /v1/oauth/tokens
	req := httptest.NewRequest("POST", "/v1/oauth/tokens", strings.NewReader("grant_type=client_credentials&client_id=redpanda"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Perform the request
	resp, err := app.Test(req, -1)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	// Read response body
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	// Verify JSON structure
	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	assert.NoError(t, err)

	assert.Equal(t, "bearer", result["token_type"])
	assert.Contains(t, result["access_token"], "bifrost-fake-token")
	assert.Equal(t, float64(86400), result["expires_in"])
}
