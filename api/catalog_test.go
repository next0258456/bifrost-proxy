package api_test

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"bifrost-proxy/api"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

// MockStorage simulates S3 Storage
type MockStorage struct {
	Tables map[string][]byte
}

func (m *MockStorage) GetTableMetadata(ctx context.Context, namespace, table string) ([]byte, error) {
	key := namespace + "/" + table
	data, ok := m.Tables[key]
	if !ok {
		return nil, storage.ErrTableNotFound
	}
	return data, nil
}

func (m *MockStorage) SaveTableMetadata(ctx context.Context, namespace, table string, data []byte) error {
	key := namespace + "/" + table
	m.Tables[key] = data
	return nil
}

func TestCatalogRead(t *testing.T) {
	app := fiber.New()
	store := &MockStorage{
		Tables: map[string][]byte{
			"default/test_table": []byte(`{"format-version": 2, "table-uuid": "uuid-123"}`),
		},
	}
	api.SetupCatalogRoutes(app, store, nil)

	// Test 1: Table exists
	req := httptest.NewRequest("GET", "/v1/namespaces/default/tables/test_table", nil)
	resp, _ := app.Test(req, -1)
	assert.Equal(t, 200, resp.StatusCode)
	
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "uuid-123")

	// Test 2: Table does not exist (404)
	req404 := httptest.NewRequest("GET", "/v1/namespaces/default/tables/not_found", nil)
	resp404, _ := app.Test(req404, -1)
	assert.Equal(t, 404, resp404.StatusCode)
	
	body404, _ := io.ReadAll(resp404.Body)
	assert.Contains(t, string(body404), "NoSuchTableException")
}

func TestCatalogCommit(t *testing.T) {
	app := fiber.New()
	store := &MockStorage{Tables: map[string][]byte{}}
	api.SetupCatalogRoutes(app, store, nil)

	req := httptest.NewRequest("POST", "/v1/namespaces/default/tables/test_table", nil)
	resp, _ := app.Test(req, -1)
	assert.Equal(t, 200, resp.StatusCode)
}
