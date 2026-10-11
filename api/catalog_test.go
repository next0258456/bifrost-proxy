package api_test

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"bifrost-proxy/api"
	"bifrost-proxy/engine"
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

// MockCatalogStore simulates PostgreSQL Catalog Store (Sync-01)
type MockCatalogStore struct {
	Pointers map[string]string
}

func (m *MockCatalogStore) GetTablePointer(ctx context.Context, namespace, table string) (string, error) {
	key := namespace + "/" + table
	ptr, ok := m.Pointers[key]
	if !ok {
		return "", storage.ErrTableNotFound
	}
	return ptr, nil
}

func (m *MockCatalogStore) SaveTablePointer(ctx context.Context, namespace, table, metadataLocation, previousLocation string) error {
	key := namespace + "/" + table
	curr, exists := m.Pointers[key]
	if previousLocation == "" {
		if exists {
			return storage.ErrCommitConflict
		}
		m.Pointers[key] = metadataLocation
		return nil
	}
	if curr != previousLocation {
		return storage.ErrCommitConflict
	}
	m.Pointers[key] = metadataLocation
	return nil
}

func (m *MockCatalogStore) Close() {}

func TestCatalogRead(t *testing.T) {
	app := fiber.New()
	store := &MockStorage{
		Tables: map[string][]byte{
			"default/test_table": []byte(`{"format-version": 2, "table-uuid": "uuid-123"}`),
		},
	}
	catStore := &MockCatalogStore{
		Pointers: map[string]string{
			"default/test_table": "s3://lakehouse/default/test_table/metadata/v1.json",
		},
	}
	api.SetupCatalogRoutes(app, store, catStore, nil)

	// Test 1: Table exists
	req := httptest.NewRequest("GET", "/v1/namespaces/default/tables/test_table", nil)
	resp, _ := app.Test(req, -1)
	assert.Equal(t, 200, resp.StatusCode)
	
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "uuid-123")

	// Test 2: Table does not exist in Postgres Catalog (404)
	req404 := httptest.NewRequest("GET", "/v1/namespaces/default/tables/not_found", nil)
	resp404, _ := app.Test(req404, -1)
	assert.Equal(t, 404, resp404.StatusCode)
	
	body404, _ := io.ReadAll(resp404.Body)
	assert.Contains(t, string(body404), "NoSuchTableException")
}

func TestCatalogCommit(t *testing.T) {
	app := fiber.New()
	store := &MockStorage{Tables: map[string][]byte{}}
	catStore := &MockCatalogStore{Pointers: map[string]string{}}
	queue := engine.NewIngestionQueue(nil, 100)
	defer queue.Stop()

	api.SetupCatalogRoutes(app, store, catStore, queue)

	// 1. Commit ban đầu (tạo mới) -> 200 OK
	req := httptest.NewRequest("POST", "/v1/namespaces/default/tables/test_table", nil)
	resp, _ := app.Test(req, -1)
	assert.Equal(t, 200, resp.StatusCode)
	assert.NotEmpty(t, catStore.Pointers["default/test_table"])

	// 2. Commit tiếp theo nhưng gửi previous location rỗng khi bảng đã tồn tại -> 409 Conflict
	reqConflict := httptest.NewRequest("POST", "/v1/namespaces/default/tables/test_table", nil)
	respConflict, _ := app.Test(reqConflict, -1)
	assert.Equal(t, 409, respConflict.StatusCode)
}
