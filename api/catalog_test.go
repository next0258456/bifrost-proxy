package api_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"bifrost-proxy/api"
	"bifrost-proxy/domain"
	"bifrost-proxy/engine"
	"bifrost-proxy/storage"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

// fakeStorage is an in-memory object store keyed by metadata location.
type fakeStorage struct {
	files map[string][]byte
}

func (m *fakeStorage) GetTableMetadata(ctx context.Context, id domain.TableIdentifier) ([]byte, error) {
	return nil, storage.ErrTableNotFound
}

func (m *fakeStorage) GetMetadataByLocation(ctx context.Context, location string) ([]byte, error) {
	data, ok := m.files[location]
	if !ok {
		return nil, storage.ErrTableNotFound
	}
	return data, nil
}

func (m *fakeStorage) SaveTableMetadata(ctx context.Context, id domain.TableIdentifier, data []byte) error {
	return nil
}

// fakeCatalog is an in-memory catalog pointer store honouring CAS semantics.
type fakeCatalog struct {
	pointers map[string]string
}

func (m *fakeCatalog) GetTablePointer(ctx context.Context, id domain.TableIdentifier) (string, error) {
	ptr, ok := m.pointers[id.String()]
	if !ok {
		return "", storage.ErrTableNotFound
	}
	return ptr, nil
}

func (m *fakeCatalog) SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error {
	curr, exists := m.pointers[id.String()]
	if previousLocation == "" && exists {
		return storage.ErrCommitConflict
	}
	if previousLocation != "" && curr != previousLocation {
		return storage.ErrCommitConflict
	}
	m.pointers[id.String()] = metadataLocation
	return nil
}

func (m *fakeCatalog) Close() {}

// recordingQueue captures submitted tasks and can simulate saturation.
type recordingQueue struct {
	full  bool
	tasks []engine.CommitTask
}

func (q *recordingQueue) Submit(task engine.CommitTask) bool {
	if q.full {
		return false
	}
	q.tasks = append(q.tasks, task)
	return true
}

func (q *recordingQueue) Stop() {}

func newApp(files map[string][]byte, pointers map[string]string, queue engine.QueueService) *fiber.App {
	app := fiber.New()
	api.SetupCatalogRoutes(app, &fakeStorage{files: files}, &fakeCatalog{pointers: pointers}, queue)
	return app
}

func post(app *fiber.App, path, body string) (int, string, map[string]string) {
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := app.Test(req, -1)
	b, _ := io.ReadAll(resp.Body)
	headers := map[string]string{"Retry-After": resp.Header.Get("Retry-After")}
	return resp.StatusCode, string(b), headers
}

func TestCatalogRead(t *testing.T) {
	app := newApp(
		map[string][]byte{"s3://lakehouse/default/t/metadata/v1.json": []byte(`{"format-version": 2, "table-uuid": "uuid-123"}`)},
		map[string]string{"default.t": "s3://lakehouse/default/t/metadata/v1.json"},
		&recordingQueue{},
	)

	resp, _ := app.Test(httptest.NewRequest("GET", "/v1/namespaces/default/tables/t", nil), -1)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(body), "uuid-123")

	resp404, _ := app.Test(httptest.NewRequest("GET", "/v1/namespaces/default/tables/missing", nil), -1)
	body404, _ := io.ReadAll(resp404.Body)
	assert.Equal(t, 404, resp404.StatusCode)
	assert.Contains(t, string(body404), "NoSuchTableException")
}

func TestCatalogCommitSuccessPropagatesRealSnapshotID(t *testing.T) {
	q := &recordingQueue{}
	app := newApp(nil, map[string]string{}, q)

	status, body, _ := post(app, "/v1/namespaces/bronze/tables/fmcg_sales", `{
		"updates": [{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 4242}],
		"metadata-location": "s3://lakehouse/bronze/fmcg_sales/metadata/v1.json"
	}`)

	assert.Equal(t, 200, status)
	assert.Contains(t, body, "s3://lakehouse/bronze/fmcg_sales/metadata/v1.json")
	// Test with set-current-snapshot as well
	status, _, _ = post(app, "/v1/namespaces/bronze/tables/fmcg_sales", `{
		"updates": [{"action": "set-current-snapshot", "snapshot-id": 5353}],
		"metadata-location": "s3://lakehouse/bronze/fmcg_sales/metadata/v2.json",
		"previous-metadata-location": "s3://lakehouse/bronze/fmcg_sales/metadata/v1.json"
	}`)
	assert.Equal(t, 200, status)
	if assert.Len(t, q.tasks, 2) {
		assert.Equal(t, int64(5353), q.tasks[1].SnapshotID)
	}
}

func TestCatalogCommitConflictOnStalePreviousPointer(t *testing.T) {
	app := newApp(nil, map[string]string{}, &recordingQueue{})
	path := "/v1/namespaces/default/tables/t"

	status, _, _ := post(app, path, `{"metadata-location": "s3://b/v1.json"}`)
	assert.Equal(t, 200, status)

	status, body, _ := post(app, path, `{"metadata-location": "s3://b/v2.json", "previous-metadata-location": "s3://b/STALE.json"}`)
	assert.Equal(t, 409, status)
	assert.Contains(t, body, "CommitFailedException")

	status, _, _ = post(app, path, `{"metadata-location": "s3://b/v2.json", "previous-metadata-location": "s3://b/v1.json"}`)
	assert.Equal(t, 200, status)
}

func TestCatalogCommitRejectsBadPayload(t *testing.T) {
	app := newApp(nil, map[string]string{}, &recordingQueue{})

	status, body, _ := post(app, "/v1/namespaces/default/tables/t", `{not json`)
	assert.Equal(t, 400, status)
	assert.Contains(t, body, "BadRequestException")

	status, _, _ = post(app, "/v1/namespaces/default/tables/t", `{}`)
	assert.Equal(t, 400, status)
}

func TestCatalogCommitSaturatedQueueReturns429WithRetryAfter(t *testing.T) {
	app := newApp(nil, map[string]string{}, &recordingQueue{full: true})

	status, body, headers := post(app, "/v1/namespaces/default/tables/t", `{"metadata-location": "s3://b/v1.json"}`)
	assert.Equal(t, 429, status)
	assert.Equal(t, "1", headers["Retry-After"])
	assert.Contains(t, body, "QueueSaturatedException")
}
