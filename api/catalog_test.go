package api_test

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
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
// fakeCatalog is an in-memory catalog pointer store honouring CAS semantics and Iceberg requirements.
type fakeCatalog struct {
	pointers  map[string]string
	uuids     map[string]string
	snapshots map[string]int64
	schemas   map[string]int
}

func newFakeCatalog(pointers map[string]string) *fakeCatalog {
	if pointers == nil {
		pointers = make(map[string]string)
	}
	return &fakeCatalog{
		pointers:  pointers,
		uuids:     make(map[string]string),
		snapshots: make(map[string]int64),
		schemas:   make(map[string]int),
	}
}

func (m *fakeCatalog) GetTablePointer(ctx context.Context, id domain.TableIdentifier) (string, error) {
	ptr, ok := m.pointers[id.String()]
	if !ok {
		return "", storage.ErrTableNotFound
	}
	return ptr, nil
}

func (m *fakeCatalog) ValidateRequirements(ctx context.Context, id domain.TableIdentifier, reqs []domain.TableRequirement) error {
	key := id.String()
	_, exists := m.pointers[key]
	for _, req := range reqs {
		switch req.Type {
		case "assert-create":
			if exists {
				return fmt.Errorf("%w: requirement failed: table already exists", storage.ErrCommitConflict)
			}
		case "assert-table-uuid":
			if !exists {
				return fmt.Errorf("%w: requirement failed: table does not exist", storage.ErrCommitConflict)
			}
			currUUID := m.uuids[key]
			if req.UUID != "" && currUUID != req.UUID {
				return fmt.Errorf("%w: requirement failed: table UUID does not match (expected %s, got %s)", storage.ErrCommitConflict, req.UUID, currUUID)
			}
		case "assert-ref-snapshot-id":
			if !exists {
				if req.SnapshotID != nil && *req.SnapshotID != -1 {
					return fmt.Errorf("%w: requirement failed: table does not exist", storage.ErrCommitConflict)
				}
			} else {
				currSnap, ok := m.snapshots[key]
				if req.SnapshotID == nil || *req.SnapshotID == -1 {
					if ok {
						return fmt.Errorf("%w: requirement failed: branch %s expected null snapshot but was %d", storage.ErrCommitConflict, req.Ref, currSnap)
					}
				} else {
					if !ok || currSnap != *req.SnapshotID {
						actual := int64(0)
						if ok {
							actual = currSnap
						}
						return fmt.Errorf("%w: requirement failed: branch %s was %d but expected %d", storage.ErrCommitConflict, req.Ref, actual, *req.SnapshotID)
					}
				}
			}
		case "assert-current-schema-id":
			if !exists {
				return fmt.Errorf("%w: requirement failed: table does not exist", storage.ErrCommitConflict)
			}
			currSchema, ok := m.schemas[key]
			if req.CurrentSchemaID != nil && (!ok || currSchema != *req.CurrentSchemaID) {
				actual := 0
				if ok {
					actual = currSchema
				}
				return fmt.Errorf("%w: requirement failed: current schema id mismatch (expected %d, got %d)", storage.ErrCommitConflict, *req.CurrentSchemaID, actual)
			}
		}
	}
	return nil
}

func (m *fakeCatalog) SaveTableCommit(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string, tableUUID string, snapshotID *int64, schemaID *int) error {
	key := id.String()
	curr, exists := m.pointers[key]
	if previousLocation == "" && exists {
		return storage.ErrCommitConflict
	}
	if previousLocation != "" && curr != previousLocation {
		return storage.ErrCommitConflict
	}
	m.pointers[key] = metadataLocation
	if tableUUID != "" {
		m.uuids[key] = tableUUID
	}
	if snapshotID != nil {
		m.snapshots[key] = *snapshotID
	}
	if schemaID != nil {
		m.schemas[key] = *schemaID
	}
	return nil
}

func (m *fakeCatalog) SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error {
	return m.SaveTableCommit(ctx, id, metadataLocation, previousLocation, "", nil, nil)
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
	api.SetupCatalogRoutes(app, &fakeStorage{files: files}, newFakeCatalog(pointers), queue)
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
	storage := &fakeStorage{files: map[string][]byte{
		"s3://lakehouse/bronze/fmcg_sales/metadata/v1.json": []byte(`{"format-version": 2, "table-uuid": "uuid-1", "current-snapshot-id": 4242}`),
		"s3://lakehouse/bronze/fmcg_sales/metadata/v2.json": []byte(`{"format-version": 2, "table-uuid": "uuid-1", "current-snapshot-id": 5353}`),
	}}
	catalog := newFakeCatalog(map[string]string{})
	app := fiber.New()
	api.SetupCatalogRoutes(app, storage, catalog, &recordingQueue{})

	// Commit 1: snapshot 4242
	status1, body1, _ := post(app, "/v1/namespaces/bronze/tables/fmcg_sales", `{
		"updates": [{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 4242}],
		"metadata-location": "s3://lakehouse/bronze/fmcg_sales/metadata/v1.json"
	}`)
	assert.Equal(t, 200, status1)
	assert.Contains(t, body1, "s3://lakehouse/bronze/fmcg_sales/metadata/v1.json")

	// Verify via HTTP GET endpoint (Single Test Seam)
	getReq1 := httptest.NewRequest("GET", "/v1/namespaces/bronze/tables/fmcg_sales", nil)
	getResp1, _ := app.Test(getReq1, -1)
	getBody1, _ := io.ReadAll(getResp1.Body)
	assert.Equal(t, 200, getResp1.StatusCode)
	assert.Contains(t, string(getBody1), "4242")

	// Commit 2: snapshot 5353
	status2, body2, _ := post(app, "/v1/namespaces/bronze/tables/fmcg_sales", `{
		"updates": [{"action": "set-current-snapshot", "snapshot-id": 5353}],
		"metadata-location": "s3://lakehouse/bronze/fmcg_sales/metadata/v2.json",
		"previous-metadata-location": "s3://lakehouse/bronze/fmcg_sales/metadata/v1.json"
	}`)
	assert.Equal(t, 200, status2)
	assert.Contains(t, body2, "s3://lakehouse/bronze/fmcg_sales/metadata/v2.json")

	// Verify via HTTP GET endpoint (Single Test Seam)
	getReq2 := httptest.NewRequest("GET", "/v1/namespaces/bronze/tables/fmcg_sales", nil)
	getResp2, _ := app.Test(getReq2, -1)
	getBody2, _ := io.ReadAll(getResp2.Body)
	assert.Equal(t, 200, getResp2.StatusCode)
	assert.Contains(t, string(getBody2), "5353")
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
	// 1. Simulated queue saturation check
	appSim := newApp(nil, map[string]string{}, &recordingQueue{full: true})
	status, body, headers := post(appSim, "/v1/namespaces/default/tables/t", `{"metadata-location": "s3://b/v1.json"}`)
	assert.Equal(t, 429, status)
	assert.Equal(t, "1", headers["Retry-After"])
	assert.Contains(t, body, "QueueSaturatedException")

	// 2. Real IngestionQueue buffer saturation via concurrent HTTP requests bursting beyond capacity
	// Initialize real queue with buffer size 1. Worker is alive and running.
	realQueue := engine.NewIngestionQueue(nil, 1)
	defer realQueue.Stop()

	appReal := newApp(nil, map[string]string{}, realQueue)

	const concurrency = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	statusCodes := make([]int, 0, concurrency)
	retryAfters := make([]string, 0, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			payload := fmt.Sprintf(`{"metadata-location": "s3://b/table-%d.json"}`, id)
			path := fmt.Sprintf("/v1/namespaces/default/tables/table_%d", id)
			st, _, hd := post(appReal, path, payload)
			mu.Lock()
			statusCodes = append(statusCodes, st)
			if hd["Retry-After"] != "" {
				retryAfters = append(retryAfters, hd["Retry-After"])
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// Under 20 concurrent requests with buffer capacity 1 and 1 sequential worker,
	// at least one request must have been rejected with 429 and Retry-After: 1
	var has429 bool
	for _, code := range statusCodes {
		if code == 429 {
			has429 = true
			break
		}
	}
	assert.True(t, has429, "Expected at least one HTTP 429 response under concurrent burst load")
	assert.Contains(t, retryAfters, "1", "Expected Retry-After header with value '1' on 429 response")
}

func TestCatalogCommitConflictOnMismatchedUUID(t *testing.T) {
	app := newApp(nil, map[string]string{}, &recordingQueue{})
	path := "/v1/namespaces/default/tables/uuid_test"

	// Create table with UUID "real-uuid-1"
	status, _, _ := post(app, path, `{
		"updates": [{"action": "assign-uuid", "uuid": "real-uuid-1"}],
		"metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 200, status)

	// Try update with wrong asserted UUID
	status, body, _ := post(app, path, `{
		"requirements": [{"type": "assert-table-uuid", "uuid": "wrong-uuid"}],
		"metadata-location": "s3://b/v2.json",
		"previous-metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 409, status)
	assert.Contains(t, body, "CommitFailedException")
	assert.Contains(t, body, "table UUID does not match")
}

func TestCatalogCommitConflictOnMismatchedSnapshotID(t *testing.T) {
	app := newApp(nil, map[string]string{}, &recordingQueue{})
	path := "/v1/namespaces/default/tables/snap_test"

	// Initial commit: snapshot 100
	status, _, _ := post(app, path, `{
		"updates": [{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 100}],
		"metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 200, status)

	// Subsequent commit asserts snapshot 99 (stale/mismatched branch)
	status, body, _ := post(app, path, `{
		"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 99}],
		"metadata-location": "s3://b/v2.json",
		"previous-metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 409, status)
	assert.Contains(t, body, "CommitFailedException")
	assert.Contains(t, body, "expected 99")
}

func TestCatalogCommitConflictOnAssertCreateWhenTableExists(t *testing.T) {
	app := newApp(nil, map[string]string{"default.existing": "s3://b/v1.json"}, &recordingQueue{})

	status, body, _ := post(app, "/v1/namespaces/default/tables/existing", `{
		"requirements": [{"type": "assert-create"}],
		"metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 409, status)
	assert.Contains(t, body, "CommitFailedException")
	assert.Contains(t, body, "already exists")
}

func TestCatalogCommitSuccessWithValidRequirements(t *testing.T) {
	app := newApp(nil, map[string]string{}, &recordingQueue{})
	path := "/v1/namespaces/default/tables/ok_test"

	// Create with assert-create
	status, _, _ := post(app, path, `{
		"requirements": [{"type": "assert-create"}],
		"updates": [
			{"action": "assign-uuid", "uuid": "valid-uuid"},
			{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 500}
		],
		"metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 200, status)

	// Update with matching UUID and matching snapshot
	status, _, _ = post(app, path, `{
		"requirements": [
			{"type": "assert-table-uuid", "uuid": "valid-uuid"},
			{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 500}
		],
		"updates": [
			{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 501}
		],
		"metadata-location": "s3://b/v2.json",
		"previous-metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 200, status)
}

func TestCatalogCommitConflictOnMismatchedSchemaID(t *testing.T) {
	app := newApp(nil, map[string]string{}, &recordingQueue{})
	path := "/v1/namespaces/default/tables/schema_test"

	// Initial commit sets schema-id: 0 (valid initial Iceberg schema)
	status, _, _ := post(app, path, `{
		"updates": [{"action": "set-current-schema", "schema-id": 0}],
		"metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 200, status)

	// Commit asserting wrong schema ID (e.g. 1) -> 409 Conflict
	status, body, _ := post(app, path, `{
		"requirements": [{"type": "assert-current-schema-id", "current-schema-id": 1}],
		"metadata-location": "s3://b/v2.json",
		"previous-metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 409, status)
	assert.Contains(t, body, "CommitFailedException")
	assert.Contains(t, body, "current schema id mismatch")

	// Commit asserting matching schema ID 0 -> 200 OK
	status, _, _ = post(app, path, `{
		"requirements": [{"type": "assert-current-schema-id", "current-schema-id": 0}],
		"updates": [{"action": "set-current-schema", "schema-id": 1}],
		"metadata-location": "s3://b/v2.json",
		"previous-metadata-location": "s3://b/v1.json"
	}`)
	assert.Equal(t, 200, status)
}

