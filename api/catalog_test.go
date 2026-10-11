package api_test

import (
	"context"
	"fmt"
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

