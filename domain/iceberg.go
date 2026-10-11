package domain

import (
	"encoding/json"
	"fmt"
)

// UpdateTableRequest is the typed form of an Iceberg REST commit payload.
// MetadataLocation and PreviousMetadataLocation are proxy-level fields: the
// writer announces the metadata.json it already put on object storage.
type UpdateTableRequest struct {
	Requirements             []TableRequirement `json:"requirements"`
	Updates                  []TableUpdate      `json:"updates"`
	MetadataLocation         string             `json:"metadata-location"`
	PreviousMetadataLocation string             `json:"previous-metadata-location"`
}

// TableRequirement is one optimistic-concurrency assertion sent by the writer.
type TableRequirement struct {
	Type            string `json:"type"` // assert-table-uuid, assert-ref-snapshot-id, assert-current-schema-id, ...
	UUID            string `json:"uuid,omitempty"`
	Ref             string `json:"ref,omitempty"`
	SnapshotID      *int64 `json:"snapshot-id,omitempty"` // nil means "ref must not exist"
	CurrentSchemaID *int   `json:"current-schema-id,omitempty"`
}

// TableUpdate is one metadata change in a commit.
type TableUpdate struct {
	Action     string          `json:"action"` // add-snapshot, set-snapshot-ref, add-schema, set-current-schema, ...
	RefName    string          `json:"ref-name,omitempty"`
	SnapshotID int64           `json:"snapshot-id,omitempty"`
	SchemaID   int             `json:"schema-id,omitempty"`
	Snapshot   *SnapshotUpdate `json:"snapshot,omitempty"`
}

// SnapshotUpdate is the snapshot carried by an add-snapshot update.
type SnapshotUpdate struct {
	SnapshotID   int64  `json:"snapshot-id"`
	ManifestList string `json:"manifest-list"`
}

// ParseUpdateTableRequest decodes a commit body. An empty body is a valid empty request.
func ParseUpdateTableRequest(body []byte) (*UpdateTableRequest, error) {
	req := &UpdateTableRequest{}
	if len(body) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(body, req); err != nil {
		return nil, fmt.Errorf("invalid commit payload: %w", err)
	}
	return req, nil
}

// CommittedSnapshotID returns the snapshot this commit makes current, or 0 when none is announced.
func (r *UpdateTableRequest) CommittedSnapshotID() int64 {
	var id int64
	for _, u := range r.Updates {
		switch u.Action {
		case "set-snapshot-ref":
			if u.SnapshotID != 0 {
				return u.SnapshotID
			}
		case "add-snapshot":
			if u.Snapshot != nil {
				id = u.Snapshot.SnapshotID
			}
		}
	}
	return id
}
