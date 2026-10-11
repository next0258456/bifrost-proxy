package storage

import (
	"context"
	"errors"
	"fmt"
	"log"

	"bifrost-proxy/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrCommitConflict = errors.New("commit conflict: table pointer has changed or table already exists")

type CatalogStore interface {
	GetTablePointer(ctx context.Context, id domain.TableIdentifier) (string, error)
	ValidateRequirements(ctx context.Context, id domain.TableIdentifier, reqs []domain.TableRequirement) error
	SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error
	SaveTableCommit(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string, tableUUID string, snapshotID *int64, schemaID *int) error
	Close()
}

type PostgresCatalogStore struct {
	pool *pgxpool.Pool
}

func NewPostgresCatalogStore(ctx context.Context, connString string) (*PostgresCatalogStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	query := `
	CREATE TABLE IF NOT EXISTS catalog_tables (
		namespace VARCHAR(255) NOT NULL,
		table_name VARCHAR(255) NOT NULL,
		metadata_location TEXT NOT NULL,
		previous_metadata_location TEXT,
		table_uuid VARCHAR(255),
		current_snapshot_id BIGINT,
		current_schema_id INT,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (namespace, table_name)
	);
	ALTER TABLE catalog_tables ADD COLUMN IF NOT EXISTS table_uuid VARCHAR(255);
	ALTER TABLE catalog_tables ADD COLUMN IF NOT EXISTS current_snapshot_id BIGINT;
	ALTER TABLE catalog_tables ADD COLUMN IF NOT EXISTS current_schema_id INT;
	`
	if _, err := pool.Exec(ctx, query); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create catalog_tables schema: %w", err)
	}

	log.Println("PostgreSQL Catalog Store initialized successfully.")
	return &PostgresCatalogStore{pool: pool}, nil
}

func (s *PostgresCatalogStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *PostgresCatalogStore) GetTablePointer(ctx context.Context, id domain.TableIdentifier) (string, error) {
	var metadataLocation string
	query := `SELECT metadata_location FROM catalog_tables WHERE namespace = $1 AND table_name = $2`
	err := s.pool.QueryRow(ctx, query, id.Namespace, id.Table).Scan(&metadataLocation)
	if err != nil {
		return "", ErrTableNotFound
	}
	return metadataLocation, nil
}

// ValidateRequirements checks table requirements against current Postgres state before commit.
func (s *PostgresCatalogStore) ValidateRequirements(ctx context.Context, id domain.TableIdentifier, reqs []domain.TableRequirement) error {
	if len(reqs) == 0 {
		return nil
	}

	query := `
	SELECT metadata_location, table_uuid, current_snapshot_id, current_schema_id
	FROM catalog_tables
	WHERE namespace = $1 AND table_name = $2
	`
	var (
		loc        string
		currUUID   *string
		currSnapID *int64
		currSchema *int
	)

	err := s.pool.QueryRow(ctx, query, id.Namespace, id.Table).Scan(&loc, &currUUID, &currSnapID, &currSchema)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	tableExists := err == nil

	for _, req := range reqs {
		switch req.Type {
		case "assert-create":
			if tableExists {
				return fmt.Errorf("%w: requirement failed: table %s already exists", ErrCommitConflict, id.String())
			}
		case "assert-table-uuid":
			if !tableExists {
				return fmt.Errorf("%w: requirement failed: table %s does not exist", ErrCommitConflict, id.String())
			}
			actual := ""
			if currUUID != nil {
				actual = *currUUID
			}
			if req.UUID != "" && actual != req.UUID {
				return fmt.Errorf("%w: requirement failed: table UUID does not match (expected %s, got %s)", ErrCommitConflict, req.UUID, actual)
			}
		case "assert-ref-snapshot-id":
			if !tableExists {
				if req.SnapshotID != nil && *req.SnapshotID != -1 {
					return fmt.Errorf("%w: requirement failed: table %s does not exist for ref %s", ErrCommitConflict, id.String(), req.Ref)
				}
			} else {
				if req.SnapshotID == nil || *req.SnapshotID == -1 {
					if currSnapID != nil {
						return fmt.Errorf("%w: requirement failed: branch %s expected null snapshot but was %d", ErrCommitConflict, req.Ref, *currSnapID)
					}
				} else {
					expected := *req.SnapshotID
					if currSnapID == nil || *currSnapID != expected {
						actual := int64(0)
						if currSnapID != nil {
							actual = *currSnapID
						}
						return fmt.Errorf("%w: requirement failed: branch %s was %d but expected %d", ErrCommitConflict, req.Ref, actual, expected)
					}
				}
			}
		case "assert-current-schema-id":
			if !tableExists {
				return fmt.Errorf("%w: requirement failed: table %s does not exist", ErrCommitConflict, id.String())
			}
			if req.CurrentSchemaID != nil {
				if currSchema == nil || *currSchema != *req.CurrentSchemaID {
					actual := 0
					if currSchema != nil {
						actual = *currSchema
					}
					return fmt.Errorf("%w: requirement failed: current schema id mismatch (expected %d, got %d)", ErrCommitConflict, *req.CurrentSchemaID, actual)
				}
			}
		}
	}
	return nil
}

// SaveTablePointer executes atomic pointer update without extra Iceberg metadata fields.
func (s *PostgresCatalogStore) SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error {
	return s.SaveTableCommit(ctx, id, metadataLocation, previousLocation, "", nil, nil)
}

// SaveTableCommit executes an atomic Compare-And-Swap (CAS) update with UUID, snapshot ID, and schema ID.
func (s *PostgresCatalogStore) SaveTableCommit(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string, tableUUID string, snapshotID *int64, schemaID *int) error {
	if previousLocation == "" {
		insertQuery := `
		INSERT INTO catalog_tables (namespace, table_name, metadata_location, previous_metadata_location, table_uuid, current_snapshot_id, current_schema_id, updated_at)
		VALUES ($1, $2, $3, '', NULLIF($4, ''), $5, $6, CURRENT_TIMESTAMP)
		ON CONFLICT (namespace, table_name) DO NOTHING;
		`
		tag, err := s.pool.Exec(ctx, insertQuery, id.Namespace, id.Table, metadataLocation, tableUUID, snapshotID, schemaID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrCommitConflict
		}
		return nil
	}

	updateQuery := `
	UPDATE catalog_tables
	SET metadata_location = $1,
	    previous_metadata_location = $2,
	    table_uuid = COALESCE(NULLIF($3, ''), table_uuid),
	    current_snapshot_id = COALESCE($4, current_snapshot_id),
	    current_schema_id = COALESCE($5, current_schema_id),
	    updated_at = CURRENT_TIMESTAMP
	WHERE namespace = $6
	  AND table_name = $7
	  AND metadata_location = $2;
	`
	tag, err := s.pool.Exec(ctx, updateQuery, metadataLocation, previousLocation, tableUUID, snapshotID, schemaID, id.Namespace, id.Table)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrCommitConflict
	}
	return nil
}
