package storage

import (
	"context"
	"errors"
	"fmt"
	"log"

	"bifrost-proxy/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrCommitConflict = errors.New("commit conflict: table pointer has changed or table already exists")

type CatalogStore interface {
	GetTablePointer(ctx context.Context, id domain.TableIdentifier) (string, error)
	SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error
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
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (namespace, table_name)
	);
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

// SaveTablePointer executes an atomic Compare-And-Swap (CAS) update.
func (s *PostgresCatalogStore) SaveTablePointer(ctx context.Context, id domain.TableIdentifier, metadataLocation, previousLocation string) error {
	if previousLocation == "" {
		insertQuery := `
		INSERT INTO catalog_tables (namespace, table_name, metadata_location, previous_metadata_location, updated_at)
		VALUES ($1, $2, $3, '', CURRENT_TIMESTAMP)
		ON CONFLICT (namespace, table_name) DO NOTHING;
		`
		tag, err := s.pool.Exec(ctx, insertQuery, id.Namespace, id.Table, metadataLocation)
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
	    updated_at = CURRENT_TIMESTAMP
	WHERE namespace = $3
	  AND table_name = $4
	  AND metadata_location = $2;
	`
	tag, err := s.pool.Exec(ctx, updateQuery, metadataLocation, previousLocation, id.Namespace, id.Table)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrCommitConflict
	}
	return nil
}
