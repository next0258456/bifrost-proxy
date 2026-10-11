package storage

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CatalogStore interface {
	GetTablePointer(ctx context.Context, namespace, table string) (string, error)
	SaveTablePointer(ctx context.Context, namespace, table, metadataLocation, previousLocation string) error
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

	// Auto-migrate catalog_tables table
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

func (s *PostgresCatalogStore) GetTablePointer(ctx context.Context, namespace, table string) (string, error) {
	var metadataLocation string
	query := `SELECT metadata_location FROM catalog_tables WHERE namespace = $1 AND table_name = $2`
	err := s.pool.QueryRow(ctx, query, namespace, table).Scan(&metadataLocation)
	if err != nil {
		return "", ErrTableNotFound
	}
	return metadataLocation, nil
}

func (s *PostgresCatalogStore) SaveTablePointer(ctx context.Context, namespace, table, metadataLocation, previousLocation string) error {
	query := `
	INSERT INTO catalog_tables (namespace, table_name, metadata_location, previous_metadata_location, updated_at)
	VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
	ON CONFLICT (namespace, table_name) DO UPDATE
	SET metadata_location = EXCLUDED.metadata_location,
	    previous_metadata_location = EXCLUDED.previous_metadata_location,
	    updated_at = EXCLUDED.updated_at
	`
	_, err := s.pool.Exec(ctx, query, namespace, table, metadataLocation, previousLocation)
	return err
}
