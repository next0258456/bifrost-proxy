package storage_test

import (
	"errors"
	"testing"

	"bifrost-proxy/domain"
	"bifrost-proxy/storage"

	"github.com/stretchr/testify/assert"
)

func TestStorageErrorDefinitions(t *testing.T) {
	assert.True(t, errors.Is(storage.ErrTableNotFound, storage.ErrTableNotFound))
	assert.True(t, errors.Is(storage.ErrCommitConflict, storage.ErrCommitConflict))
}

func TestTableIdentifierEquality(t *testing.T) {
	id1 := domain.NewTableIdentifier("bronze", "sales")
	id2 := domain.NewTableIdentifier("bronze", "sales")
	id3 := domain.NewTableIdentifier("silver", "sales")

	assert.Equal(t, id1, id2)
	assert.NotEqual(t, id1, id3)
	assert.Equal(t, "bronze.sales", id1.String())
}
