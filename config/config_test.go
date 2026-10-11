package config_test

import (
	"testing"

	"bifrost-proxy/config"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("PG_CONN_STRING", "")
	cfg := config.LoadConfig()
	assert.Equal(t, "minio:9000", cfg.S3Endpoint)
	assert.Equal(t, "lakehouse", cfg.S3Bucket)
	assert.Contains(t, cfg.PGConnString, "postgres:5432/polaris")
}

func TestLoadConfigOverride(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "localhost:9000")
	cfg := config.LoadConfig()
	assert.Equal(t, "localhost:9000", cfg.S3Endpoint)
}
