package config

import "os"

// Config holds every environment-driven setting of the proxy in one place.
type Config struct {
	S3Endpoint         string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	S3Bucket           string
	PGConnString       string
}

// LoadConfig reads the environment once and applies the single set of defaults.
func LoadConfig() *Config {
	return &Config{
		S3Endpoint:         getEnv("S3_ENDPOINT", "minio:9000"),
		AWSAccessKeyID:     getEnv("AWS_ACCESS_KEY_ID", "admin"),
		AWSSecretAccessKey: getEnv("AWS_SECRET_ACCESS_KEY", "password"),
		S3Bucket:           getEnv("S3_BUCKET", "lakehouse"),
		PGConnString:       getEnv("PG_CONN_STRING", "postgres://admin:password@postgres:5432/polaris?sslmode=disable"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
