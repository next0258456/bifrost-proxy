package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"bifrost-proxy/domain"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var ErrTableNotFound = errors.New("table not found")

// StorageService defines the interface for interacting with underlying storage
type StorageService interface {
	GetTableMetadata(ctx context.Context, id domain.TableIdentifier) ([]byte, error)
	GetMetadataByLocation(ctx context.Context, location string) ([]byte, error)
	SaveTableMetadata(ctx context.Context, id domain.TableIdentifier, data []byte) error
}

type MinioStorage struct {
	client *minio.Client
	bucket string
}

func NewMinioStorage(endpoint, accessKey, secretKey, bucket string) (*MinioStorage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}
	return &MinioStorage{
		client: client,
		bucket: bucket,
	}, nil
}

func parseS3Location(location, defaultBucket string) (bucket, key string) {
	if strings.HasPrefix(location, "s3://") {
		trimmed := strings.TrimPrefix(location, "s3://")
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
		return defaultBucket, trimmed
	}
	return defaultBucket, location
}

func (s *MinioStorage) GetMetadataByLocation(ctx context.Context, location string) ([]byte, error) {
	bucket, key := parseS3Location(location, s.bucket)
	obj, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	_, err = obj.Stat()
	if err != nil {
		errResp := minio.ToErrorResponse(err)
		if errResp.Code == "NoSuchKey" {
			return nil, ErrTableNotFound
		}
		return nil, err
	}

	return io.ReadAll(obj)
}

func (s *MinioStorage) GetTableMetadata(ctx context.Context, id domain.TableIdentifier) ([]byte, error) {
	objectName := fmt.Sprintf("%s/%s/metadata.json", id.Namespace, id.Table)
	return s.GetMetadataByLocation(ctx, objectName)
}

func (s *MinioStorage) SaveTableMetadata(ctx context.Context, id domain.TableIdentifier, data []byte) error {
	objectName := fmt.Sprintf("%s/%s/metadata/current.json", id.Namespace, id.Table)
	reader := bytes.NewReader(data)
	_, err := s.client.PutObject(ctx, s.bucket, objectName, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}
