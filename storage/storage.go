package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var ErrTableNotFound = errors.New("table not found")

// StorageService defines the interface for interacting with underlying storage
type StorageService interface {
	GetTableMetadata(ctx context.Context, namespace, table string) ([]byte, error)
	SaveTableMetadata(ctx context.Context, namespace, table string, data []byte) error
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

func (s *MinioStorage) GetTableMetadata(ctx context.Context, namespace, table string) ([]byte, error) {
	objectName := fmt.Sprintf("%s/%s/metadata.json", namespace, table)
	
	obj, err := s.client.GetObject(ctx, s.bucket, objectName, minio.GetObjectOptions{})
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

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *MinioStorage) SaveTableMetadata(ctx context.Context, namespace, table string, data []byte) error {
	// To be expanded in production, uploads file to bucket
	return nil
}
