package engine

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"bifrost-proxy/storage"

	"github.com/linkedin/goavro/v2"
)

type ManifestDataFile struct {
	Path            string
	FileSizeInBytes int64
	RecordCount     int64
	Content         int    // 0 = data, 1 = position deletes, 2 = equality deletes
	ReferencedPath  string // field 2147483546 if delete file
}

// ReadManifestList parses an Avro manifest list file and returns entries added in targetSnapshotID.
func ReadManifestList(data []byte, targetSnapshotID int64) ([]string, error) {
	ocf, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF reader for manifest list: %w", err)
	}

	var manifestPaths []string
	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest list record: %w", err)
		}
		record, ok := datum.(map[string]interface{})
		if !ok {
			continue
		}

		path, _ := record["manifest_path"].(string)
		if path == "" {
			continue
		}

		// If targetSnapshotID is 0, take all manifests
		if targetSnapshotID == 0 {
			manifestPaths = append(manifestPaths, path)
			continue
		}

		if addedSnap, ok := record["added_snapshot_id"].(int64); ok {
			if addedSnap == targetSnapshotID {
				manifestPaths = append(manifestPaths, path)
			}
		} else {
			manifestPaths = append(manifestPaths, path)
		}
	}
	return manifestPaths, nil
}

// ReadManifestFile parses an Iceberg manifest Avro file and returns data and delete files.
func ReadManifestFile(data []byte) ([]ManifestDataFile, error) {
	ocf, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF reader for manifest: %w", err)
	}

	var files []ManifestDataFile
	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest record: %w", err)
		}
		record, ok := datum.(map[string]interface{})
		if !ok {
			continue
		}

		// Iceberg manifest entry status: 0 = EXISTING, 1 = ADDED, 2 = DELETED
		status, _ := record["status"].(int32)
		if status == 2 { // Skip deleted entries in delta addition
			continue
		}

		dataFileMap, ok := record["data_file"].(map[string]interface{})
		if !ok {
			continue
		}

		var mdf ManifestDataFile
		if p, ok := dataFileMap["file_path"].(string); ok {
			mdf.Path = p
		}
		if s, ok := dataFileMap["file_size_in_bytes"].(int64); ok {
			mdf.FileSizeInBytes = s
		}
		if r, ok := dataFileMap["record_count"].(int64); ok {
			mdf.RecordCount = r
		}
		if c, ok := dataFileMap["content"].(int32); ok {
			mdf.Content = int(c)
		}

		if mdf.Path != "" {
			files = append(files, mdf)
		}
	}
	return files, nil
}

// ExtractDataFilesFromStorage traverses manifest list and manifests via StorageService.
func ExtractDataFilesFromStorage(ctx context.Context, store storage.StorageService, manifestListLocation string, snapshotID int64) ([]ManifestDataFile, error) {
	if store == nil || manifestListLocation == "" {
		return nil, nil
	}

	listBytes, err := store.GetMetadataByLocation(ctx, manifestListLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest list from %s: %w", manifestListLocation, err)
	}

	manifestPaths, err := ReadManifestList(listBytes, snapshotID)
	if err != nil {
		log.Printf("Warning: failed to decode manifest list Avro: %v", err)
		return nil, err
	}

	var allFiles []ManifestDataFile
	for _, mPath := range manifestPaths {
		mBytes, err := store.GetMetadataByLocation(ctx, mPath)
		if err != nil {
			log.Printf("Warning: failed to fetch manifest file %s: %v", mPath, err)
			continue
		}
		mFiles, err := ReadManifestFile(mBytes)
		if err != nil {
			log.Printf("Warning: failed to decode manifest Avro %s: %v", mPath, err)
			continue
		}
		allFiles = append(allFiles, mFiles...)
	}
	return allFiles, nil
}
