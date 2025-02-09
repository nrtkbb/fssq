package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/nrtkbb/fssq/models"
)

func MergeDatabase(ctx context.Context, sourceDB, destDB string) error {
	source, err := sql.Open("sqlite3", sourceDB)
	if err != nil {
		return fmt.Errorf("failed to open source database: %v", err)
	}
	defer source.Close()

	dest, err := sql.Open("sqlite3", destDB)
	if err != nil {
		return fmt.Errorf("failed to open destination database: %v", err)
	}
	defer dest.Close()

	// Get the maximum dump_id from destination
	var maxDumpID int64
	err = dest.QueryRow("SELECT COALESCE(MAX(dump_id), 0) FROM dumps").Scan(&maxDumpID)
	if err != nil {
		return fmt.Errorf("failed to get max dump_id: %v", err)
	}

	// Start transaction
	destTx, err := dest.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer destTx.Rollback()

	// Copy dumps with updated IDs
	rows, err := source.Query(`
		SELECT dump_id, storage_name, created_at, processed_at, 
		file_count, directory_count, total_size_bytes 
		FROM dumps
	`)
	if err != nil {
		return fmt.Errorf("failed to query source dumps: %v", err)
	}
	defer rows.Close()

	// Prepare statements
	dumpStmt, err := destTx.Prepare(`
		INSERT INTO dumps (
			dump_id, storage_name, created_at, processed_at, 
			file_count, directory_count, total_size_bytes
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare dump statement: %v", err)
	}
	defer dumpStmt.Close()

	// Map old dump IDs to new ones
	dumpIDMap := make(map[int64]int64)
	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var oldID, created, processed, fileCount, dirCount, totalSize int64
			var storageName string
			if err := rows.Scan(&oldID, &storageName, &created, &processed, &fileCount, &dirCount, &totalSize); err != nil {
				return fmt.Errorf("failed to scan dump row: %v", err)
			}

			newID := maxDumpID + oldID
			dumpIDMap[oldID] = newID

			if _, err := dumpStmt.Exec(newID, storageName+"_merged", created, processed, fileCount, dirCount, totalSize); err != nil {
				return fmt.Errorf("failed to insert dump: %v", err)
			}
		}
	}

	// Prepare metadata statement
	metadataStmt, err := destTx.Prepare(`
		INSERT INTO file_metadata (
			dump_id, file_path, file_name, directory, size_bytes,
			creation_time_utc, modification_time_utc, access_time_utc,
			file_mode, is_directory, is_file, is_symlink,
			is_hidden, is_system, is_archive, is_readonly,
			file_extension, sha256
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare metadata statement: %v", err)
	}
	defer metadataStmt.Close()

	// Process metadata in batches
	const batchSize = 1000
	for oldDumpID := range dumpIDMap {
		offset := 0
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				rows, err := source.Query(`
					SELECT * FROM file_metadata 
					WHERE dump_id = ? 
					LIMIT ? OFFSET ?
				`, oldDumpID, batchSize, offset)
				if err != nil {
					return fmt.Errorf("failed to query metadata: %v", err)
				}

				count := 0
				for rows.Next() {
					var metadata models.FileMetadata
					var dumpID int64
					var sha256Str sql.NullString

					// Scan all fields
					err := rows.Scan(
						&dumpID,
						&metadata.FilePath,
						&metadata.FileName,
						&metadata.Directory,
						&metadata.SizeBytes,
						&metadata.CreationTimeUTC,
						&metadata.ModificationTimeUTC,
						&metadata.AccessTimeUTC,
						&metadata.FileMode,
						&metadata.IsDirectory,
						&metadata.IsFile,
						&metadata.IsSymlink,
						&metadata.IsHidden,
						&metadata.IsSystem,
						&metadata.IsArchive,
						&metadata.IsReadonly,
						&metadata.FileExtension,
						&sha256Str,
					)
					if err != nil {
						rows.Close()
						return fmt.Errorf("failed to scan metadata row: %v", err)
					}

					// Handle NULL sha256 values
					if sha256Str.Valid {
						metadata.SHA256 = &sha256Str.String
					}

					// Insert with new dump_id
					newDumpID := dumpIDMap[dumpID]
					_, err = metadataStmt.Exec(
						newDumpID,
						metadata.FilePath,
						metadata.FileName,
						metadata.Directory,
						metadata.SizeBytes,
						metadata.CreationTimeUTC,
						metadata.ModificationTimeUTC,
						metadata.AccessTimeUTC,
						metadata.FileMode,
						metadata.IsDirectory,
						metadata.IsFile,
						metadata.IsSymlink,
						metadata.IsHidden,
						metadata.IsSystem,
						metadata.IsArchive,
						metadata.IsReadonly,
						metadata.FileExtension,
						metadata.SHA256,
					)
					if err != nil {
						rows.Close()
						return fmt.Errorf("failed to insert metadata: %v", err)
					}
					count++
				}
				rows.Close()

				if count < batchSize {
					break
				}
				offset += batchSize

				log.Printf("Processed %d records from dump_id %d", offset+count, oldDumpID)
			}
		}
	}

	return destTx.Commit()
}
