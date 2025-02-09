package db

import (
	"database/sql"
	"log"
)

// DumpInfo is a structure that holds dump information
type DumpInfo struct {
	DumpID         int64
	IsAmend        bool
	ProcessedPaths map[string]struct{}
}

// CreateOrResumeDump creates a new dump or resumes an existing one
func CreateOrResumeDump(db *sql.DB, storageName string, amend bool) *DumpInfo {
	info := &DumpInfo{
		ProcessedPaths: make(map[string]struct{}),
		IsAmend:        amend,
	}

	if amend {
		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("Failed to begin transaction: %v", err)
		}

		err = tx.QueryRow(`
			SELECT dump_id FROM dumps 
			WHERE storage_name = ? 
			ORDER BY created_at DESC 
			LIMIT 1
		`, storageName).Scan(&info.DumpID)
		if err == sql.ErrNoRows {
			tx.Rollback()
			log.Printf("Warning: No previous dump found for storage name %s. Creating new.", storageName)
			info.IsAmend = false
		} else if err != nil {
			tx.Rollback()
			log.Fatalf("Error searching for previous dump: %v", err)
		} else {
			// Get processed file paths
			rows, err := tx.Query(`
				SELECT file_path 
				FROM file_metadata 
				WHERE dump_id = ?
			`, info.DumpID)
			if err != nil {
				tx.Rollback()
				log.Fatalf("Error retrieving processed files: %v", err)
			}
			defer rows.Close()

			for rows.Next() {
				var path string
				if err := rows.Scan(&path); err != nil {
					tx.Rollback()
					log.Fatalf("Error reading processed file: %v", err)
				}
				info.ProcessedPaths[path] = struct{}{}
			}

			if err := tx.Commit(); err != nil {
				log.Fatalf("Failed to commit transaction: %v", err)
			}

			log.Printf("Will skip %d previously processed files", len(info.ProcessedPaths))
			return info
		}
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	result, err := tx.Exec(
		"INSERT INTO dumps (storage_name, created_at) VALUES (?, strftime('%s', 'now'))",
		storageName,
	)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to insert into dumps: %v", err)
	}

	info.DumpID, err = result.LastInsertId()
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to get last insert ID: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	return info
}
