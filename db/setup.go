package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

func SetupDatabase(dbPath string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %v", err)
	}

	needsInit := false
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		needsInit = true
		if _, err := os.Create(dbPath); err != nil {
			return nil, fmt.Errorf("failed to create database file: %v", err)
		}
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	if needsInit || NeedsMigration(db) {
		log.Println("Running database migrations...")
		if err := RunMigrations(dbPath); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to run migrations: %v", err)
		}
	}

	// Performance settings
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -2000000;
		PRAGMA temp_store = MEMORY;
		PRAGMA busy_timeout = 5000;
		PRAGMA foreign_keys = ON;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to set database pragmas: %v", err)
	}

	return db, nil
}
