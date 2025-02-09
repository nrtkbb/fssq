package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"embed"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sys/unix"
)

type FileMetadata struct {
	FilePath           string
	FileName           string
	Directory          string
	SizeBytes          int64
	CreationTimeUTC    int64
	ModificationTimeUTC int64
	AccessTimeUTC      int64
	FileMode           string
	IsDirectory        bool
	IsFile             bool
	IsSymlink          bool
	IsHidden           bool
	IsSystem           bool
	IsArchive          bool
	IsReadonly         bool
	FileExtension      string
	SHA256             *string
}

type ProgressStats struct {
	processedFiles int64
	processedBytes int64
	startTime      time.Time
	lastLogTime    time.Time
	mutex          sync.Mutex
}

type AppContext struct {
	db           *sql.DB
	tx           *sql.Tx
	stmt         *sql.Stmt
	metadataChan chan FileMetadata
	wg           *sync.WaitGroup
	stats        *ProgressStats
	cancel       context.CancelFunc
	cleanup      sync.Once
	lastCommit   time.Time
	channelClosed bool
}

const (
	workerCount      = 4    // Number of parallel workers
	batchSize        = 100  // Batch size for SQLite inserts
	progressInterval = 1    // Progress display interval (seconds)
	logInterval      = 5    // Detailed log output interval (seconds)
	filesPerLog      = 1000 // Threshold for file processing count before logging
)

//go:embed schema/migrations/*.sql
var migrationsFS embed.FS

func NewProgressStats() *ProgressStats {
	now := time.Now()
	return &ProgressStats{
		startTime:   now,
		lastLogTime: now,
	}
}

func (ps *ProgressStats) LogProgress(currentPath string) {
	atomic.AddInt64(&ps.processedFiles, 1)
	processedFiles := atomic.LoadInt64(&ps.processedFiles)

	// Log every filesPerLog files
	if processedFiles%filesPerLog == 0 {
		ps.mutex.Lock()
		now := time.Now()
		elapsed := now.Sub(ps.startTime)
		filesPerSecond := float64(processedFiles) / elapsed.Seconds()

		// Get current directory (truncate if too long)
		currentDir := filepath.Dir(currentPath)
		if len(currentDir) > 50 {
			currentDir = "..." + currentDir[len(currentDir)-47:]
		}

		log.Printf("[Progress] Processed %d/ files at %.1f files/sec - Current dir: %s",
			processedFiles, filesPerSecond, currentDir)

		ps.lastLogTime = now
		ps.mutex.Unlock()
	}
}

func (app *AppContext) parformCleanup() {
	app.cleanup.Do(func() {
		log.Println("Starting shutdown...")

		if app.metadataChan != nil && !app.channelClosed {
			close(app.metadataChan)
			app.channelClosed = true
		}

		if app.wg != nil {
			log.Println("Waiting for pending operations to complete...")
			app.wg.Wait()
		}

		// Handle transaction
		if app.tx != nil {
			log.Println("Committing final transaction...")
			if err := app.tx.Commit(); err != nil {
				log.Printf("Error committing transaction during shutdown: %v", err)
				if rbErr := app.tx.Rollback(); rbErr != nil {
					log.Printf("Error rolling back transaction: %v", rbErr)
				}
			}
		}

		// Clean up database connection
		if app.db != nil {
			log.Println("Cleaning up database...")

			// Force WAL checkpoint before closing
			if _, err := app.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				log.Printf("Error executing WAL checkpoint: %v", err)
			}

			if err := app.db.Close(); err != nil {
				log.Printf("Error closing database: %v", err)
			}

			// Check for and clean up WAL and SHM files
			var dbPath string
			err := app.db.QueryRow("PRAGMA database_list").Scan(nil, &dbPath, nil)
			if err != nil {
				log.Printf("Warning: Failed to get database path: %v", err)
			} else {
				walPath := dbPath + "-wal"
				shmPath := dbPath + "-shm"
				
				if _, err := os.Stat(walPath); err == nil {
					log.Printf("WAL file still exists: %s", walPath)
				}
				if _, err := os.Stat(shmPath); err == nil {
					log.Printf("SHM file still exists: %s", shmPath)
				}
			}
		}

		// Clean up statement
		if app.stmt != nil {
			app.stmt.Close()
		}

		log.Println("Graceful shutdown completed")
	})
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize application context
	app := &AppContext{
		wg:     &sync.WaitGroup{},
		cancel: cancel,
	}
	defer app.parformCleanup()

	// Set up signal handling early
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Force quit flag
	var forceQuit atomic.Bool
	
	go func() {
		for sig := range sigChan {
			log.Printf("Received signal: %v", sig)
			if forceQuit.Load() {
				log.Println("Forcing immediate shutdown...")
				os.Exit(1)
			}
			
			forceQuit.Store(true)
			log.Println("Press Ctrl+C again to force quit. Wait for normal shutdown to complete...")
			cancel() // Cancel context to notify goroutines
			
			// Reset forceQuit flag after 5 seconds
			go func() {
				time.Sleep(5 * time.Second)
				forceQuit.Store(false)
			}()
		}
	}()

	storageName, rootDir, dbPath, skipHash, mergeSource, amend := parseFlags()

	// If merge mode is specified, perform merge and exit
	if mergeSource != "" {
		log.Printf("Merging database %s into %s...", mergeSource, dbPath)
		if err := mergeDatabase(ctx, mergeSource, dbPath); err != nil {
			if err == context.Canceled {
				log.Println("Database merge cancelled by user")
				return
			}
			log.Fatalf("Failed to merge database: %v", err)
		}
		log.Println("Database merge completed successfully")
		return
	}

	// Set up database connection and setup
	db, err := setupDatabase(dbPath)
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}
	app.db = db

	// Manage progress
	stats := NewProgressStats()
	app.stats = stats

	// Check if we were cancelled during counting
	if ctx.Err() != nil {
		return
	}

	log.Printf("Starting metadata collection from root directory: %s", rootDir)

	// Create new dump entry (if not in amend mode)
	var dumpID int64
	if amend {
		// Start new transaction for amend mode
		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("Failed to begin transaction: %v", err)
		}
		
		// Get existing dump_id
		err = tx.QueryRow(`
			SELECT dump_id FROM dumps 
			WHERE storage_name = ? 
			ORDER BY created_at DESC 
			LIMIT 1
		`, storageName).Scan(&dumpID)
		if err == sql.ErrNoRows {
			tx.Rollback() // ロールバックしてから新規作成モードへ
			log.Printf("Warning: No previous dump found for storage name %s. Creating new.", storageName)
			amend = false
		} else if err != nil {
			tx.Rollback()
			log.Fatalf("Error searching for previous dump: %v", err)
		} else {
			app.tx = tx // エラーがない場合のみapp.txに設定
			log.Printf("Resuming processing for dump_id %d", dumpID)
		}
	}

	if !amend {
		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("Failed to begin transaction: %v", err)
		}
		
		dumpID, err = createDumpEntry(tx, storageName)
		if err != nil {
			tx.Rollback()
			log.Fatal(err)
		}
		app.tx = tx // エラーがない場合のみapp.txに設定
	}

	// Get processed file paths (for amend mode)
	processedPaths := make(map[string]struct{})
	if amend {
		rows, err := app.tx.Query(`
			SELECT file_path 
			FROM file_metadata 
			WHERE dump_id = ?
		`, dumpID)
		if err != nil {
			log.Fatalf("Error retrieving processed files: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				log.Fatalf("Error reading processed file: %v", err)
			}
			processedPaths[path] = struct{}{}
		}
		log.Printf("Will skip %d previously processed files", len(processedPaths))
	}

	// Metadata processing channel
	metadataChan := make(chan FileMetadata, workerCount*10)
	app.metadataChan = metadataChan

	// Database write goroutine
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		
		// Initial transaction setup
		if err := app.refreshTransaction(); err != nil {
			log.Printf("Failed to setup initial transaction: %v", err)
			app.cancel()
			return
		}

		const commitInterval = 30 * time.Second // Commit every 30 seconds
		processedSinceCommit := 0
		const commitThreshold = 10000          // Or commit every 10000 records

		for metadata := range metadataChan {
			select {
			case <-ctx.Done():
				return
			default:
				_, err := app.stmt.Exec(
					dumpID, metadata.FilePath, metadata.FileName, metadata.Directory,
					metadata.SizeBytes, metadata.CreationTimeUTC, metadata.ModificationTimeUTC,
					metadata.AccessTimeUTC, metadata.FileMode, boolToInt(metadata.IsDirectory),
					boolToInt(metadata.IsFile), boolToInt(metadata.IsSymlink),
					boolToInt(metadata.IsHidden), boolToInt(metadata.IsSystem),
					boolToInt(metadata.IsArchive), boolToInt(metadata.IsReadonly),
					metadata.FileExtension, metadata.SHA256,
				)
				if err != nil {
					log.Printf("Error inserting metadata for %s: %v", metadata.FilePath, err)
				}
				atomic.AddInt64(&stats.processedBytes, metadata.SizeBytes)
				
				processedSinceCommit++

				// Commit when time interval elapsed or record threshold reached
				if time.Since(app.lastCommit) >= commitInterval || processedSinceCommit >= commitThreshold {
					if err := app.refreshTransaction(); err != nil {
						log.Printf("Failed to refresh transaction: %v", err)
						app.cancel()
						return
					}
					log.Printf("Transaction committed (%d records processed)", processedSinceCommit)
					processedSinceCommit = 0
				}
			}
		}

		// Commit any remaining uncommitted data
		if processedSinceCommit > 0 {
			if err := app.refreshTransaction(); err != nil {
				log.Printf("Failed to commit final transaction: %v", err)
			}
		}
	}()

	var newFilesFound int64 // Track count of new files
	const logInterval = 1000 // Log every 1000 new files
	// Modify metadata collection part
	scanComplete := make(chan struct{})
	var scanWg sync.WaitGroup
	scanWg.Add(1)
	go func() {
		defer scanWg.Done()
		err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
			select {
			case <-ctx.Done():
				return filepath.SkipAll
			default:
				if err != nil {
					log.Printf("Warning: Error accessing path %s: %v", path, err)
					return nil
				}

				relPath, err := filepath.Rel(rootDir, path)
				if err != nil {
					log.Printf("Warning: Cannot get relative path for %s: %v", path, err)
					return nil
				}

				// Skip already processed paths in amend mode
				if amend {
					if _, exists := processedPaths[relPath]; exists {
						return nil
					}
					count := atomic.AddInt64(&newFilesFound, 1)
					if count%logInterval == 0 {
						log.Printf("Found %d new files to process", count)
					}
				}

				// Process sequentially
				metadata := collectMetadata(path, info, relPath, skipHash)
				select {
				case <-ctx.Done():
					return filepath.SkipAll
				case metadataChan <- metadata:
					stats.LogProgress(path)
				}

				return nil
			}
		})

		// Final summary after scan completion
		if amend {
			totalNew := atomic.LoadInt64(&newFilesFound)
			if totalNew == 0 {
				log.Println("No new files found. All files already processed.")
			} else {
				log.Printf("Total new files found: %d", totalNew)
			}
		}

		if err != nil && err != filepath.SkipAll {
			log.Printf("Error during file walk: %v", err)
			app.cancel()
		}

		close(scanComplete)
	}()

	// Wait for scan completion and all workers to finish
	<-scanComplete
	scanWg.Wait()

	if err != nil && err != filepath.SkipAll {
		app.parformCleanup()
		log.Fatalf("Failed during file walk: %v", err)
	}

	if ctx.Err() == nil {
		logFinalStatistics(stats)
	}
	// Close metadata channel to signal database worker to finish
	if !app.channelClosed {
		close(app.metadataChan)
		app.channelClosed = true
	}

	// Wait for database worker to complete
	app.wg.Wait()
}

func parseFlags() (string, string, string, bool, string, bool) {
	storageName := flag.String("storage", "", "Storage name identifier")
	rootDir := flag.String("root", "", "Root directory to scan")
	dbPath := flag.String("db", "", "SQLite database path")
	skipHash := flag.Bool("skip-hash", false, "Skip SHA256 hash calculation")
	mergeSource := flag.String("merge", "", "Source database to merge from")
	amend := flag.Bool("amend", false, "Continue from last incomplete dump")
	flag.Parse()

	// Validate normal mode
	if *mergeSource == "" {
		if *storageName == "" || *rootDir == "" || *dbPath == "" {
			log.Fatal("All arguments are required: -storage, -root, -db")
		}
		return *storageName, *rootDir, *dbPath, *skipHash, *mergeSource, *amend
	}

	// Validate merge mode
	if *dbPath == "" {
		log.Fatal("Destination database (-db) is required for merge operation")
	}
	if *mergeSource == *dbPath {
		log.Fatal("Source and destination databases must be different")
	}

	return *storageName, *rootDir, *dbPath, *skipHash, *mergeSource, *amend
}

func createDumpEntry(tx *sql.Tx, storageName string) (int64, error) {
	result, err := tx.Exec(
		"INSERT INTO dumps (storage_name, created_at) VALUES (?, strftime('%s', 'now'))",
		storageName,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into dumps: %v", err)
	}

	dumpID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert ID: %v", err)
	}
	return dumpID, nil
}

func setupDatabase(dbPath string) (*sql.DB, error) {
	// Create directory for database file
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %v", err)
	}

	// Check if database file exists
	needsInit := false
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		needsInit = true
		// Create empty file
		if _, err := os.Create(dbPath); err != nil {
			return nil, fmt.Errorf("failed to create database file: %v", err)
		}
	}

	// sql.Open only validates settings and doesn't establish actual connection,
	// so it won't error even with empty file. Real connection check is done with Ping()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Verify database connection
	// Ping succeeds even with empty file, so we also check for basic tables
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	if needsInit || needsMigration(db) {
		log.Println("Running database migrations...")
		if err := runMigrations(dbPath); err != nil {
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

func needsMigration(db *sql.DB) bool {
	var exists int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='table' AND name='schema_migrations'
	`).Scan(&exists)
	
	return err != nil || exists == 0
}

func runMigrations(dbPath string) error {
	d, err := iofs.New(migrationsFS, "schema/migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %v", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	config := &sqlite3.Config{
		DatabaseName: dbPath,
		NoTxWrap:    true, // sqlite3ではDDLをトランザクションで囲めないため
	}
	driver, err := sqlite3.WithInstance(db, config)
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %v", err)
	}

	m, err := migrate.NewWithInstance(
		"iofs", d,
		"sqlite3", driver,
	)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %v", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %v", err)
	}

	return nil
}

func formatFileMode(mode os.FileMode) string {
	// Mask basic permission bits
	permBits := mode & os.ModePerm

	// Check file type
	var typeChar string
	switch {
	case mode&os.ModeDir != 0:
		typeChar = "d"
	case mode&os.ModeSymlink != 0:
		typeChar = "l"
	default:
		typeChar = "-"
	}

	// Convert permissions to string
	result := typeChar

	// Owner permissions
	result += map[bool]string{true: "r", false: "-"}[(permBits&0400) != 0]
	result += map[bool]string{true: "w", false: "-"}[(permBits&0200) != 0]
	result += map[bool]string{true: "x", false: "-"}[(permBits&0100) != 0]

	// Group permissions
	result += map[bool]string{true: "r", false: "-"}[(permBits&040) != 0]
	result += map[bool]string{true: "w", false: "-"}[(permBits&020) != 0]
	result += map[bool]string{true: "x", false: "-"}[(permBits&010) != 0]

	// Others permissions
	result += map[bool]string{true: "r", false: "-"}[(permBits&04) != 0]
	result += map[bool]string{true: "w", false: "-"}[(permBits&02) != 0]
	result += map[bool]string{true: "x", false: "-"}[(permBits&01) != 0]

	return result
}

func collectMetadata(path string, info os.FileInfo, relPath string, skipHash bool) FileMetadata {
	stat := info.Sys().(*syscall.Stat_t)

	// Get macOS-specific attributes
	var isSystem, isArchive bool
	finderInfo := make([]byte, 32) // FinderInfo
	_, err := unix.Getxattr(path, "com.apple.FinderInfo", finderInfo)
	if err == nil {
		// Parse Finder flags
		isSystem = finderInfo[8]&uint8(0x04) != 0  // kIsSystemFileBit
		isArchive = finderInfo[8]&uint8(0x20) != 0 // kIsArchiveBit
	}

	// Calculate SHA256 hash (only for files)
	var sha256Hash *string
	if !skipHash && !info.IsDir() {
		if hash, err := calculateSHA256(path); err == nil {
			sha256Hash = &hash
		}
	}

	return FileMetadata{
		FilePath:           relPath,
		FileName:           info.Name(),
		Directory:          filepath.Dir(relPath),
		SizeBytes:          info.Size(),
		CreationTimeUTC:    stat.Birthtimespec.Sec,
		ModificationTimeUTC: stat.Mtimespec.Sec,
		AccessTimeUTC:      stat.Atimespec.Sec,
		FileMode:           formatFileMode(info.Mode()),
		IsDirectory:        info.IsDir(),
		IsFile:             !info.IsDir(),
		IsSymlink:          info.Mode()&os.ModeSymlink != 0,
		IsHidden:           strings.HasPrefix(filepath.Base(path), "."),
		IsSystem:           isSystem,
		IsArchive:          isArchive,
		IsReadonly:         info.Mode()&0200 == 0,
		FileExtension:      strings.ToLower(filepath.Ext(path)),
		SHA256:             sha256Hash,
	}
}

func calculateSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func logFinalStatistics(stats *ProgressStats) {
	duration := time.Since(stats.startTime)
	speed := float64(stats.processedFiles) / duration.Seconds()
	log.Printf("Successfully completed metadata collection in %v", duration)
	log.Printf("Final statistics:")
	log.Printf("- Processed %d files", stats.processedFiles)
	log.Printf("- Total size: %.2f GB", float64(stats.processedBytes)/(1024*1024*1024))
	log.Printf("- Average speed: %.1f files/sec", speed)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func mergeDatabase(ctx context.Context, sourceDB, destDB string) error {
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
	rows, err := source.Query("SELECT dump_id, storage_name, created_at, processed_at, file_count, directory_count, total_size_bytes FROM dumps")
	if err != nil {
		return fmt.Errorf("failed to query source dumps: %v", err)
	}
	defer rows.Close()

	// Prepare dump insert statement
	dumpStmt, err := destTx.Prepare(`
		INSERT INTO dumps (dump_id, storage_name, created_at, processed_at, file_count, directory_count, total_size_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?)
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

			_, err = dumpStmt.Exec(newID, storageName+"_merged", created, processed, fileCount, dirCount, totalSize)
			if err != nil {
				return fmt.Errorf("failed to insert dump: %v", err)
			}
		}
	}

	// Copy file metadata with updated dump_ids
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
				query := `SELECT * FROM file_metadata WHERE dump_id = ? LIMIT ? OFFSET ?`
				rows, err := source.Query(query, oldDumpID, batchSize, offset)
				if err != nil {
					return fmt.Errorf("failed to query metadata: %v", err)
				}

				count := 0
				for rows.Next() {
					select {
					case <-ctx.Done():
						rows.Close()
						return ctx.Err()
					default:
						var metadata FileMetadata
						var dumpID int64
						var sha256Str sql.NullString // For handling NULL values in sha256 field

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
							boolToInt(metadata.IsDirectory),
							boolToInt(metadata.IsFile),
							boolToInt(metadata.IsSymlink),
							boolToInt(metadata.IsHidden),
							boolToInt(metadata.IsSystem),
							boolToInt(metadata.IsArchive),
							boolToInt(metadata.IsReadonly),
							metadata.FileExtension,
							metadata.SHA256,
						)
						if err != nil {
							rows.Close()
							return fmt.Errorf("failed to insert metadata: %v", err)
						}
						count++
					}
				}
				rows.Close()

				if count < batchSize {
					break
				}
				offset += batchSize

				// Log progress
				log.Printf("Processed %d records from dump_id %d", offset+count, oldDumpID)
			}
		}
	}

	// Final commit
	if err := destTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func (app *AppContext) refreshTransaction() error {
	if app.tx != nil {
		// Close existing statement
		if app.stmt != nil {
			if err := app.stmt.Close(); err != nil {
				log.Printf("Warning: Failed to close statement: %v", err)
			}
			app.stmt = nil
		}
		
		// Commit current transaction
		if err := app.tx.Commit(); err != nil {
			if rbErr := app.tx.Rollback(); rbErr != nil {
				return fmt.Errorf("failed to rollback after commit error: %v (original error: %v)", rbErr, err)
			}
			return fmt.Errorf("failed to commit transaction: %v", err)
		}
	}

	// Start new transaction
	var err error
	app.tx, err = app.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start new transaction: %v", err)
	}

	// Create new prepared statement
	app.stmt, err = app.tx.Prepare(`
		INSERT INTO file_metadata (
			dump_id, file_path, file_name, directory, size_bytes,
			creation_time_utc, modification_time_utc, access_time_utc,
			file_mode, is_directory, is_file, is_symlink,
			is_hidden, is_system, is_archive, is_readonly,
			file_extension, sha256
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		if rbErr := app.tx.Rollback(); rbErr != nil {
			return fmt.Errorf("failed to rollback after prepare statement error: %v (original error: %v)", rbErr, err)
		}
		return fmt.Errorf("failed to prepare statement: %v", err)
	}

	app.lastCommit = time.Now()
	return nil
}