package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nrtkbb/fssq/app"
	"github.com/nrtkbb/fssq/db"
	"github.com/nrtkbb/fssq/models"
	"github.com/nrtkbb/fssq/scanner"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCtx := app.NewAppContext(ctx, cancel)
	defer appCtx.PerformCleanup()

	setupSignalHandling(appCtx)

	storageName, rootDir, dbPath, skipHash, mergeSource, amend, migrateOnly := parseFlags()

	if migrateOnly {
		handleMigrateOnly(dbPath)
		return
	}

	if mergeSource != "" {
		handleMerge(ctx, mergeSource, dbPath)
		return
	}

	// Set up database connection
	database, err := db.SetupDatabase(dbPath)
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}
	appCtx.DB = database

	// Get dump information
	dumpInfo := db.CreateOrResumeDump(appCtx.DB, storageName, amend)
	dumpID := dumpInfo.DumpID
	amend = dumpInfo.IsAmend // Update amend flag
	processedPaths := dumpInfo.ProcessedPaths

	// Start transaction for file metadata
	tx, err := appCtx.DB.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	appCtx.Tx = tx

	// Metadata processing channel setup
	metadataChan := make(chan models.FileMetadata, 100)
	appCtx.MetadataChan = metadataChan

	// Database write goroutine
	appCtx.Wg.Add(1)
	go func() {
		defer appCtx.Wg.Done()

		stmt, err := appCtx.Tx.Prepare(`
			INSERT INTO file_metadata (
				dump_id, file_path, file_name, directory, size_bytes,
				creation_time_utc, modification_time_utc, access_time_utc,
				file_mode, is_directory, is_file, is_symlink,
				is_hidden, is_system, is_archive, is_readonly,
				file_extension, sha256
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			log.Printf("Failed to prepare statement: %v", err)
			appCtx.Cancel()
			return
		}
		appCtx.Stmt = stmt
		defer stmt.Close()

		for metadata := range metadataChan {
			select {
			case <-ctx.Done():
				return
			default:
				_, err := stmt.Exec(
					dumpID,
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
					log.Printf("Error inserting metadata for %s: %v", metadata.FilePath, err)
				}
				atomic.AddInt64(&appCtx.Stats.ProcessedBytes, metadata.SizeBytes)
			}
		}
	}()

	// File scanning
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
				}

				metadata := scanner.CollectMetadata(path, info, relPath, skipHash)
				atomic.AddInt64(&appCtx.Stats.ProcessedFiles, 1)

				select {
				case <-ctx.Done():
					return filepath.SkipAll
				case metadataChan <- metadata:
				}

				return nil
			}
		})

		close(scanComplete)
	}()

	<-scanComplete
	scanWg.Wait()

	if !appCtx.ChannelClosed {
		close(appCtx.MetadataChan)
		appCtx.ChannelClosed = true
	}

	if err != nil && err != filepath.SkipAll {
		log.Fatalf("Failed during file walk: %v", err)
	}

	// Final commit
	if err := appCtx.Tx.Commit(); err != nil {
		log.Fatalf("Failed to commit final transaction: %v", err)
	}

	// Log final statistics
	elapsed := time.Since(appCtx.Stats.StartTime)
	processedFiles := atomic.LoadInt64(&appCtx.Stats.ProcessedFiles)
	processedBytes := atomic.LoadInt64(&appCtx.Stats.ProcessedBytes)

	log.Printf("Scan completed in %v", elapsed)
	log.Printf("Processed %d files (%.2f GB)",
		processedFiles,
		float64(processedBytes)/(1024*1024*1024),
	)
}

func parseFlags() (string, string, string, bool, string, bool, bool) {
	storageName := flag.String("storage", "", "Storage name identifier")
	rootDir := flag.String("root", "", "Root directory to scan")
	dbPath := flag.String("db", "", "SQLite database path")
	skipHash := flag.Bool("skip-hash", false, "Skip SHA256 hash calculation")
	mergeSource := flag.String("merge", "", "Source database to merge from")
	amend := flag.Bool("amend", false, "Continue from last incomplete dump")
	migrateOnly := flag.Bool("migrate", false, "Only run database migrations")
	flag.Parse()

	// マイグレーションモードの場合はdbPathのみ必須
	if *migrateOnly {
		if *dbPath == "" {
			log.Fatal("Database path (-db) is required for migration")
		}
		// マイグレーションモードでは他のパラメータは無視
		return "", "", *dbPath, false, "", false, *migrateOnly
	}

	// マージモードのバリデーション
	if *mergeSource != "" {
		if *dbPath == "" {
			log.Fatal("Destination database (-db) is required for merge operation")
		}
		if *mergeSource == *dbPath {
			log.Fatal("Source and destination databases must be different")
		}
		return "", "", *dbPath, *skipHash, *mergeSource, false, false
	}

	// 通常モードのバリデーション
	if *storageName == "" || *rootDir == "" || *dbPath == "" {
		log.Fatal("All arguments are required: -storage, -root, -db")
	}

	return *storageName, *rootDir, *dbPath, *skipHash, *mergeSource, *amend, *migrateOnly
}

func setupSignalHandling(app *app.AppContext) {
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
			app.Cancel() // Cancel context to notify goroutines

			// Reset forceQuit flag after 5 seconds
			go func() {
				time.Sleep(5 * time.Second)
				forceQuit.Store(false)
			}()
		}
	}()
}

func handleMigrateOnly(dbPath string) {
	log.Printf("Running database migrations for %s...", dbPath)
	if err := db.RunMigrations(dbPath); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}
	log.Println("Database migrations completed successfully")
}

func handleMerge(ctx context.Context, mergeSource, dbPath string) {
	log.Printf("Merging database %s into %s...", mergeSource, dbPath)
	if err := db.MergeDatabase(ctx, mergeSource, dbPath); err != nil {
		if err == context.Canceled {
			log.Println("Database merge cancelled by user")
			return
		}
		log.Fatalf("Failed to merge database: %v", err)
	}
	log.Println("Database merge completed successfully")
}
