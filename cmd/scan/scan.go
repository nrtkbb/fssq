package scan

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

	"github.com/google/subcommands"
	"github.com/nrtkbb/fssq/app"
	"github.com/nrtkbb/fssq/db"
	"github.com/nrtkbb/fssq/models"
	"github.com/nrtkbb/fssq/scanner"
)

type Command struct {
	dbPath      string
	rootDir     string
	storageName string
	amend       bool
	skipHash    bool
}

type FileMetadata struct {
	FilePath            string
	FileName            string
	Directory           string
	SizeBytes           int64
	CreationTimeUTC     int64
	ModificationTimeUTC int64
	AccessTimeUTC       int64
	FileMode            string
	IsDirectory         bool
	IsFile              bool
	IsSymlink           bool
	IsHidden            bool
	IsSystem            bool
	IsArchive           bool
	IsReadonly          bool
	FileExtension       string
	SHA256              *string
}

func (*Command) Name() string     { return "scan" }
func (*Command) Synopsis() string { return "Scan directory and store metadata in SQLite" }
func (*Command) Usage() string {
	return `scan -db <database> -root <directory> -storage <name> [-amend] [-skip-hash]:
  Scan directory recursively and store file metadata in SQLite database.
`
}

func (c *Command) SetFlags(f *flag.FlagSet) {
	f.StringVar(&c.dbPath, "db", "", "database file path (required)")
	f.StringVar(&c.rootDir, "root", "", "directory to scan (required)")
	f.StringVar(&c.storageName, "storage", "", "storage name identifier (required)")
	f.BoolVar(&c.skipHash, "skip-hash", false, "skip SHA256 calculation")
	f.BoolVar(&c.amend, "amend", false, "amend existing dump")
}

func (c *Command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if c.dbPath == "" || c.rootDir == "" || c.storageName == "" {
		f.Usage()
		return subcommands.ExitUsageError
	}

	appCtx := app.NewAppContext(ctx)
	defer appCtx.PerformCleanup()

	setupSignalHandling(appCtx)

	// Set up database connection
	database, err := db.SetupDatabase(c.dbPath)
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}
	appCtx.DB = database

	// Get dump information
	dumpInfo := db.CreateOrResumeDump(appCtx.DB, c.storageName, c.amend)
	dumpID := dumpInfo.DumpID
	amend := dumpInfo.IsAmend
	processedPaths := dumpInfo.ProcessedPaths

	// Metadata processing channel setup
	metadataChan := make(chan models.FileMetadata, 100)
	appCtx.MetadataChan = metadataChan

	// Database write goroutine
	const batchSize = 1000
	var currentBatch int
	var mu sync.Mutex

	createNewTransaction := func() error {
		mu.Lock()
		defer mu.Unlock()

		if appCtx.Tx != nil {
			if err := appCtx.Tx.Commit(); err != nil {
				return err
			}
		}

		tx, err := appCtx.DB.Begin()
		if err != nil {
			return err
		}
		appCtx.Tx = tx

		stmt, err := tx.Prepare(`
			INSERT INTO file_metadata (
				dump_id, file_path, file_name, directory, size_bytes,
				creation_time_utc, modification_time_utc, access_time_utc,
				file_mode, is_directory, is_file, is_symlink,
				is_hidden, is_system, is_archive, is_readonly,
				file_extension, sha256
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}

		if appCtx.Stmt != nil {
			appCtx.Stmt.Close()
		}
		appCtx.Stmt = stmt
		return nil
	}

	// Initial transaction
	if err := createNewTransaction(); err != nil {
		log.Fatalf("Failed to create initial transaction: %v", err)
	}

	appCtx.Wg.Add(1)
	go func() {
		defer appCtx.Wg.Done()
		defer func() {
			if appCtx.Stmt != nil {
				appCtx.Stmt.Close()
			}
		}()

		for metadata := range metadataChan {
			select {
			case <-appCtx.Context.Done():
				return
			default:
				mu.Lock()
				_, err := appCtx.Stmt.Exec(
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
				mu.Unlock()

				if err != nil {
					log.Printf("Error inserting metadata for %s: %v", metadata.FilePath, err)
					continue
				}

				atomic.AddInt64(&appCtx.Stats.ProcessedBytes, metadata.SizeBytes)

				currentBatch++
				if currentBatch >= batchSize {
					if err := createNewTransaction(); err != nil {
						log.Printf("Error creating new transaction: %v", err)
						appCtx.Cancel()
						return
					}
					currentBatch = 0
				}
			}
		}

		// Final commit for the last batch
		mu.Lock()
		defer mu.Unlock()
		if appCtx.Tx != nil {
			if err := appCtx.Tx.Commit(); err != nil {
				log.Printf("Error committing final batch: %v", err)
			}
			appCtx.Tx = nil
		}
	}()

	// File scanning
	scanComplete := make(chan struct{})
	var scanWg sync.WaitGroup
	scanWg.Add(1)
	go func() {
		defer scanWg.Done()
		err = filepath.Walk(c.rootDir, func(path string, info os.FileInfo, err error) error {
			select {
			case <-appCtx.Context.Done():
				return filepath.SkipAll
			default:
				if err != nil {
					log.Printf("Warning: Error accessing path %s: %v", path, err)
					return nil
				}

				relPath, err := filepath.Rel(c.rootDir, path)
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

				metadata := scanner.CollectMetadata(path, info, relPath, c.skipHash)
				atomic.AddInt64(&appCtx.Stats.ProcessedFiles, 1)

				select {
				case <-appCtx.Context.Done():
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

	appCtx.Wg.Wait()

	// Log final statistics
	elapsed := time.Since(appCtx.Stats.StartTime)
	processedFiles := atomic.LoadInt64(&appCtx.Stats.ProcessedFiles)
	processedBytes := atomic.LoadInt64(&appCtx.Stats.ProcessedBytes)

	log.Printf("Scan completed in %v", elapsed)
	log.Printf("Processed %d files (%.2f GB)",
		processedFiles,
		float64(processedBytes)/(1024*1024*1024),
	)

	return subcommands.ExitSuccess
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
