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
	totalFiles     int64
	processedFiles int64
	totalBytes     int64
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
}

const (
	workerCount      = 4    // Number of parallel workers
	batchSize        = 100  // Batch size for SQLite inserts
	progressInterval = 1    // Progress display interval (seconds)
	logInterval      = 5    // Detailed log output interval (seconds)
	filesPerLog      = 1000 // Threshold for file processing count before logging
)

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
		totalFiles := atomic.LoadInt64(&ps.totalFiles)
		percentComplete := float64(processedFiles) / float64(totalFiles) * 100

		// Get current directory (truncate if too long)
		currentDir := filepath.Dir(currentPath)
		if len(currentDir) > 50 {
			currentDir = "..." + currentDir[len(currentDir)-47:]
		}

		log.Printf("[Progress] Processed %d/%d files (%.1f%%) at %.1f files/sec - Current dir: %s",
			processedFiles, totalFiles, percentComplete, filesPerSecond, currentDir)

		ps.lastLogTime = now
		ps.mutex.Unlock()
	}
}

func (app *AppContext) parformCleanup() {
	app.cleanup.Do(func() {
		log.Println("Starting graceful shutdown...")

		// Complete processing of pending metadata before closing channel
		if app.metadataChan != nil {
			close(app.metadataChan)
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

			// WALファイルとSHMファイルの存在確認とクリーンアップ
			var dbPath string
			err := app.db.QueryRow("PRAGMA database_list").Scan(nil, &dbPath, nil)
			if err != nil {
				log.Printf("Warning: データベースパスの取得に失敗: %v", err)
			} else {
				walPath := dbPath + "-wal"
				shmPath := dbPath + "-shm"
				
				if _, err := os.Stat(walPath); err == nil {
					log.Printf("WALファイルが残存しています: %s", walPath)
				}
				if _, err := os.Stat(shmPath); err == nil {
					log.Printf("SHMファイルが残存しています: %s", shmPath)
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

	// 強制終了フラグ
	var forceQuit atomic.Bool
	
	go func() {
		for sig := range sigChan {
			log.Printf("Received signal: %v", sig)
			if forceQuit.Load() {
				log.Println("強制終了します...")
				os.Exit(1)
			}
			
			forceQuit.Store(true)
			log.Println("もう一度Ctrl+Cを押すと強制終了します。通常の終了を待つ場合はお待ちください...")
			cancel() // Cancel context to notify goroutines
			
			// 5秒後にforceQuitフラグをリセット
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

	// Count files with context awareness
	if err := countFilesAndSize(rootDir, stats, ctx); err != nil {
		if err == context.Canceled {
			log.Println("File counting cancelled by user")
			return
		}
		log.Fatal(err)
	}

	// Check if we were cancelled during counting
	if ctx.Err() != nil {
		return
	}

	log.Printf("Starting metadata collection from root directory: %s", rootDir)

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	app.tx = tx

	// amend モードの場合、トランザクションの状態を確認
	if amend {
		var inTransaction int
		err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master LIMIT 1").Scan(&inTransaction)
		if err != nil {
			log.Fatalf("データベースの状態確認に失敗: %v", err)
		}
		
		// 安全のため、明示的にロールバックを実行
		if _, err := db.Exec("ROLLBACK"); err != nil {
			log.Printf("警告: ロールバックの実行中にエラー: %v", err)
		}
		log.Println("データベースの状態をリセットしました")
	}

	// 既存のdump_idを取得（amend モード用）
	var dumpID int64
	if amend {
		err = db.QueryRow(`
			SELECT dump_id FROM dumps 
			WHERE storage_name = ? 
			ORDER BY created_at DESC 
			LIMIT 1
		`, storageName).Scan(&dumpID)
		if err == sql.ErrNoRows {
			log.Printf("警告: 指定されたストレージ名 %s の以前のダンプが見つかりません。新規作成します。", storageName)
			amend = false
		} else if err != nil {
			log.Fatalf("以前のダンプの検索中にエラーが発生: %v", err)
		} else {
			log.Printf("dump_id %d の処理を再開します", dumpID)
		}
	}

	// 新規ダンプの作成（amend モードでない場合）
	if !amend {
		dumpID, err = createDumpEntry(tx, storageName)
		if err != nil {
			tx.Rollback()
			log.Fatal(err)
		}
	}

	// 処理済みのファイルパスを取得（amend モード用）
	processedPaths := make(map[string]struct{})
	if amend {
		rows, err := db.Query(`
			SELECT file_path 
			FROM file_metadata_template 
			WHERE dump_id = ?
		`, dumpID)
		if err != nil {
			log.Fatalf("処理済みファイルの取得中にエラー: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				log.Fatalf("処理済みファイルの読み取り中にエラー: %v", err)
			}
			processedPaths[path] = struct{}{}
		}
		log.Printf("%d 個の処理済みファイルをスキップします", len(processedPaths))
	}

	// Metadata processing channel
	metadataChan := make(chan FileMetadata, workerCount*2)
	app.metadataChan = metadataChan

	// SQLite insert prepared statement
	stmt, err := tx.Prepare(`
		INSERT INTO file_metadata_template (
			dump_id, file_path, file_name, directory, size_bytes,
			creation_time_utc, modification_time_utc, access_time_utc,
			file_mode, is_directory, is_file, is_symlink,
			is_hidden, is_system, is_archive, is_readonly,
			file_extension, sha256
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Database write goroutine
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		
		// 初期トランザクションのセットアップ
		if err := app.refreshTransaction(); err != nil {
			log.Printf("初期トランザクションのセットアップに失敗: %v", err)
			app.cancel()
			return
		}

		const commitInterval = 30 * time.Second // 30秒ごとにコミット
		processedSinceCommit := 0
		const commitThreshold = 10000          // または10000レコードごとにコミット

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
					log.Printf("メタデータの挿入エラー %s: %v", metadata.FilePath, err)
				}
				atomic.AddInt64(&stats.processedBytes, metadata.SizeBytes)
				
				processedSinceCommit++

				// 一定時間経過またはレコード数の閾値を超えた場合にコミット
				if time.Since(app.lastCommit) >= commitInterval || processedSinceCommit >= commitThreshold {
					if err := app.refreshTransaction(); err != nil {
						log.Printf("トランザクションの更新に失敗: %v", err)
						app.cancel()
						return
					}
					log.Printf("トランザクションをコミットしました（%d レコード処理済み）", processedSinceCommit)
					processedSinceCommit = 0
				}
			}
		}

		// 最終的な未コミットのデータをコミット
		if processedSinceCommit > 0 {
			if err := app.refreshTransaction(); err != nil {
				log.Printf("最終トランザクションのコミットに失敗: %v", err)
			}
		}
	}()

	// File system scan
	semaphore := make(chan struct{}, workerCount)
	scanComplete := make(chan struct{}) // 追加：スキャン完了を通知するチャネル
	
	// ファイルシステムスキャンを別のゴルーチンで実行
	go func() {
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

				// amend モードの場合、既に処理済みのパスをスキップ
				if amend {
					if _, exists := processedPaths[relPath]; exists {
						return nil
					}
				}

				semaphore <- struct{}{} // セマフォを獲得
				app.wg.Add(1)
				go func(path string, info os.FileInfo, relPath string) {
					defer app.wg.Done()
					defer func() { <-semaphore }() // セマフォを解放
					metadata := collectMetadata(path, info, relPath, skipHash)
					select {
					case <-ctx.Done():
						return
					case metadataChan <- metadata:
						stats.LogProgress(path)
					}
				}(path, info, relPath)

				return nil
			}
		})

		// スキャン完了後の処理
		close(scanComplete) // スキャン完了を通知

		// セマフォを空にする
		for i := 0; i < workerCount; i++ {
			semaphore <- struct{}{}
		}

		if err != nil && err != filepath.SkipAll {
			log.Printf("Error during file walk: %v", err)
			app.cancel()
		}
	}()

	// スキャン完了とすべてのワーカーの終了を待つ
	<-scanComplete
	app.wg.Wait()

	// メタデータチャネルを閉じる
	close(metadataChan)

	// データベースワーカーの完了を待つ
	app.wg.Wait()

	if err != nil && err != filepath.SkipAll {
		app.parformCleanup()
		log.Fatalf("Failed during file walk: %v", err)
	}

	if ctx.Err() == nil {
		logFinalStatistics(stats)
	}
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

func countFilesAndSize(rootDir string, stats *ProgressStats, ctx context.Context) error {
	log.Println("Counting files and calculating total size...")
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return filepath.SkipAll
		default:
			if err != nil {
				return nil
			}
			atomic.AddInt64(&stats.totalFiles, 1)
			atomic.AddInt64(&stats.totalBytes, info.Size())
			return nil
		}
	})
	if err != nil && err != filepath.SkipAll {
		return fmt.Errorf("failed to count files: %v", err)
	}

	// Only log the results if we weren't cancelled
	if ctx.Err() == nil {
		log.Printf("Found %d files, total size: %.2f GB",
			stats.totalFiles,
			float64(stats.totalBytes)/(1024*1024*1024))
	}
	return ctx.Err()
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
	// データベースを開く前にWALのリカバリを確認
	if _, err := os.Stat(dbPath + "-wal"); err == nil {
		log.Println("WALファイルが見つかりました。リカバリを実行します...")
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// WALモード設定の前にチェックポイントを実行
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		log.Printf("Warning: WALチェックポイントの実行に失敗: %v", err)
	} else {
		log.Println("WALチェックポイントが正常に実行されました")
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

	// データベースの整合性チェック
	var integrityCheck string
	err = db.QueryRow("PRAGMA quick_check").Scan(&integrityCheck)
	if err != nil {
		return nil, fmt.Errorf("データベースの整合性チェックに失敗: %v", err)
	}
	if integrityCheck != "ok" {
		return nil, fmt.Errorf("データベースの整合性に問題があります: %s", integrityCheck)
	}

	return db, nil
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
		INSERT INTO file_metadata_template (
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
				query := `SELECT * FROM file_metadata_template WHERE dump_id = ? LIMIT ? OFFSET ?`
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
		// 既存のステートメントをクローズ
		if app.stmt != nil {
			app.stmt.Close()
		}
		
		// 現在のトランザクションをコミット
		if err := app.tx.Commit(); err != nil {
			return fmt.Errorf("トランザクションのコミットに失敗: %v", err)
		}
	}

	// 新しいトランザクションを開始
	var err error
	app.tx, err = app.db.Begin()
	if err != nil {
		return fmt.Errorf("新しいトランザクションの開始に失敗: %v", err)
	}

	// 新しいPrepared Statementを作成
	app.stmt, err = app.tx.Prepare(`
		INSERT INTO file_metadata_template (
			dump_id, file_path, file_name, directory, size_bytes,
			creation_time_utc, modification_time_utc, access_time_utc,
			file_mode, is_directory, is_file, is_symlink,
			is_hidden, is_system, is_archive, is_readonly,
			file_extension, sha256
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("ステートメントの準備に失敗: %v", err)
	}

	app.lastCommit = time.Now()
	return nil
}