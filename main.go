package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
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

const (
	workerCount      = 4    // 並列処理のワーカー数
	batchSize        = 100  // SQLiteへの一括挿入サイズ
	progressInterval = 1    // 進捗表示の間隔（秒）
	logInterval      = 5    // 詳細ログの出力間隔（秒）
	filesPerLog      = 1000 // ログを出力するファイル処理数の閾値
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

	// filesPerLog個ごとにログを出力
	if processedFiles%filesPerLog == 0 {
		ps.mutex.Lock()
		now := time.Now()
		elapsed := now.Sub(ps.startTime)
		filesPerSecond := float64(processedFiles) / elapsed.Seconds()
		totalFiles := atomic.LoadInt64(&ps.totalFiles)
		percentComplete := float64(processedFiles) / float64(totalFiles) * 100

		// 現在のディレクトリを取得（長すぎる場合は省略）
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

func parseFlags() (string, string, string, bool) {
	storageName := flag.String("storage", "", "Storage name identifier")
	rootDir := flag.String("root", "", "Root directory to scan")
	dbPath := flag.String("db", "", "SQLite database path")
	skipHash := flag.Bool("skip-hash", false, "Skip SHA256 hash calculation")
	flag.Parse()

	if *storageName == "" || *rootDir == "" || *dbPath == "" {
		log.Fatal("All arguments are required: -storage, -root, -db")
	}

	return *storageName, *rootDir, *dbPath, *skipHash
}

func countFilesAndSize(rootDir string, stats *ProgressStats) error {
	log.Println("Counting files and calculating total size...")
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		atomic.AddInt64(&stats.totalFiles, 1)
		atomic.AddInt64(&stats.totalBytes, info.Size())
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to count files: %v", err)
	}

	log.Printf("Found %d files, total size: %.2f GB",
		stats.totalFiles,
		float64(stats.totalBytes)/(1024*1024*1024))
	return nil
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

func processFiles(rootDir string, skipHash bool, stats *ProgressStats, metadataChan chan<- FileMetadata, wg *sync.WaitGroup) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Warning: Error accessing path %s: %v", path, err)
			return nil
		}

		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			log.Printf("Warning: Cannot get relative path for %s: %v", path, err)
			return nil
		}

		wg.Add(1)
		go func(path string, info os.FileInfo, relPath string) {
			metadata := collectMetadata(path, info, relPath, skipHash)
			metadataChan <- metadata
			stats.LogProgress(path)
		}(path, info, relPath)

		return nil
	})
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

func main() {
	storageName, rootDir, dbPath, skipHash := parseFlags()

	// データベース接続とセットアップ
	db, err := setupDatabase(dbPath)
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}
	defer db.Close()

	// 進捗状況の管理
	stats := NewProgressStats()
	
	if err := countFilesAndSize(rootDir, stats); err != nil {
		log.Fatal(err)
	}

	log.Printf("Starting metadata collection from root directory: %s", rootDir)
	
	// トランザクション開始
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	dumpID, err := createDumpEntry(tx, storageName)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	// メタデータ処理用のチャネル
	metadataChan := make(chan FileMetadata, workerCount*2)
	var wg sync.WaitGroup

	// SQLite挿入用のプリペアドステートメント
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

	// データベース書き込み用ゴルーチン
	go func() {
		for metadata := range metadataChan {
			_, err := stmt.Exec(
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
			wg.Done()
		}
	}()

	if err := processFiles(rootDir, skipHash, stats, metadataChan, &wg); err != nil {
		tx.Rollback()
		log.Fatalf("Failed during file walk: %v", err)
	}

	wg.Wait()
	close(metadataChan)

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	logFinalStatistics(stats)
}

func setupDatabase(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// パフォーマンス設定
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -2000000;
		PRAGMA temp_store = MEMORY;
		PRAGMA busy_timeout = 5000;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to set database pragmas: %v", err)
	}

	return db, nil
}

func formatFileMode(mode os.FileMode) string {
    // 基本的なパーミッションビットをマスク
    permBits := mode & os.ModePerm
    
    // ファイルタイプをチェック
    var typeChar string
    switch {
    case mode&os.ModeDir != 0:
        typeChar = "d"
    case mode&os.ModeSymlink != 0:
        typeChar = "l"
    default:
        typeChar = "-"
    }
    
    // パーミッションを文字列に変換
    result := typeChar
    
    // オーナーのパーミッション
    result += map[bool]string{true: "r", false: "-"}[(permBits&0400) != 0]
    result += map[bool]string{true: "w", false: "-"}[(permBits&0200) != 0]
    result += map[bool]string{true: "x", false: "-"}[(permBits&0100) != 0]
    
    // グループのパーミッション
    result += map[bool]string{true: "r", false: "-"}[(permBits&040) != 0]
    result += map[bool]string{true: "w", false: "-"}[(permBits&020) != 0]
    result += map[bool]string{true: "x", false: "-"}[(permBits&010) != 0]
    
    // その他のパーミッション
    result += map[bool]string{true: "r", false: "-"}[(permBits&04) != 0]
    result += map[bool]string{true: "w", false: "-"}[(permBits&02) != 0]
    result += map[bool]string{true: "x", false: "-"}[(permBits&01) != 0]
    
    return result
}

func collectMetadata(path string, info os.FileInfo, relPath string, skipHash bool) FileMetadata {
    stat := info.Sys().(*syscall.Stat_t)
    
    // macOS固有の属性を取得
    var isSystem, isArchive bool
    finderInfo := make([]byte, 32) // FinderInfoは32バイト
    _, err := unix.Getxattr(path, "com.apple.FinderInfo", finderInfo)
    if err == nil {
        // Finder flagsの解析
        isSystem = finderInfo[8]&uint8(0x04) != 0  // kIsSystemFileBit
        isArchive = finderInfo[8]&uint8(0x20) != 0 // kIsArchiveBit
    }

    // SHA256ハッシュの計算（ファイルの場合のみ）
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
        FileMode:           formatFileMode(info.Mode()), // 新しい関数を使用
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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
