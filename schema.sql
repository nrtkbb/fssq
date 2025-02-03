-- File: schema.sql
-- Description: Complete database schema for file metadata system

PRAGMA foreign_keys = ON;
PRAGMA encoding = 'UTF-8';

-- 1. Core Tables --

-- Dumps テーブル：ファイル情報の管理
CREATE TABLE IF NOT EXISTS dumps (
    dump_id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_name TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    processed_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    file_count INTEGER NOT NULL DEFAULT 0,
    directory_count INTEGER NOT NULL DEFAULT 0,
    total_size_bytes INTEGER NOT NULL DEFAULT 0
);

-- ファイルメタデータのテンプレートテーブル
CREATE TABLE IF NOT EXISTS file_metadata_template (
    dump_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    directory TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    creation_time_utc INTEGER NOT NULL,
    modification_time_utc INTEGER NOT NULL,
    access_time_utc INTEGER NOT NULL,
    file_mode TEXT NOT NULL,
    is_directory INTEGER NOT NULL,
    is_file INTEGER NOT NULL,
    is_symlink INTEGER NOT NULL,
    is_hidden INTEGER NOT NULL,
    is_system INTEGER NOT NULL,
    is_archive INTEGER NOT NULL,
    is_readonly INTEGER NOT NULL,
    file_extension TEXT NOT NULL,
    sha256 TEXT,
    
    PRIMARY KEY (dump_id, file_path),
    FOREIGN KEY (dump_id) REFERENCES dumps(dump_id),
    
    CHECK (is_directory IN (0, 1)),
    CHECK (is_file IN (0, 1)),
    CHECK (is_symlink IN (0, 1)),
    CHECK (is_hidden IN (0, 1)),
    CHECK (is_system IN (0, 1)),
    CHECK (is_archive IN (0, 1)),
    CHECK (is_readonly IN (0, 1)),
    CHECK (size_bytes >= 0),
    CHECK (creation_time_utc >= 0),
    CHECK (modification_time_utc >= 0),
    CHECK (access_time_utc >= 0),
    CHECK (file_mode GLOB '[drwx-]{10}')
) WITHOUT ROWID;

-- パーティション管理テーブル
CREATE TABLE IF NOT EXISTS partition_registry (
    partition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dump_id_start INTEGER NOT NULL,
    dump_id_end INTEGER NOT NULL,
    table_name TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    record_count INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    
    UNIQUE (dump_id_start, dump_id_end),
    CHECK (dump_id_start <= dump_id_end),
    CHECK (is_active IN (0, 1))
);

-- 2. Cache Tables --

-- ディレクトリ構造キャッシュ
CREATE TABLE IF NOT EXISTS directory_cache (
    dump_id INTEGER NOT NULL,
    directory TEXT NOT NULL,
    parent_directory TEXT,
    depth INTEGER NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    dir_count INTEGER NOT NULL DEFAULT 0,
    total_size_bytes INTEGER NOT NULL DEFAULT 0,
    last_modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    is_stale INTEGER NOT NULL DEFAULT 0,
    
    PRIMARY KEY (dump_id, directory),
    FOREIGN KEY (dump_id) REFERENCES dumps(dump_id),
    CHECK (depth >= 0),
    CHECK (is_stale IN (0, 1))
) WITHOUT ROWID;

-- 拡張子統計キャッシュ
CREATE TABLE IF NOT EXISTS extension_cache (
    dump_id INTEGER NOT NULL,
    file_extension TEXT NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size_bytes INTEGER NOT NULL DEFAULT 0,
    avg_size_bytes REAL NOT NULL DEFAULT 0,
    min_size_bytes INTEGER NOT NULL DEFAULT 0,
    max_size_bytes INTEGER NOT NULL DEFAULT 0,
    last_modified INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    is_stale INTEGER NOT NULL DEFAULT 0,
    
    PRIMARY KEY (dump_id, file_extension),
    FOREIGN KEY (dump_id) REFERENCES dumps(dump_id),
    CHECK (is_stale IN (0, 1))
) WITHOUT ROWID;

-- 3. Indexes --

-- ファイルメタデータのインデックス
CREATE INDEX IF NOT EXISTS idx_file_metadata_file_name 
    ON file_metadata_template(file_name);

CREATE INDEX IF NOT EXISTS idx_file_metadata_directory 
    ON file_metadata_template(directory);

CREATE INDEX IF NOT EXISTS idx_file_metadata_extension 
    ON file_metadata_template(file_extension);

CREATE INDEX IF NOT EXISTS idx_file_metadata_sha256 
    ON file_metadata_template(sha256);

CREATE INDEX IF NOT EXISTS idx_file_metadata_modification 
    ON file_metadata_template(modification_time_utc);

-- キャッシュテーブルのインデックス
CREATE INDEX IF NOT EXISTS idx_directory_cache_parent 
    ON directory_cache(parent_directory);

CREATE INDEX IF NOT EXISTS idx_directory_cache_stale 
    ON directory_cache(is_stale) 
    WHERE is_stale = 1;

CREATE INDEX IF NOT EXISTS idx_extension_cache_stale 
    ON extension_cache(is_stale) 
    WHERE is_stale = 1;

-- 4. Triggers --

-- Dumps統計更新トリガー
CREATE TRIGGER IF NOT EXISTS update_dumps_stats
AFTER INSERT ON file_metadata_template
BEGIN
    UPDATE dumps 
    SET 
        file_count = file_count + CASE WHEN NEW.is_file = 1 THEN 1 ELSE 0 END,
        directory_count = directory_count + CASE WHEN NEW.is_directory = 1 THEN 1 ELSE 0 END,
        total_size_bytes = total_size_bytes + CASE WHEN NEW.is_file = 1 THEN NEW.size_bytes ELSE 0 END,
        processed_at = strftime('%s', 'now')
    WHERE dump_id = NEW.dump_id;
END;

-- ディレクトリキャッシュ更新トリガー
CREATE TRIGGER IF NOT EXISTS mark_directory_cache_stale
AFTER INSERT ON file_metadata_template
BEGIN
    UPDATE directory_cache
    SET is_stale = 1
    WHERE dump_id = NEW.dump_id 
    AND (directory = NEW.directory OR NEW.directory LIKE directory || '/%');
END;

-- 拡張子キャッシュ更新トリガー
CREATE TRIGGER IF NOT EXISTS mark_extension_cache_stale
AFTER INSERT ON file_metadata_template
WHEN NEW.is_file = 1
BEGIN
    UPDATE extension_cache
    SET is_stale = 1
    WHERE dump_id = NEW.dump_id 
    AND file_extension = NEW.file_extension;
END;

-- 5. Views --

-- キャッシュ状態監視ビュー
CREATE VIEW IF NOT EXISTS cache_status AS
SELECT 
    'directory' as cache_type,
    dump_id,
    COUNT(*) as total_entries,
    SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) as stale_entries,
    MAX(last_modified) as last_update,
    CASE 
        WHEN SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) > 1000 THEN 'REBUILD'
        WHEN SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) > 0 THEN 'UPDATE'
        ELSE 'FRESH'
    END as status
FROM directory_cache
GROUP BY dump_id

UNION ALL

SELECT 
    'extension' as cache_type,
    dump_id,
    COUNT(*) as total_entries,
    SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) as stale_entries,
    MAX(last_modified) as last_update,
    CASE 
        WHEN SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) > 1000 THEN 'REBUILD'
        WHEN SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) > 0 THEN 'UPDATE'
        ELSE 'FRESH'
    END as status
FROM extension_cache
GROUP BY dump_id;

-- パーティション最適化提案ビュー
CREATE VIEW IF NOT EXISTS partition_optimization AS
SELECT 
    pr.table_name,
    pr.dump_id_start,
    pr.dump_id_end,
    pr.record_count,
    CASE 
        WHEN pr.record_count < 100000 THEN 'MERGE'
        WHEN pr.record_count > 1000000 THEN 'SPLIT'
        ELSE 'OPTIMAL'
    END as suggestion,
    CASE 
        WHEN pr.record_count < 100000 
        THEN (
            SELECT table_name 
            FROM partition_registry 
            WHERE dump_id_end = pr.dump_id_start - 1 
            OR dump_id_start = pr.dump_id_end + 1
            LIMIT 1
        )
        ELSE NULL
    END as merge_with
FROM partition_registry pr
WHERE pr.is_active = 1;

-- ファイルシステム統計ビュー
CREATE VIEW IF NOT EXISTS filesystem_stats AS
SELECT 
    d.dump_id,
    d.storage_name,
    d.file_count,
    d.directory_count,
    d.total_size_bytes,
    COUNT(DISTINCT m.directory) as unique_directories,
    COUNT(DISTINCT m.file_extension) as unique_extensions,
    MAX(m.modification_time_utc) as latest_modification,
    MIN(m.creation_time_utc) as earliest_creation
FROM dumps d
LEFT JOIN file_metadata_template m ON d.dump_id = m.dump_id
GROUP BY d.dump_id, d.storage_name;

-- 6. Performance Settings --

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -2000000; -- 約2GB のキャッシュ
PRAGMA mmap_size = 30000000000; -- 30GB までのメモリマッピング
PRAGMA temp_store = MEMORY;
PRAGMA auto_vacuum = INCREMENTAL;
