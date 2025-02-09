-- 6. Performance Settings --
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = FULL;
PRAGMA cache_size = -2000; -- Reset to default
PRAGMA mmap_size = 0;      -- Disable memory mapping
PRAGMA temp_store = FILE;
PRAGMA auto_vacuum = NONE;

-- 5. Views --
DROP VIEW IF EXISTS filesystem_stats;
DROP VIEW IF EXISTS partition_optimization;
DROP VIEW IF EXISTS cache_status;

-- 4. Triggers --
DROP TRIGGER IF EXISTS mark_extension_cache_stale;
DROP TRIGGER IF EXISTS mark_directory_cache_stale;
DROP TRIGGER IF EXISTS update_dumps_stats;

-- 3. Indexes --
DROP INDEX IF EXISTS idx_extension_cache_stale;
DROP INDEX IF EXISTS idx_directory_cache_stale;
DROP INDEX IF EXISTS idx_directory_cache_parent;

DROP INDEX IF EXISTS idx_file_metadata_modification;
DROP INDEX IF EXISTS idx_file_metadata_sha256;
DROP INDEX IF EXISTS idx_file_metadata_extension;
DROP INDEX IF EXISTS idx_file_metadata_directory;
DROP INDEX IF EXISTS idx_file_metadata_file_name;

-- 2. Cache Tables --
DROP TABLE IF EXISTS extension_cache;
DROP TABLE IF EXISTS directory_cache;

-- 1. Core Tables --
DROP TABLE IF EXISTS partition_registry;
DROP TABLE IF EXISTS file_metadata_template;
DROP TABLE IF EXISTS dumps;

PRAGMA foreign_keys = OFF; 