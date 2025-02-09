-- Drop existing view
DROP VIEW IF EXISTS filesystem_stats;

-- Drop existing indexes
DROP INDEX IF EXISTS idx_file_metadata_file_name;
DROP INDEX IF EXISTS idx_file_metadata_directory;
DROP INDEX IF EXISTS idx_file_metadata_extension;
DROP INDEX IF EXISTS idx_file_metadata_sha256;
DROP INDEX IF EXISTS idx_file_metadata_modification;

-- Rename table back
ALTER TABLE file_metadata RENAME TO file_metadata_template;

-- Recreate indexes with original table name
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

-- Recreate view with original table name
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