# FSSQ (File System SQLite Query)

FSSQ is a high-performance tool for analyzing file system metadata through SQLite. It recursively scans directories, stores metadata in a SQLite database, and enables powerful SQL-based analysis of your file system with minimal memory footprint.

## Features

- Blazing-fast recursive directory scanning with parallel workers
- Efficient SQLite storage with write-ahead logging
- Comprehensive file metadata collection:
  - Core attributes (name, path, size, timestamps)
  - File properties (directory, symlink, hidden, system flags)
  - Content verification (SHA256 hashing)
- Advanced SQL querying capabilities
- Real-time progress monitoring
- Database merge functionality

## Installation

```bash
go install github.com/nrtkbb/fssq@latest
```

## Quick Start

```bash
# Scan a directory
fssq scan -db data.db -root /path/to/scan -storage "backup2024"

# Merge two databases
fssq merge -source db1.db -dest db2.db

# Query examples
sqlite3 data.db "SELECT * FROM filesystem_stats"
```

## Command Reference

### Scan Command

```bash
fssq scan [flags]

Flags:
  -db string       Database file path (required)
  -root string     Directory to scan (required)
  -storage string  Storage name identifier (required)
  -workers int     Number of parallel workers (default 4)
  -skip-hash       Skip SHA256 calculation
```

### Merge Command

```bash
fssq merge [flags]

Flags:
  -source string   Source database file (required)
  -dest string     Destination database file (required)
```

## Example Queries

```sql
-- Find duplicate files
SELECT sha256, COUNT(*) as copies, SUM(size_bytes) as total_size
FROM file_metadata
WHERE sha256 IS NOT NULL
GROUP BY sha256
HAVING COUNT(*) > 1
ORDER BY total_size DESC;

-- Storage usage by extension
SELECT file_extension, 
       COUNT(*) as count,
       SUM(size_bytes) / 1024.0 / 1024.0 as size_mb
FROM file_metadata
GROUP BY file_extension
ORDER BY size_mb DESC;

-- Recent changes
SELECT file_path, 
       datetime(modification_time_utc, 'unixepoch') as modified
FROM file_metadata
ORDER BY modification_time_utc DESC
LIMIT 20;
```

## License

MIT

For more details, visit: https://github.com/nrtkbb/fssq
