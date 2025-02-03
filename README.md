# fssq (File System SQL)

fssq is a high-performance file system metadata collector that allows you to query your file system using SQL. It recursively scans directories, stores file metadata in a SQLite database, and enables SQL-based file system analysis. The tool is specifically optimized for macOS environments.

## Features

- SQL-powered file system analysis
- Fast recursive directory scanning with parallel processing
- Comprehensive metadata collection:
  - Basic file information (name, path, size, timestamps)
  - File attributes (directory, symlink, hidden, system, archive, readonly)
  - SHA256 hash calculation (optional)
  - macOS-specific attributes (Finder flags)
- Real-time progress monitoring
- Optimized SQLite storage with transaction support
- Efficient memory usage with configurable batch processing

## Prerequisites

- Go 1.19 or later
- SQLite 3
- macOS operating system

## Installation

1. Clone the repository:

```bash
git clone https://github.com/nrtkbb/fssq.git
cd fssq
```

2. Install the required dependencies:

```bash
go mod tidy
```

3. Build the program:

```bash
go build
```

4. Initialize the SQLite database:

```bash
# Create a new SQLite database file
sqlite3 filedata.db < schema.sql
```

## Usage

### Basic Command

```bash
./fssq -storage <storage-name> -root <root-directory> -db <database-path>
```

### Command Line Arguments

- `-storage`: Identifier for this scanning session (required)
- `-root`: Root directory to scan (required)
- `-db`: Path to SQLite database file (required)
- `-skip-hash`: Skip SHA256 hash calculation (optional, default: false)

### Example

```bash
./fssq -storage "mac-home" -root "/Users/username/Documents" -db "./filedata.db"
```

## Performance Settings

The program includes configurable performance parameters:

```go
const (
    workerCount      = 4     // Number of parallel workers
    batchSize        = 100   // Batch size for SQLite insertions
    progressInterval = 1     // Progress display interval (seconds)
    logInterval      = 5     // Detailed log interval (seconds)
    filesPerLog      = 1000  // Number of files between log entries
)
```

## Database Schema

The SQLite database uses several optimized tables and indices. The schema is defined in `schema.sql` and includes:

### Core Tables

- `dumps`: Stores scanning session information
- `file_metadata_template`: Stores detailed file metadata

### Cache Tables

- `directory_cache`: Directory structure cache
- `extension_cache`: File extension statistics cache

### Performance Optimizations

The schema includes:

- Optimized indices for common queries
- Triggers for automatic statistics updates
- Views for system monitoring and optimization
- Performance-tuned PRAGMA settings

For detailed schema information, refer to `schema.sql` in the repository.

## Example Output

```
2024/02/03 10:00:00 Found 50000 files, total size: 25.50 GB
2024/02/03 10:00:00 Starting metadata collection from root directory: /Users/username/Documents
2024/02/03 10:00:01 [Progress] Processed 1000/50000 files (2.0%) at 1000.5 files/sec - Current dir: .../Documents/Projects
...
2024/02/03 10:10:00 Successfully completed metadata collection in 10m0s
2024/02/03 10:10:00 Final statistics:
2024/02/03 10:10:00 - Processed 50000 files
2024/02/03 10:10:00 - Total size: 25.50 GB
2024/02/03 10:10:00 - Average speed: 83.3 files/sec
```

## Example SQL Queries

After collecting metadata, you can run SQL queries like:

```sql
-- Find largest files
SELECT file_path, size_bytes
FROM file_metadata_template
WHERE dump_id = ?
ORDER BY size_bytes DESC
LIMIT 10;

-- Count files by extension
SELECT file_extension, COUNT(*) as count, SUM(size_bytes) as total_size
FROM file_metadata_template
WHERE dump_id = ?
GROUP BY file_extension
ORDER BY count DESC;

-- Find recently modified files
SELECT file_path, datetime(modification_time_utc, 'unixepoch') as modified
FROM file_metadata_template
WHERE dump_id = ?
ORDER BY modification_time_utc DESC
LIMIT 20;

-- Get directory statistics
SELECT directory, COUNT(*) as file_count, SUM(size_bytes) as total_size
FROM file_metadata_template
WHERE dump_id = ?
GROUP BY directory
ORDER BY total_size DESC;
```

## Technical Notes

### Database Optimization

- Uses WAL journal mode for better concurrency
- Memory-optimized temporary storage
- Large cache size configuration (2GB)
- Memory-mapped I/O support (up to 30GB)
- Prepared statements for efficient insertions
- Transaction-based processing

### Limitations

- Currently optimized for macOS only
- Memory usage scales with worker count
- SHA256 calculation may impact performance
- Large cache size requirements (2GB+)

## License

MIT License
