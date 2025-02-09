package api

import (
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"
)

type Handler struct {
	db *sql.DB
}

type DirectoryEntry struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	IsDir     bool   `json:"is_dir"`
	Size      int64  `json:"size"`
	Modified  int64  `json:"modified"`
	FileCount int    `json:"file_count,omitempty"`
	DirCount  int    `json:"dir_count,omitempty"`
}

type FileMetadata struct {
	Name          string  `json:"name"`
	Path          string  `json:"path"`
	Size          int64   `json:"size"`
	Created       int64   `json:"created"`
	Modified      int64   `json:"modified"`
	Accessed      int64   `json:"accessed"`
	IsDir         bool    `json:"is_dir"`
	IsSymlink     bool    `json:"is_symlink"`
	IsHidden      bool    `json:"is_hidden"`
	FileMode      string  `json:"file_mode"`
	FileExtension string  `json:"file_extension"`
	SHA256        *string `json:"sha256,omitempty"`
}

// Extension statistics response types
type ExtensionStats struct {
	Extension    string  `json:"extension"`
	FileCount    int     `json:"file_count"`
	TotalSize    int64   `json:"total_size"`
	AverageSize  float64 `json:"average_size"`
	MinSize      int64   `json:"min_size"`
	MaxSize      int64   `json:"max_size"`
	LastModified int64   `json:"last_modified"`
}

type DirectoryStats struct {
	Path           string `json:"path"`
	ParentPath     string `json:"parent_path,omitempty"`
	Depth          int    `json:"depth"`
	FileCount      int    `json:"file_count"`
	DirectoryCount int    `json:"dir_count"`
	TotalSize      int64  `json:"total_size"`
	LastModified   int64  `json:"last_modified"`
}

type StorageComparison struct {
	Path        string `json:"path"`
	OldSize     int64  `json:"old_size"`
	NewSize     int64  `json:"new_size"`
	SizeDiff    int64  `json:"size_diff"`
	OldModified int64  `json:"old_modified"`
	NewModified int64  `json:"new_modified"`
	Status      string `json:"status"` // "added", "deleted", "modified", "unchanged"
}

type CacheStatus struct {
	CacheType    string `json:"cache_type"`
	TotalEntries int    `json:"total_entries"`
	StaleEntries int    `json:"stale_entries"`
	LastUpdate   int64  `json:"last_update"`
	Status       string `json:"status"` // "REBUILD", "UPDATE", "FRESH"
}

func NewHandler(db *sql.DB) *Handler {
	return &Handler{db: db}
}

// ListDirectory returns the contents of a directory
func (h *Handler) ListDirectory(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		path = "/"
	}

	// Get the latest dump_id
	var dumpID int64
	err := h.db.QueryRow(`
		SELECT dump_id FROM dumps 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get latest dump")
	}

	rows, err := h.db.Query(`
		SELECT 
			file_name,
			file_path,
			is_directory,
			size_bytes,
			modification_time_utc,
			(
				SELECT COUNT(*) 
				FROM file_metadata f2 
				WHERE f2.directory LIKE f1.file_path || '/%'
				AND f2.dump_id = f1.dump_id
				AND f2.is_file = 1
			) as file_count,
			(
				SELECT COUNT(*) 
				FROM file_metadata f2 
				WHERE f2.directory LIKE f1.file_path || '/%'
				AND f2.dump_id = f1.dump_id
				AND f2.is_directory = 1
			) as dir_count
		FROM file_metadata f1
		WHERE directory = ? AND dump_id = ?
	`, path, dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to query directory")
	}
	defer rows.Close()

	entries := []DirectoryEntry{}
	for rows.Next() {
		var entry DirectoryEntry
		var fileCount, dirCount int
		err := rows.Scan(
			&entry.Name,
			&entry.Path,
			&entry.IsDir,
			&entry.Size,
			&entry.Modified,
			&fileCount,
			&dirCount,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		if entry.IsDir {
			entry.FileCount = fileCount
			entry.DirCount = dirCount
		}
		entries = append(entries, entry)
	}

	return c.JSON(http.StatusOK, entries)
}

// GetFileMetadata returns detailed metadata for a specific file
func (h *Handler) GetFileMetadata(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Path parameter is required")
	}

	// Get the latest dump_id
	var dumpID int64
	err := h.db.QueryRow(`
		SELECT dump_id FROM dumps 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get latest dump")
	}

	var metadata FileMetadata
	var sha256Str sql.NullString
	err = h.db.QueryRow(`
		SELECT 
			file_name,
			file_path,
			size_bytes,
			creation_time_utc,
			modification_time_utc,
			access_time_utc,
			is_directory,
			is_symlink,
			is_hidden,
			file_mode,
			file_extension,
			sha256
		FROM file_metadata
		WHERE file_path = ? AND dump_id = ?
	`, path, dumpID).Scan(
		&metadata.Name,
		&metadata.Path,
		&metadata.Size,
		&metadata.Created,
		&metadata.Modified,
		&metadata.Accessed,
		&metadata.IsDir,
		&metadata.IsSymlink,
		&metadata.IsHidden,
		&metadata.FileMode,
		&metadata.FileExtension,
		&sha256Str,
	)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, "File not found")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get file metadata")
	}

	if sha256Str.Valid {
		metadata.SHA256 = &sha256Str.String
	}

	return c.JSON(http.StatusOK, metadata)
}

// GetStats returns statistics about the filesystem
func (h *Handler) GetStats(c echo.Context) error {
	var stats struct {
		TotalFiles       int64  `json:"total_files"`
		TotalDirectories int64  `json:"total_directories"`
		TotalSize        int64  `json:"total_size"`
		UniqueExtensions int    `json:"unique_extensions"`
		LatestModified   int64  `json:"latest_modified"`
		StorageName      string `json:"storage_name"`
		ScanTime         int64  `json:"scan_time"`
	}

	err := h.db.QueryRow(`
		SELECT 
			d.file_count,
			d.directory_count,
			d.total_size_bytes,
			d.storage_name,
			d.processed_at,
			(SELECT COUNT(DISTINCT file_extension) FROM file_metadata WHERE dump_id = d.dump_id) as ext_count,
			(SELECT MAX(modification_time_utc) FROM file_metadata WHERE dump_id = d.dump_id) as latest_mod
		FROM dumps d
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(
		&stats.TotalFiles,
		&stats.TotalDirectories,
		&stats.TotalSize,
		&stats.StorageName,
		&stats.ScanTime,
		&stats.UniqueExtensions,
		&stats.LatestModified,
	)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get statistics")
	}

	return c.JSON(http.StatusOK, stats)
}

// SearchFiles searches for files matching the given pattern
func (h *Handler) SearchFiles(c echo.Context) error {
	pattern := c.QueryParam("pattern")
	if pattern == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Pattern parameter is required")
	}

	// Get the latest dump_id
	var dumpID int64
	err := h.db.QueryRow(`
		SELECT dump_id FROM dumps 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get latest dump")
	}

	rows, err := h.db.Query(`
		SELECT 
			file_name,
			file_path,
			is_directory,
			size_bytes,
			modification_time_utc
		FROM file_metadata
		WHERE dump_id = ? AND (
			file_name LIKE ? OR
			file_path LIKE ?
		)
		LIMIT 100
	`, dumpID, "%"+pattern+"%", "%"+pattern+"%")
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to search files")
	}
	defer rows.Close()

	entries := []DirectoryEntry{}
	for rows.Next() {
		var entry DirectoryEntry
		err := rows.Scan(
			&entry.Name,
			&entry.Path,
			&entry.IsDir,
			&entry.Size,
			&entry.Modified,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		entries = append(entries, entry)
	}

	return c.JSON(http.StatusOK, entries)
}

// GetExtensionStats returns statistics about file extensions
func (h *Handler) GetExtensionStats(c echo.Context) error {
	limit := c.QueryParam("limit")
	if limit == "" {
		limit = "10"
	}

	// Get the latest dump_id
	var dumpID int64
	err := h.db.QueryRow(`
		SELECT dump_id FROM dumps 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get latest dump")
	}

	rows, err := h.db.Query(`
		SELECT 
			file_extension,
			file_count,
			total_size_bytes,
			avg_size_bytes,
			min_size_bytes,
			max_size_bytes,
			last_modified
		FROM extension_cache
		WHERE dump_id = ? AND is_stale = 0
		ORDER BY file_count DESC
		LIMIT ?
	`, dumpID, limit)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get extension statistics")
	}
	defer rows.Close()

	stats := []ExtensionStats{}
	for rows.Next() {
		var stat ExtensionStats
		err := rows.Scan(
			&stat.Extension,
			&stat.FileCount,
			&stat.TotalSize,
			&stat.AverageSize,
			&stat.MinSize,
			&stat.MaxSize,
			&stat.LastModified,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		stats = append(stats, stat)
	}

	return c.JSON(http.StatusOK, stats)
}

// GetDirectoryTree returns the directory tree structure with statistics
func (h *Handler) GetDirectoryTree(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		path = "/"
	}

	depth := c.QueryParam("depth")
	if depth == "" {
		depth = "1"
	}

	// Get the latest dump_id
	var dumpID int64
	err := h.db.QueryRow(`
		SELECT dump_id FROM dumps 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get latest dump")
	}

	rows, err := h.db.Query(`
		SELECT 
			directory,
			parent_directory,
			depth,
			file_count,
			dir_count,
			total_size_bytes,
			last_modified
		FROM directory_cache
		WHERE dump_id = ? 
		AND directory LIKE ? || '%'
		AND depth <= (SELECT depth FROM directory_cache WHERE directory = ? AND dump_id = ?) + ?
		AND is_stale = 0
		ORDER BY depth, directory
	`, dumpID, path, path, dumpID, depth)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get directory tree")
	}
	defer rows.Close()

	dirs := []DirectoryStats{}
	for rows.Next() {
		var dir DirectoryStats
		var parentPath sql.NullString
		err := rows.Scan(
			&dir.Path,
			&parentPath,
			&dir.Depth,
			&dir.FileCount,
			&dir.DirectoryCount,
			&dir.TotalSize,
			&dir.LastModified,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		if parentPath.Valid {
			dir.ParentPath = parentPath.String
		}
		dirs = append(dirs, dir)
	}

	return c.JSON(http.StatusOK, dirs)
}

// AdvancedSearch performs advanced file search with multiple criteria
func (h *Handler) AdvancedSearch(c echo.Context) error {
	// Get the latest dump_id
	var dumpID int64
	err := h.db.QueryRow(`
		SELECT dump_id FROM dumps 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get latest dump")
	}

	// Build query conditions
	query := `
		SELECT 
			file_name,
			file_path,
			is_directory,
			size_bytes,
			modification_time_utc
		FROM file_metadata
		WHERE dump_id = ?
	`
	params := []interface{}{dumpID}

	// Size filters
	if minSize := c.QueryParam("min_size"); minSize != "" {
		query += " AND size_bytes >= ?"
		params = append(params, minSize)
	}
	if maxSize := c.QueryParam("max_size"); maxSize != "" {
		query += " AND size_bytes <= ?"
		params = append(params, maxSize)
	}

	// Time filters
	if modifiedAfter := c.QueryParam("modified_after"); modifiedAfter != "" {
		query += " AND modification_time_utc >= ?"
		params = append(params, modifiedAfter)
	}
	if modifiedBefore := c.QueryParam("modified_before"); modifiedBefore != "" {
		query += " AND modification_time_utc <= ?"
		params = append(params, modifiedBefore)
	}

	// File attributes
	if isHidden := c.QueryParam("is_hidden"); isHidden != "" {
		query += " AND is_hidden = ?"
		params = append(params, isHidden)
	}
	if isSymlink := c.QueryParam("is_symlink"); isSymlink != "" {
		query += " AND is_symlink = ?"
		params = append(params, isSymlink)
	}
	if extension := c.QueryParam("extension"); extension != "" {
		query += " AND file_extension = ?"
		params = append(params, extension)
	}

	query += " LIMIT 100"

	rows, err := h.db.Query(query, params...)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to search files")
	}
	defer rows.Close()

	entries := []DirectoryEntry{}
	for rows.Next() {
		var entry DirectoryEntry
		err := rows.Scan(
			&entry.Name,
			&entry.Path,
			&entry.IsDir,
			&entry.Size,
			&entry.Modified,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		entries = append(entries, entry)
	}

	return c.JSON(http.StatusOK, entries)
}

// CompareDumps compares two dumps and returns changes
func (h *Handler) CompareDumps(c echo.Context) error {
	oldStorage := c.QueryParam("old_storage")
	newStorage := c.QueryParam("new_storage")
	if oldStorage == "" || newStorage == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Both old_storage and new_storage parameters are required")
	}

	rows, err := h.db.Query(`
		WITH old_dump AS (
			SELECT dump_id, file_path, size_bytes, modification_time_utc
			FROM file_metadata
			WHERE dump_id = (
				SELECT dump_id FROM dumps 
				WHERE storage_name = ? 
				ORDER BY created_at DESC 
				LIMIT 1
			)
		),
		new_dump AS (
			SELECT dump_id, file_path, size_bytes, modification_time_utc
			FROM file_metadata
			WHERE dump_id = (
				SELECT dump_id FROM dumps 
				WHERE storage_name = ? 
				ORDER BY created_at DESC 
				LIMIT 1
			)
		)
		SELECT 
			COALESCE(n.file_path, o.file_path) as file_path,
			o.size_bytes as old_size,
			n.size_bytes as new_size,
			o.modification_time_utc as old_modified,
			n.modification_time_utc as new_modified,
			CASE
				WHEN o.file_path IS NULL THEN 'added'
				WHEN n.file_path IS NULL THEN 'deleted'
				WHEN n.size_bytes != o.size_bytes OR n.modification_time_utc != o.modification_time_utc THEN 'modified'
				ELSE 'unchanged'
			END as status
		FROM new_dump n
		FULL OUTER JOIN old_dump o ON n.file_path = o.file_path
		WHERE status != 'unchanged'
		LIMIT 1000
	`, oldStorage, newStorage)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to compare dumps")
	}
	defer rows.Close()

	changes := []StorageComparison{}
	for rows.Next() {
		var change StorageComparison
		var oldSize, newSize, oldModified, newModified sql.NullInt64
		err := rows.Scan(
			&change.Path,
			&oldSize,
			&newSize,
			&oldModified,
			&newModified,
			&change.Status,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}

		change.OldSize = oldSize.Int64
		change.NewSize = newSize.Int64
		change.OldModified = oldModified.Int64
		change.NewModified = newModified.Int64
		change.SizeDiff = change.NewSize - change.OldSize

		changes = append(changes, change)
	}

	return c.JSON(http.StatusOK, changes)
}

// GetCacheStatus returns the current status of caches
func (h *Handler) GetCacheStatus(c echo.Context) error {
	// Get the latest dump_id
	var dumpID int64
	err := h.db.QueryRow(`
		SELECT dump_id FROM dumps 
		ORDER BY created_at DESC 
		LIMIT 1
	`).Scan(&dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get latest dump")
	}

	rows, err := h.db.Query(`
		SELECT * FROM cache_status
		WHERE dump_id = ?
	`, dumpID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get cache status")
	}
	defer rows.Close()

	statuses := []CacheStatus{}
	for rows.Next() {
		var status CacheStatus
		var dumpID int64
		err := rows.Scan(
			&status.CacheType,
			&dumpID,
			&status.TotalEntries,
			&status.StaleEntries,
			&status.LastUpdate,
			&status.Status,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		statuses = append(statuses, status)
	}

	return c.JSON(http.StatusOK, statuses)
}
