package api

import (
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// GetStats returns statistics about the filesystem
func (h *Handler) GetStats(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "GetStats")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	var stats struct {
		TotalFiles       int64  `json:"total_files"`
		TotalDirectories int64  `json:"total_directories"`
		TotalSize        int64  `json:"total_size"`
		UniqueExtensions int    `json:"unique_extensions"`
		LatestModified   int64  `json:"latest_modified"`
		StorageName      string `json:"storage_name"`
		ScanTime         int64  `json:"scan_time"`
	}

	err = h.db.QueryRowContext(ctx, `
		SELECT 
			d.file_count,
			d.directory_count,
			d.total_size_bytes,
			d.storage_name,
			d.processed_at,
			(SELECT COUNT(DISTINCT file_extension) FROM file_metadata WHERE dump_id = d.dump_id) as ext_count,
			(SELECT MAX(modification_time_utc) FROM file_metadata WHERE dump_id = d.dump_id) as latest_mod
		FROM dumps d
		WHERE d.dump_id = ?
	`, dumpID).Scan(
		&stats.TotalFiles,
		&stats.TotalDirectories,
		&stats.TotalSize,
		&stats.StorageName,
		&stats.ScanTime,
		&stats.UniqueExtensions,
		&stats.LatestModified,
	)
	if err != nil {
		span.RecordError(err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get statistics")
	}

	return c.JSON(http.StatusOK, stats)
}

// GetExtensionStats returns statistics about file extensions
func (h *Handler) GetExtensionStats(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "GetExtensionStats")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	limit := c.QueryParam("limit")
	if limit == "" {
		limit = "10"
	}
	span.SetAttributes(attribute.String("limit", limit))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	rows, err := h.db.QueryContext(ctx, `
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
		span.RecordError(err)
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
			span.RecordError(err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		stats = append(stats, stat)
	}

	return c.JSON(http.StatusOK, stats)
}

// GetDirectoryTree returns the directory tree structure with statistics
func (h *Handler) GetDirectoryTree(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "GetDirectoryTree")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	path := c.QueryParam("path")
	if path == "" {
		path = "."
	}
	span.SetAttributes(attribute.String("path", path))

	depth := c.QueryParam("depth")
	if depth == "" {
		depth = "1"
	}
	span.SetAttributes(attribute.String("depth", depth))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	rows, err := h.db.QueryContext(ctx, `
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
		span.RecordError(err)
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
			span.RecordError(err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		if parentPath.Valid {
			dir.ParentPath = parentPath.String
		}
		dirs = append(dirs, dir)
	}

	return c.JSON(http.StatusOK, dirs)
}
