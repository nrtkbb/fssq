package api

import (
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// ListDirectory returns the contents of a directory
func (h *Handler) ListDirectory(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "ListDirectory")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	path := c.QueryParam("path")
	if path == "" {
		path = "."
	}
	span.SetAttributes(attribute.String("path", path))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	var total int
	err = h.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM file_metadata f1
		WHERE directory = ? AND dump_id = ?
	`, path, dumpID).Scan(&total)
	if err != nil {
		span.RecordError(err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get total count")
	}

	page, err := h.getPageFromQuery(c, total)
	if err != nil {
		span.RecordError(err)
		return err
	}
	perPage := 100
	offset := (page - 1) * perPage

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
		LIMIT ? OFFSET ?
	`, path, dumpID, perPage, offset)
	if err != nil {
		span.RecordError(err)
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
			span.RecordError(err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		if entry.IsDir {
			entry.FileCount = fileCount
			entry.DirCount = dirCount
		}
		entries = append(entries, entry)
	}

	return c.JSON(http.StatusOK, NewPaginatedResponse(c, entries, page, perPage, total))
}

// GetFileMetadata returns detailed metadata for a specific file
func (h *Handler) GetFileMetadata(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "GetFileMetadata")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	path := c.QueryParam("path")
	if path == "" {
		err := echo.NewHTTPError(http.StatusBadRequest, "Path parameter is required")
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.String("path", path))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	var metadata FileMetadata
	var sha256Str sql.NullString
	err = h.db.QueryRowContext(ctx, `
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
		span.RecordError(err)
		return echo.NewHTTPError(http.StatusNotFound, "File not found")
	}
	if err != nil {
		span.RecordError(err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get file metadata")
	}

	if sha256Str.Valid {
		metadata.SHA256 = &sha256Str.String
	}

	return c.JSON(http.StatusOK, metadata)
}
