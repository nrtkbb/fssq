package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// SearchFiles searches for files matching the given pattern
func (h *Handler) SearchFiles(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "SearchFiles")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	pattern := c.QueryParam("pattern")
	if pattern == "" {
		err := echo.NewHTTPError(http.StatusBadRequest, "Pattern parameter is required")
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.String("pattern", pattern))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	var total int
	err = h.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM file_metadata
		WHERE dump_id = ? AND (
			file_name LIKE ? OR
			file_path LIKE ?
		)
	`, dumpID, "%"+pattern+"%", "%"+pattern+"%").Scan(&total)
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
			modification_time_utc
		FROM file_metadata
		WHERE dump_id = ? AND (
			file_name LIKE ? OR
			file_path LIKE ?
		)
		LIMIT ? OFFSET ?
	`, dumpID, "%"+pattern+"%", "%"+pattern+"%", perPage, offset)
	if err != nil {
		span.RecordError(err)
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
			span.RecordError(err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		entries = append(entries, entry)
	}

	return c.JSON(http.StatusOK, NewPaginatedResponse(c, entries, page, perPage, total))
}

// AdvancedSearch performs advanced file search with multiple criteria
func (h *Handler) AdvancedSearch(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "AdvancedSearch")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	if minSize := c.QueryParam("min_size"); minSize != "" {
		span.SetAttributes(attribute.String("min_size", minSize))
	}
	if maxSize := c.QueryParam("max_size"); maxSize != "" {
		span.SetAttributes(attribute.String("max_size", maxSize))
	}
	if extension := c.QueryParam("extension"); extension != "" {
		span.SetAttributes(attribute.String("extension", extension))
	}

	baseQuery := `
		FROM file_metadata
		WHERE dump_id = ?
	`
	params := []interface{}{dumpID}

	if minSize := c.QueryParam("min_size"); minSize != "" {
		baseQuery += " AND size_bytes >= ?"
		params = append(params, minSize)
	}
	if maxSize := c.QueryParam("max_size"); maxSize != "" {
		baseQuery += " AND size_bytes <= ?"
		params = append(params, maxSize)
	}

	if modifiedAfter := c.QueryParam("modified_after"); modifiedAfter != "" {
		baseQuery += " AND modification_time_utc >= ?"
		params = append(params, modifiedAfter)
	}
	if modifiedBefore := c.QueryParam("modified_before"); modifiedBefore != "" {
		baseQuery += " AND modification_time_utc <= ?"
		params = append(params, modifiedBefore)
	}

	if isHidden := c.QueryParam("is_hidden"); isHidden != "" {
		baseQuery += " AND is_hidden = ?"
		params = append(params, isHidden)
	}
	if isSymlink := c.QueryParam("is_symlink"); isSymlink != "" {
		baseQuery += " AND is_symlink = ?"
		params = append(params, isSymlink)
	}
	if extension := c.QueryParam("extension"); extension != "" {
		baseQuery += " AND file_extension = ?"
		params = append(params, extension)
	}

	var total int
	countParams := make([]interface{}, len(params))
	copy(countParams, params)
	err = h.db.QueryRowContext(ctx, "SELECT COUNT(*) "+baseQuery, countParams...).Scan(&total)
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

	params = append(params, perPage, offset)
	query := `
		SELECT 
			file_name,
			file_path,
			is_directory,
			size_bytes,
			modification_time_utc
	` + baseQuery + " LIMIT ? OFFSET ?"

	rows, err := h.db.QueryContext(ctx, query, params...)
	if err != nil {
		span.RecordError(err)
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
			span.RecordError(err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		entries = append(entries, entry)
	}

	return c.JSON(http.StatusOK, NewPaginatedResponse(c, entries, page, perPage, total))
}
