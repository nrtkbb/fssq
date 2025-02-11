package api

import (
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// CompareDumps compares two dumps and returns changes
func (h *Handler) CompareDumps(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "CompareDumps")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	oldStorage := c.QueryParam("old_storage")
	newStorage := c.QueryParam("new_storage")
	if oldStorage == "" || newStorage == "" {
		err := echo.NewHTTPError(http.StatusBadRequest, "Both old_storage and new_storage parameters are required")
		span.RecordError(err)
		return err
	}
	span.SetAttributes(
		attribute.String("old_storage", oldStorage),
		attribute.String("new_storage", newStorage),
	)

	var total int
	err := h.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM comparison
	`, oldStorage, newStorage).Scan(&total)
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

	baseQuery := `
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
		),
		comparison AS (
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
		)`

	rows, err := h.db.QueryContext(ctx, baseQuery+`
		SELECT * FROM comparison
		LIMIT ? OFFSET ?
	`, oldStorage, newStorage, perPage, offset)
	if err != nil {
		span.RecordError(err)
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
			span.RecordError(err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}

		change.OldSize = oldSize.Int64
		change.NewSize = newSize.Int64
		change.OldModified = oldModified.Int64
		change.NewModified = newModified.Int64
		change.SizeDiff = change.NewSize - change.OldSize

		changes = append(changes, change)
	}

	return c.JSON(http.StatusOK, NewPaginatedResponse(c, changes, page, perPage, total))
}

// GetCacheStatus returns the current status of caches
func (h *Handler) GetCacheStatus(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("api/handlers")
	ctx, span := tracer.Start(ctx, "GetCacheStatus")
	defer span.End()

	c.SetRequest(c.Request().WithContext(ctx))

	dumpID, err := h.getDumpIDFromQuery(c)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("dump_id", dumpID))

	rows, err := h.db.QueryContext(ctx, `
		SELECT * FROM cache_status
		WHERE dump_id = ?
	`, dumpID)
	if err != nil {
		span.RecordError(err)
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
			span.RecordError(err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to scan row")
		}
		statuses = append(statuses, status)
	}

	return c.JSON(http.StatusOK, statuses)
}
