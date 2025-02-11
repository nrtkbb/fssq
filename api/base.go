package api

import (
	"database/sql"
	"net/http"
	"reflect"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Handler struct {
	db *sql.DB
}

func NewHandler(db *sql.DB) *Handler {
	return &Handler{db: db}
}

// NewPaginatedResponse creates a new paginated response and adds telemetry
func NewPaginatedResponse(c echo.Context, data interface{}, page int, perPage int, total int) *PaginatedResponse {
	totalPages := (total + perPage - 1) / perPage
	hasNext := page < totalPages

	// Use span from request context
	if span := trace.SpanFromContext(c.Request().Context()); span != nil {
		span.SetAttributes(
			attribute.Bool("has_next_page", hasNext),
			attribute.Int("response_items", reflect.ValueOf(data).Len()),
		)
	}

	return &PaginatedResponse{
		Data:       data,
		Page:       page,
		PerPage:    perPage,
		Total:      total,
		TotalPages: totalPages,
		HasNext:    hasNext,
	}
}

// getDumpIDFromQuery gets and validates dump_id from query parameters
func (h *Handler) getDumpIDFromQuery(c echo.Context) (int64, error) {
	dumpIDStr := c.QueryParam("dump_id")
	if dumpIDStr == "" {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "dump_id parameter is required")
	}

	dumpID, err := strconv.ParseInt(dumpIDStr, 10, 64)
	if err != nil {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Invalid dump_id")
	}

	return dumpID, nil
}

// getPageFromQuery gets and validates page number from query parameters
func (h *Handler) getPageFromQuery(c echo.Context, total int) (int, error) {
	pageStr := c.QueryParam("page")
	if pageStr == "" {
		return 1, nil
	}

	page, err := strconv.Atoi(pageStr)
	if err != nil {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Invalid page number")
	}

	if page < 1 {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Page number must be greater than 0")
	}

	perPage := 100
	totalPages := (total + perPage - 1) / perPage
	if page > totalPages {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Page number exceeds total pages. Total pages: "+strconv.Itoa(totalPages))
	}

	// Add OpenTelemetry attributes for pagination using the parent span from request context
	span := trace.SpanFromContext(c.Request().Context())
	span.SetAttributes(
		attribute.Int("page", page),
		attribute.Int("per_page", perPage),
		attribute.Int("total", total),
		attribute.Int("total_pages", totalPages),
	)

	return page, nil
}
