package serve

import (
	"context"
	"flag"
	"log"
	"net/http"

	"github.com/google/subcommands"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/nrtkbb/fssq/api"
	"github.com/nrtkbb/fssq/db"
)

type Command struct {
	dbPath string
	port   string
}

func (*Command) Name() string     { return "serve" }
func (*Command) Synopsis() string { return "Start HTTP server to serve file metadata API" }
func (*Command) Usage() string {
	return `serve -db <database> [-port <port>]:
  Start an HTTP server that provides REST API access to the file metadata database.
`
}

func (c *Command) SetFlags(f *flag.FlagSet) {
	f.StringVar(&c.dbPath, "db", "", "database file path (required)")
	f.StringVar(&c.port, "port", "8080", "port to listen on")
}

func (c *Command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if c.dbPath == "" {
		f.Usage()
		return subcommands.ExitUsageError
	}

	// Set up database connection
	database, err := db.SetupDatabase(c.dbPath)
	if err != nil {
		log.Printf("Failed to setup database: %v", err)
		return subcommands.ExitFailure
	}
	defer database.Close()

	// Create Echo instance
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	// Create handler
	h := api.NewHandler(database)

	// Routes
	e.GET("/api/ls", h.ListDirectory)
	e.GET("/api/stat", h.GetFileMetadata)
	e.GET("/api/search", h.SearchFiles)
	e.GET("/api/stats", h.GetStats)

	// New API routes
	e.GET("/api/extensions", h.GetExtensionStats)
	e.GET("/api/tree", h.GetDirectoryTree)
	e.GET("/api/search/advanced", h.AdvancedSearch)
	e.GET("/api/compare", h.CompareDumps)
	e.GET("/api/cache/status", h.GetCacheStatus)

	// Start server
	log.Printf("Starting server on port %s...", c.port)
	if err := e.Start(":" + c.port); err != nil && err != http.ErrServerClosed {
		log.Printf("Failed to start server: %v", err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}
