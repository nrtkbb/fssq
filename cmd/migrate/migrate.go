package migrate

import (
	"context"
	"flag"
	"log"

	"github.com/google/subcommands"
	"github.com/nrtkbb/fssq/db"
)

type Command struct {
	dbPath string
}

func (*Command) Name() string     { return "migrate" }
func (*Command) Synopsis() string { return "Run database migrations" }
func (*Command) Usage() string {
	return `migrate -db <database>:
  Run database migrations on the specified SQLite database.
`
}

func (c *Command) SetFlags(f *flag.FlagSet) {
	f.StringVar(&c.dbPath, "db", "", "database file path (required)")
}

func (c *Command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if c.dbPath == "" {
		f.Usage()
		return subcommands.ExitUsageError
	}

	log.Printf("Running database migrations for %s...", c.dbPath)
	if err := db.RunMigrations(c.dbPath); err != nil {
		log.Printf("Failed to run migrations: %v", err)
		return subcommands.ExitFailure
	}
	log.Println("Database migrations completed successfully")

	return subcommands.ExitSuccess
}
