package merge

import (
	"context"
	"flag"

	"github.com/google/subcommands"
	"github.com/nrtkbb/fssq/db"
)

type Command struct {
	sourceDB string
	destDB   string
}

func (*Command) Name() string     { return "merge" }
func (*Command) Synopsis() string { return "Merge two SQLite databases" }
func (*Command) Usage() string {
	return `merge -source <source.db> -dest <dest.db>:
  Merge source database into destination database.
`
}

func (c *Command) SetFlags(f *flag.FlagSet) {
	f.StringVar(&c.sourceDB, "source", "", "source database file (required)")
	f.StringVar(&c.destDB, "dest", "", "destination database file (required)")
}

func (c *Command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if c.sourceDB == "" || c.destDB == "" {
		f.Usage()
		return subcommands.ExitUsageError
	}

	if err := db.MergeDatabase(ctx, c.sourceDB, c.destDB); err != nil {
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}
