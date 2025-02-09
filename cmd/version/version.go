package version

import (
	"context"
	"flag"
	"fmt"

	"github.com/google/subcommands"
)

var (
	// These variables are set by goreleaser
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

type Command struct{}

func (*Command) Name() string     { return "version" }
func (*Command) Synopsis() string { return "Print version information" }
func (*Command) Usage() string {
	return `version:
  Print version, build commit, and build date information.
`
}

func (c *Command) SetFlags(f *flag.FlagSet) {}

func (c *Command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fmt.Printf("fssq version %s\n", Version)
	fmt.Printf("commit: %s\n", Commit)
	fmt.Printf("built: %s\n", Date)
	return subcommands.ExitSuccess
}
