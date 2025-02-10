package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"
	"github.com/nrtkbb/fssq/cmd/merge"
	"github.com/nrtkbb/fssq/cmd/migrate"
	"github.com/nrtkbb/fssq/cmd/scan"
	"github.com/nrtkbb/fssq/cmd/serve"
	"github.com/nrtkbb/fssq/cmd/testdata"
	"github.com/nrtkbb/fssq/cmd/version"
)

func main() {
	// Register subcommands
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&scan.Command{}, "")
	subcommands.Register(&merge.Command{}, "")
	subcommands.Register(&migrate.Command{}, "")
	subcommands.Register(&serve.Command{}, "")
	subcommands.Register(&version.Command{}, "")
	subcommands.Register(&testdata.Command{}, "")

	// Set the default subcommand to help if no subcommand is specified
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Execute the specified subcommand
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
