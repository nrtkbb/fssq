package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/google/subcommands"
	"github.com/nrtkbb/fssq/cmd/merge"
	"github.com/nrtkbb/fssq/cmd/migrate"
	"github.com/nrtkbb/fssq/cmd/scan"
	"github.com/nrtkbb/fssq/cmd/serve"
	"github.com/nrtkbb/fssq/cmd/testdata"
	"github.com/nrtkbb/fssq/cmd/version"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// initTracer initializes the OpenTelemetry tracer provider
func initTracer() (*sdktrace.TracerProvider, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}

	resource := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("fssq"),
		semconv.ServiceVersion("1.0.0"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
	)
	otel.SetTracerProvider(tp)

	return tp, nil
}

func main() {
	tp, err := initTracer()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

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
