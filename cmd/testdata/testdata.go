package testdata

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/subcommands"
)

type Command struct {
	outputDir string
}

func (*Command) Name() string     { return "testdata" }
func (*Command) Synopsis() string { return "Generate test data for scanning" }
func (*Command) Usage() string {
	return `testdata -out <directory>:
  Generate test data directory structure with files for testing scan functionality.
`
}

func (c *Command) SetFlags(f *flag.FlagSet) {
	f.StringVar(&c.outputDir, "out", "", "output directory path (required)")
}

func (c *Command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if c.outputDir == "" {
		f.Usage()
		return subcommands.ExitUsageError
	}

	if err := generateTestData(c.outputDir); err != nil {
		log.Printf("Failed to generate test data: %v", err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func generateTestData(outputDir string) error {
	// Create base directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Create 10 directories
	dirs := []string{
		"docs",
		"images",
		"videos",
		"music",
		"docs/reports",
		"docs/presentations",
		"images/thumbnails",
		"images/originals",
		"videos/raw",
		"videos/edited",
	}

	for _, dir := range dirs {
		path := filepath.Join(outputDir, dir)
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	// Create files with different contents
	// Some files will have the same content to test hash matching
	contents := []struct {
		content string
		count   int
		ext     string
	}{
		{"This is a test document\n", 5, ".txt"},
		{"Hello, World!\n", 3, ".txt"},
		{"const main = () => console.log('test');\n", 4, ".js"},
		{"# Test Markdown\n\nThis is a test.\n", 3, ".md"},
		{"<!DOCTYPE html><html><body>Test</body></html>\n", 3, ".html"},
		{"package main\n\nfunc main() {}\n", 4, ".go"},
		{"CREATE TABLE test (id INT);\n", 3, ".sql"},
		{"Test data for duplicate files\n", 5, ".txt"},
		{strings.Repeat("Large content repeated ", 1000), 2, ".log"},
	}

	// Generate files
	fileCount := 1
	for _, c := range contents {
		for i := 0; i < c.count; i++ {
			// Select random directory
			dir := dirs[fileCount%len(dirs)]
			filename := fmt.Sprintf("file%d%s", fileCount, c.ext)
			path := filepath.Join(outputDir, dir, filename)

			// Create file with content
			if err := os.WriteFile(path, []byte(c.content), 0644); err != nil {
				return fmt.Errorf("failed to create file %s: %v", filename, err)
			}

			// Add some delay to ensure different modification times
			time.Sleep(10 * time.Millisecond)
			fileCount++
		}
	}

	log.Printf("Generated %d directories and %d files in %s", len(dirs), fileCount-1, outputDir)
	return nil
}
