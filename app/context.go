package app

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/nrtkbb/fssq/models"
)

type AppContext struct {
	DB            *sql.DB
	Tx            *sql.Tx
	Stmt          *sql.Stmt
	MetadataChan  chan models.FileMetadata
	Wg            *sync.WaitGroup
	Stats         *models.ProgressStats
	Context       context.Context
	Cancel        context.CancelFunc
	Cleanup       sync.Once
	LastCommit    time.Time
	ChannelClosed bool
}

func NewAppContext(parentCtx context.Context) *AppContext {
	ctx, cancel := context.WithCancel(parentCtx)
	return &AppContext{
		Context: ctx,
		Wg:      &sync.WaitGroup{},
		Cancel:  cancel,
		Stats:   NewProgressStats(),
	}
}

func NewProgressStats() *models.ProgressStats {
	now := time.Now()
	return &models.ProgressStats{
		StartTime:   now,
		LastLogTime: now,
	}
}

func (app *AppContext) PerformCleanup() {
	app.Cleanup.Do(func() {
		log.Println("Starting shutdown...")

		if app.MetadataChan != nil && !app.ChannelClosed {
			close(app.MetadataChan)
			app.ChannelClosed = true
		}

		if app.Wg != nil {
			log.Println("Waiting for pending operations to complete...")
			app.Wg.Wait()
		}

		// Handle transaction
		if app.Tx != nil {
			log.Println("Committing final transaction...")
			if err := app.Tx.Commit(); err != nil {
				log.Printf("Error committing transaction during shutdown: %v", err)
				if rbErr := app.Tx.Rollback(); rbErr != nil {
					log.Printf("Error rolling back transaction: %v", rbErr)
				}
			}
		}

		// Clean up database connection
		if app.DB != nil {
			log.Println("Cleaning up database...")

			// Force WAL checkpoint before closing
			if _, err := app.DB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				log.Printf("Error executing WAL checkpoint: %v", err)
			}

			if err := app.DB.Close(); err != nil {
				log.Printf("Error closing database: %v", err)
			}
		}

		// Clean up statement
		if app.Stmt != nil {
			app.Stmt.Close()
		}

		log.Println("Graceful shutdown completed")
	})
}
