package models

import (
	"sync"
	"time"
)

type FileMetadata struct {
	FilePath            string
	FileName            string
	Directory           string
	SizeBytes           int64
	CreationTimeUTC     int64
	ModificationTimeUTC int64
	AccessTimeUTC       int64
	FileMode            string
	IsDirectory         bool
	IsFile              bool
	IsSymlink           bool
	IsHidden            bool
	IsSystem            bool
	IsArchive           bool
	IsReadonly          bool
	FileExtension       string
	SHA256              *string
}

type ProgressStats struct {
	ProcessedFiles int64
	ProcessedBytes int64
	StartTime      time.Time
	LastLogTime    time.Time
	Mutex          sync.Mutex
}
