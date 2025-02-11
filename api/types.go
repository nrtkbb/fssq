package api

// DirectoryEntry represents a directory listing entry
type DirectoryEntry struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	IsDir     bool   `json:"is_dir"`
	Size      int64  `json:"size"`
	Modified  int64  `json:"modified"`
	FileCount int    `json:"file_count,omitempty"`
	DirCount  int    `json:"dir_count,omitempty"`
}

// FileMetadata represents detailed file metadata
type FileMetadata struct {
	Name          string  `json:"name"`
	Path          string  `json:"path"`
	Size          int64   `json:"size"`
	Created       int64   `json:"created"`
	Modified      int64   `json:"modified"`
	Accessed      int64   `json:"accessed"`
	IsDir         bool    `json:"is_dir"`
	IsSymlink     bool    `json:"is_symlink"`
	IsHidden      bool    `json:"is_hidden"`
	FileMode      string  `json:"file_mode"`
	FileExtension string  `json:"file_extension"`
	SHA256        *string `json:"sha256,omitempty"`
}

// ExtensionStats represents file extension statistics
type ExtensionStats struct {
	Extension    string  `json:"extension"`
	FileCount    int     `json:"file_count"`
	TotalSize    int64   `json:"total_size"`
	AverageSize  float64 `json:"average_size"`
	MinSize      int64   `json:"min_size"`
	MaxSize      int64   `json:"max_size"`
	LastModified int64   `json:"last_modified"`
}

// DirectoryStats represents directory statistics
type DirectoryStats struct {
	Path           string `json:"path"`
	ParentPath     string `json:"parent_path,omitempty"`
	Depth          int    `json:"depth"`
	FileCount      int    `json:"file_count"`
	DirectoryCount int    `json:"dir_count"`
	TotalSize      int64  `json:"total_size"`
	LastModified   int64  `json:"last_modified"`
}

// StorageComparison represents a comparison between two storage states
type StorageComparison struct {
	Path        string `json:"path"`
	OldSize     int64  `json:"old_size"`
	NewSize     int64  `json:"new_size"`
	SizeDiff    int64  `json:"size_diff"`
	OldModified int64  `json:"old_modified"`
	NewModified int64  `json:"new_modified"`
	Status      string `json:"status"` // "added", "deleted", "modified", "unchanged"
}

// CacheStatus represents the status of a cache
type CacheStatus struct {
	CacheType    string `json:"cache_type"`
	TotalEntries int    `json:"total_entries"`
	StaleEntries int    `json:"stale_entries"`
	LastUpdate   int64  `json:"last_update"`
	Status       string `json:"status"` // "REBUILD", "UPDATE", "FRESH"
}

// PaginatedResponse represents a paginated response
type PaginatedResponse struct {
	Data       interface{} `json:"data"`
	Page       int         `json:"page"`
	PerPage    int         `json:"per_page"`
	Total      int         `json:"total"`
	TotalPages int         `json:"total_pages"`
	HasNext    bool        `json:"has_next"`
}
