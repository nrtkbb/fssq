package scanner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nrtkbb/fssq/models"
)

func CollectMetadata(path string, info os.FileInfo, relPath string, skipHash bool) models.FileMetadata {
	isSystem, isArchive := getPlatformSpecificAttributes(path)
	creation, modification, access := getFileTimes(info.Sys())

	// Calculate weak ETag (only for files)
	var weakETag *string
	if !skipHash && !info.IsDir() {
		etag := CalculateWeakETag(info)
		weakETag = &etag
	}

	return models.FileMetadata{
		FilePath:            relPath,
		FileName:            info.Name(),
		Directory:           filepath.Dir(relPath),
		SizeBytes:           info.Size(),
		CreationTimeUTC:     creation,
		ModificationTimeUTC: modification,
		AccessTimeUTC:       access,
		FileMode:            FormatFileMode(info.Mode()),
		IsDirectory:         info.IsDir(),
		IsFile:              !info.IsDir(),
		IsSymlink:           info.Mode()&os.ModeSymlink != 0,
		IsHidden:            strings.HasPrefix(filepath.Base(path), "."),
		IsSystem:            isSystem,
		IsArchive:           isArchive,
		IsReadonly:          info.Mode()&0200 == 0,
		FileExtension:       strings.ToLower(filepath.Ext(path)),
		SHA256:              weakETag,
	}
}

// CalculateWeakETag generates a weak ETag based on file metadata (size + mtime)
// This is similar to NGINX's weak ETag approach and avoids reading file contents
func CalculateWeakETag(info os.FileInfo) string {
	// Format: W/"<size>-<mtime_hex>"
	// Using hexadecimal representation of modification time for compactness
	return fmt.Sprintf("W/\"%x-%x\"", info.Size(), info.ModTime().Unix())
}

func FormatFileMode(mode os.FileMode) string {
	permBits := mode & os.ModePerm

	var typeChar string
	switch {
	case mode&os.ModeDir != 0:
		typeChar = "d"
	case mode&os.ModeSymlink != 0:
		typeChar = "l"
	default:
		typeChar = "-"
	}

	result := typeChar

	// Owner permissions
	result += map[bool]string{true: "r", false: "-"}[(permBits&0400) != 0]
	result += map[bool]string{true: "w", false: "-"}[(permBits&0200) != 0]
	result += map[bool]string{true: "x", false: "-"}[(permBits&0100) != 0]

	// Group permissions
	result += map[bool]string{true: "r", false: "-"}[(permBits&040) != 0]
	result += map[bool]string{true: "w", false: "-"}[(permBits&020) != 0]
	result += map[bool]string{true: "x", false: "-"}[(permBits&010) != 0]

	// Others permissions
	result += map[bool]string{true: "r", false: "-"}[(permBits&04) != 0]
	result += map[bool]string{true: "w", false: "-"}[(permBits&02) != 0]
	result += map[bool]string{true: "x", false: "-"}[(permBits&01) != 0]

	return result
}
