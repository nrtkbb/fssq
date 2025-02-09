package scanner

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/nrtkbb/fssq/models"
	"golang.org/x/sys/unix"
)

func CollectMetadata(path string, info os.FileInfo, relPath string, skipHash bool) models.FileMetadata {
	stat := info.Sys().(*syscall.Stat_t)

	// Get macOS-specific attributes
	var isSystem, isArchive bool
	finderInfo := make([]byte, 32)
	_, err := unix.Getxattr(path, "com.apple.FinderInfo", finderInfo)
	if err == nil {
		isSystem = finderInfo[8]&uint8(0x04) != 0  // kIsSystemFileBit
		isArchive = finderInfo[8]&uint8(0x20) != 0 // kIsArchiveBit
	}

	// Calculate SHA256 hash (only for files)
	var sha256Hash *string
	if !skipHash && !info.IsDir() {
		if hash, err := CalculateSHA256(path); err == nil {
			sha256Hash = &hash
		}
	}

	return models.FileMetadata{
		FilePath:            relPath,
		FileName:            info.Name(),
		Directory:           filepath.Dir(relPath),
		SizeBytes:           info.Size(),
		CreationTimeUTC:     stat.Birthtimespec.Sec,
		ModificationTimeUTC: stat.Mtimespec.Sec,
		AccessTimeUTC:       stat.Atimespec.Sec,
		FileMode:            FormatFileMode(info.Mode()),
		IsDirectory:         info.IsDir(),
		IsFile:              !info.IsDir(),
		IsSymlink:           info.Mode()&os.ModeSymlink != 0,
		IsHidden:            strings.HasPrefix(filepath.Base(path), "."),
		IsSystem:            isSystem,
		IsArchive:           isArchive,
		IsReadonly:          info.Mode()&0200 == 0,
		FileExtension:       strings.ToLower(filepath.Ext(path)),
		SHA256:              sha256Hash,
	}
}

func CalculateSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
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
