package scanner

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nrtkbb/fssq/models"
)

func CollectMetadata(path string, info os.FileInfo, relPath string, skipHash bool) models.FileMetadata {
	isSystem, isArchive := getPlatformSpecificAttributes(path)
	creation, modification, access := getFileTimes(info.Sys())

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
