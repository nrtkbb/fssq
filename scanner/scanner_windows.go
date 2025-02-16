//go:build windows
// +build windows

package scanner

import (
	"os"
	"syscall"
	"time"
)

func getPlatformSpecificAttributes(path string) (isSystem bool, isArchive bool) {
	fileAttributes := uint32(0)
	if info, err := os.Stat(path); err == nil {
		if sysInfo, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
			fileAttributes = sysInfo.FileAttributes
		}
	}

	isSystem = fileAttributes&syscall.FILE_ATTRIBUTE_SYSTEM != 0
	isArchive = fileAttributes&syscall.FILE_ATTRIBUTE_ARCHIVE != 0
	return
}

func getFileTimes(stat interface{}) (creation int64, modification int64, access int64) {
	if stat == nil {
		return 0, 0, 0
	}
	if winStat, ok := stat.(*syscall.Win32FileAttributeData); ok {
		// Convert Windows timestamps to Unix timestamps
		creation = time.Unix(0, winStat.CreationTime.Nanoseconds()).Unix()
		modification = time.Unix(0, winStat.LastWriteTime.Nanoseconds()).Unix()
		access = time.Unix(0, winStat.LastAccessTime.Nanoseconds()).Unix()
	}
	return
}
