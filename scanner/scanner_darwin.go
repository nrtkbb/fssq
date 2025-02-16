//go:build darwin
// +build darwin

package scanner

import (
	"golang.org/x/sys/unix"
)

func getPlatformSpecificAttributes(path string) (isSystem bool, isArchive bool) {
	finderInfo := make([]byte, 32)
	_, err := unix.Getxattr(path, "com.apple.FinderInfo", finderInfo)
	if err == nil {
		isSystem = finderInfo[8]&uint8(0x04) != 0  // kIsSystemFileBit
		isArchive = finderInfo[8]&uint8(0x20) != 0 // kIsArchiveBit
	}
	return
}

func getFileTimes(stat interface{}) (creation int64, modification int64, access int64) {
	if stat == nil {
		return 0, 0, 0
	}
	statT := stat.(*unix.Stat_t)
	// macOSでは creation time は Birthtimespec で取得できる
	return statT.Birthtimespec.Sec, statT.Mtimespec.Sec, statT.Atimespec.Sec
}
