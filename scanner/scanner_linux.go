//go:build linux
// +build linux

package scanner

import (
	"golang.org/x/sys/unix"
)

func getPlatformSpecificAttributes(path string) (isSystem bool, isArchive bool) {
	// On Linux, there are no direct equivalents to Windows system/archive flags
	// We could check for extended attributes but for simplicity return false
	return false, false
}

func getFileTimes(stat interface{}) (creation int64, modification int64, access int64) {
	if stat == nil {
		return 0, 0, 0
	}
	statT := stat.(*unix.Stat_t)
	// Linux doesn't have a creation time in the standard stat structure
	// Use modification time as creation time
	return statT.Mtim.Sec, statT.Mtim.Sec, statT.Atim.Sec
}