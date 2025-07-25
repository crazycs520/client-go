//go:build linux

package tikvrpc

import (
	"os"

	"golang.org/x/sys/unix"
)

// createEventFDFile creates a new eventfd file with CLOEXEC and NONBLOCK flags.
func createEventFDFile() *pooledEventFD {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return &pooledEventFD{err: err}
	}
	f := os.NewFile(uintptr(fd), "eventfd")
	return &pooledEventFD{f: f, fd: fd}
}
