//go:build linux

package tikvrpc

import (
	"os"

	"golang.org/x/sys/unix"
)

// CreateEventFDFile creates a new eventfd file with CLOEXEC and NONBLOCK flags.
func CreateEventFDFile() (*os.File, int, error) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return nil, -1, err
	}
	f := os.NewFile(uintptr(fd), "eventfd")
	return f, fd, nil
}
