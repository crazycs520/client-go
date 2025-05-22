//go:build !linux

package tikvrpc

import (
	"os"
)

// CreateEventFDFile creates a new eventfd file with CLOEXEC and NONBLOCK flags.
func CreateEventFDFile() (*os.File, int, error) {
	panic("eventfd is only supported on linux")
}
