//go:build darwin

package tikvrpc

import (
	"os"
	"runtime"
	"syscall"

	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
)

// createEventFDFile uses pipe to mimic eventfd. It's only used in macOS,
// performance is not critical.
func createEventFDFile() *pooledEventFD {
	fds := make([]int, 2)
	if err := syscall.Pipe(fds); err != nil {
		return &pooledEventFD{err: err}
	}
	f := os.NewFile(uintptr(fds[0]), "pipe_as_eventfd")
	ret := &pooledEventFD{f: f, fd: fds[1]}
	runtime.AddCleanup(ret, func(fd int) {
		err2 := syscall.Close(fd)
		if err2 != nil {
			logutil.BgLogger().Error(
				"golang GC failed to close pipe fd",
				zap.Int("fd", fd), zap.Error(err2))
		}
	}, fds[1])
	return ret
}
