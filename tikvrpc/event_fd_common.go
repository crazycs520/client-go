package tikvrpc

import (
	"encoding/binary"
	"os"
	"sync"
)

// waitEventFDFile waits for the eventfd file to be readable and returns the read uint64 value.
func waitEventFDFile(f *os.File) uint64 {
	buf := make([]byte, 8)
	// Ignore here referencing Go's runtime handling:
	// See https://github.com/golang/go/blob/f9ce1dddc264cb30e68bfedbabf159b32bb6a719/src/runtime/netpoll_epoll.go#L152
	f.Read(buf)
	ts := binary.LittleEndian.Uint64(buf)
	// If ts is 0, it indicates an error occurred.
	// If ts is the maximum uint64 value, it indicates that the notify timestamp is invalid.
	// Otherwise, ts represents the valid notify timestamp.
	return ts
}

type pooledEventFD struct {
	f   *os.File
	fd  int
	err error
}

var eventFDPool = sync.Pool{
	New: func() interface{} {
		f, fd, err := CreateEventFDFile()
		// Wrap into a pooledEventFD even if err != nil
		return &pooledEventFD{f: f, fd: fd, err: err}
	},
}

// GetEventFDFile retrieves an eventfd from the pool.
// On error, it returns nil, -1, and the creation error.
func GetEventFDFile() (*os.File, int, error) {
	p := eventFDPool.Get().(*pooledEventFD)
	if p.err != nil {
		// Do not put the error sentinel back into the pool.
		return nil, -1, p.err
	}
	return p.f, p.fd, nil
}

// PutEventFDFile returns a valid eventfd back into the pool.
// If f is nil (i.e. a sentinel), it will be discarded.
func PutEventFDFile(f *os.File, fd int) {
	if f == nil {
		return
	}
	eventFDPool.Put(&pooledEventFD{f: f, fd: fd})
}
