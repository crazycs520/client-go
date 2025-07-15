package tikvrpc

import (
	"encoding/binary"
	"os"
	"sync"
)

// waitEventFDFile waits for the eventfd file to be readable and returns the read uint64 value.
func waitEventFDFile(c *pooledEventFD) uint64 {
	buf := make([]byte, 8)
	// Ignore here referencing Go's runtime handling:
	// See https://github.com/golang/go/blob/f9ce1dddc264cb30e68bfedbabf159b32bb6a719/src/runtime/netpoll_epoll.go#L152
	c.f.Read(buf)
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
		// Wrap into a pooledEventFD even if err != nil
		return createEventFDFile()
	},
}

// getEventFDFile retrieves an eventfd from the pool.
// On error, it returns nil, -1, and the creation error.
func getEventFDFile() (*pooledEventFD, error) {
	p := eventFDPool.Get().(*pooledEventFD)
	if p.err != nil {
		// Do not put the error sentinel back into the pool.
		return nil, p.err
	}
	return p, nil
}

// putEventFDFile returns a valid eventfd back into the pool.
// If c is nil (i.e. a sentinel), it will be discarded.
func putEventFDFile(c *pooledEventFD) {
	if c != nil {
		eventFDPool.Put(c)
	}
}
