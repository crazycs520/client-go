//go:build !linux && !darwin

package tikvrpc

func createEventFDFile() *pooledEventFD {
	panic("eventfd is only supported on linux")
}
