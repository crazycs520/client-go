// Package ffi is added because we want to set TiKVAddrInSameProcess to package
// "github.com/tikv/client-go/v2/internal/client", but it's an internal package.
// And strange that that internal package also imports other non-internal so
// cyclic import is easy to happen. I add this package to break the cycle.
package ffi

import (
	"github.com/tikv/client-go/v2/internal/client"
)

// SetTiKVAddrInSameProcess is used to specify the address of TiKV server when
// TiKV is running in the same process. So when creating gRPC client to TiKV we
// can return a FFI client directly.
func SetTiKVAddrInSameProcess(addr string) {
	client.TiKVAddrInSameProcess = addr
}
