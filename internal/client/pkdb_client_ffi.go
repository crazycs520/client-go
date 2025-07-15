//go:build fusion

package client

import (
	"context"

	"github.com/tikv/client-go/v2/tikvrpc"
)

var callFFI = func(ctx context.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	if addr == TiKVAddrInSameProcess && tikvrpc.EnableTiKVLocalCall.Load() && tikvrpc.CmdTypeCanFFI(req.Type) {
		if tikvrpc.CmdTypeFFIWithChannel(req.Type) {
			return tikvrpc.CallFFIWithChannel(ctx, req)
		}
		return tikvrpc.CallFFIWithEventFD(ctx, req)
	}
	return nil, nil
}
