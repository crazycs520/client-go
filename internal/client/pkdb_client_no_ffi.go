//go:build !fusion

package client

import (
	"context"

	"github.com/tikv/client-go/v2/tikvrpc"
)

var callFFI func(context.Context, string, *tikvrpc.Request) (*tikvrpc.Response, error) = nil
