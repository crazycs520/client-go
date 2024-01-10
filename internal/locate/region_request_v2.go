package locate

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type Replica struct {
	store    *Store
	peer     *metapb.Peer
	epoch    uint32
	attempts int
	// deadlineErrUsingConfTimeout indicates the replica is already tried, but the received deadline exceeded error.
	deadlineErrUsingConfTimeout bool
}

type ReplicaSelector struct {
	endpointTp      tikvrpc.EndpointType
	replicaReadType kv.ReplicaReadType

	regionCache *RegionCache
	region      *Region
	replicas    []*replica
}
