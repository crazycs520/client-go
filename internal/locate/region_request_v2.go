package locate

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/client"
)

type RegionRequestSenderV2 struct {
	regionCache     *RegionCache
	client          client.Client
	storeAddr       string
	rpcError        error
	replicaSelector *replicaSelector
	RegionRequestRuntimeStats
}

func NewRegionRequestSenderV2(regionCache *RegionCache, client client.Client) *RegionRequestSenderV2 {
	return &RegionRequestSenderV2{
		regionCache: regionCache,
		client:      client,
	}
}

//func (s *RegionRequestSenderV2) SendReq(
//	bo *retry.Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration,
//) (*tikvrpc.Response, int, error) {
//	resp, _, retryTimes, err := s.SendReqCtx(bo, req, regionID, timeout, tikvrpc.TiKV)
//	return resp, retryTimes, err
//}

type replicaSelectorV2 struct {
	regionCache *RegionCache
	region      *Region
	replicas    []*replica
	labels      []*metapb.StoreLabel
	state       selectorStateV2
	// replicas[targetIdx] is the replica handling the request this time
	targetIdx AccessIndex
}

type selectorStateV2 interface {
	next(*retry.Backoffer, *replicaSelector) (*RPCContext, error)
	onSendSuccess(*replicaSelector)
	onSendFailure(*retry.Backoffer, *replicaSelector, error)
	onNoLeader(*replicaSelector)
}
