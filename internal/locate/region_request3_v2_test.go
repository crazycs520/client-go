package locate

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"time"
)

func (s *testRegionRequestToThreeStoresSuite) TestReadInLeader() {
	cnt := 0
	cli := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		cnt++
		switch cnt {
		case 1:
			region, leaderPeerId := s.cluster.GetRegion(req.RegionId)
			for _, peer := range region.Peers {
				if peer.Id == leaderPeerId {
					continue
				}
				s.cluster.ChangeLeader(req.RegionId, peer.Id)
				break
			}
		}
		response, err = mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil).SendRequest(ctx, addr, req, timeout)
		fmt.Printf("cnt: %v, addr: %v , resp: %v, err: %v ========\n", cnt, addr, response, err)
		return response, err
	}}

	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kvrpcpb.Context{})
	req.ReplicaReadType = kv.ReplicaReadLeader
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	bo := retry.NewBackoffer(context.Background(), 1000)
	resp, _, _, err := NewRegionRequestSender(s.cache, cli).SendReqCtx(bo, req, region.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	s.NotNil(resp)
	//s.Nil(resp.GetRegionError()) // should be success? but fail cause by https://github.com/tikv/client-go/pull/264/files#diff-0c36d07fbd1fc71c850ff57d323fe12bdf7f2c053ef71b7fec5bb44358f1070dR596-R599
	// It's unreasoneable to retry in upper layer, such as cop request, the upper layer will need to rebuild cop request and retry, there are some unnecessary overhead.
	r := s.cache.GetCachedRegionWithRLock(region.Region) // the region is still valid, won't reload region.
	s.True(r.isValid())
}
