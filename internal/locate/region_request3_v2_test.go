package locate

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"testing"
	"time"
)

func (s *testRegionRequestToThreeStoresSuite) TestReadInNotLeader0() {
	cnt := 0
	cli := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		cnt++
		switch cnt {
		case 1:
			// return no leader without new leader info
			response = &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}},
			}}
		default:
			return nil, fmt.Errorf("unexpected request")
		}
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
	regionErr, err := resp.GetRegionError()
	s.Nil(err)
	s.Equal(regionErr.String(), "epoch_not_match:<> ") // should be 'not leader'?
	s.Equal(cnt, 1)
	r := s.cache.GetCachedRegionWithRLock(region.Region)
	s.False(r.isValid())
}

func (s *testRegionRequestToThreeStoresSuite) TestReadInNotLeader1() {
	cnt := 0
	var location *KeyLocation
	cli := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		cnt++
		switch cnt {
		case 1:
			region := s.cache.GetCachedRegionWithRLock(location.Region)
			s.NotNil(region)
			leaderPeerIdx := int(region.getStore().workTiKVIdx)
			peers := region.meta.Peers
			// return no leader with new leader info
			response = &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{
					RegionId: req.RegionId,
					Leader:   peers[(leaderPeerIdx+1)%len(peers)],
				}},
			}}
		case 2:
			response = &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
				Value: []byte("a"),
			}}
		default:
			return nil, fmt.Errorf("unexpected request")
		}
		return response, err
	}}

	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kvrpcpb.Context{})
	req.ReplicaReadType = kv.ReplicaReadLeader
	var err error
	location, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(location)
	bo := retry.NewBackoffer(context.Background(), 1000)
	resp, _, _, err := NewRegionRequestSender(s.cache, cli).SendReqCtx(bo, req, location.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	s.NotNil(resp)
	regionErr, err := resp.GetRegionError()
	s.Nil(err)
	s.Nil(regionErr) // should be success? but fail cause by https://github.com/tikv/client-go/pull/264/files#diff-0c36d07fbd1fc71c850ff57d323fe12bdf7f2c053ef71b7fec5bb44358f1070dR596-R599
	// It's unreasoneable to retry in upper layer, such as cop request, the upper layer will need to rebuild cop request and retry, there are some unnecessary overhead.
	s.Equal(cnt, 2)
	r := s.cache.GetCachedRegionWithRLock(location.Region)
	s.True(r.isValid())
}

func (s *testRegionRequestToThreeStoresSuite) TestReadMeetRegionNotFound() {
	cnt := 0
	cli := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		cnt++
		switch cnt {
		case 1:
			response = &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{
					RegionNotFound: &errorpb.RegionNotFound{RegionId: req.RegionId},
				},
			}}
		default:
			return nil, fmt.Errorf("unexpected request")
		}
		return response, err
	}}

	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kvrpcpb.Context{})
	req.ReplicaReadType = kv.ReplicaReadLeader
	location, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(location)
	bo := retry.NewBackoffer(context.Background(), 1000)
	resp, _, _, err := NewRegionRequestSender(s.cache, cli).SendReqCtx(bo, req, location.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	s.NotNil(resp)
	regionErr, err := resp.GetRegionError()
	s.Nil(err)
	s.Equal(regionErr.String(), "region_not_found:<region_id:7 > ")
	r := s.cache.GetCachedRegionWithRLock(location.Region)
	s.Equal(r.isValid(), false)
}

func BenchmarkLeaderRead(b *testing.B) {
	b.StopTimer()
	mvccStore := mocktikv.MustNewMVCCStore()
	cluster := mocktikv.NewCluster(mvccStore)
	mocktikv.BootstrapWithMultiStores(cluster, 3)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	cache := NewRegionCache(pdCli)
	defer func() {
		cache.Close()
		mvccStore.Close()
	}()

	cli := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			Value: []byte("a"),
		}}, err
	}}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bo := retry.NewBackoffer(context.Background(), 1000)
		location, err := cache.LocateKey(bo, []byte("a"))
		require.NoError(b, err)
		require.NotNil(b, location)
		req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kvrpcpb.Context{})
		resp, _, _, err := NewRegionRequestSender(cache, cli).SendReqCtx(bo, req, location.Region, time.Second, tikvrpc.TiKV)
		require.NoError(b, err)
		regionErr, err := resp.GetRegionError()
		require.NoError(b, err)
		require.Nil(b, regionErr)
	}
}
