package locate

import (
	"context"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorByLeader() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kvrpcpb.Context{})
	req.ReplicaReadType = kv.ReplicaReadLeader
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	for i := 1; i <= 13; i++ {
		rpcCtx, err := selector.next(bo)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		leaderIdx := rc.getStore().workTiKVIdx
		if i <= 10 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			s.Equal(rpcCtx.Peer.Id, rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Store.storeID, rc.GetLeaderStoreID())
			s.Equal(selector.replicas[selector.targetIdx].attempts, i)
			s.Equal(selector.targetIdx, leaderIdx)
		} else if i <= 12 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			pi := i - 10
			peerIdx := AccessIndex((int(leaderIdx) + pi) % len(selector.replicas))
			replica := selector.replicas[peerIdx]
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			s.Equal(rpcCtx.Store.storeID, replica.store.storeID)
			s.Equal(selector.replicas[selector.targetIdx].attempts, 1)
			s.Equal(selector.targetIdx, peerIdx)
		} else {
			s.Nil(rpcCtx)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorByFollower() {
	// case-1: follower read, no label.
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kvrpcpb.Context{})
	req.ReplicaReadType = kv.ReplicaReadFollower
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i <= 3 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.targetReplica()
			s.True(replica.peer.Id != lastPeerId)
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
		} else {
			s.Nil(rpcCtx)
		}
	}

	// case-2: follower read, with label.
	labels := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "us-west-1",
		},
	}
	for _, staleRead := range []bool{false, true} {
		req.ReplicaReadType = kv.ReplicaReadFollower
		req.StaleRead = staleRead
		for mi := range selector.replicas {
			leaderIdx := selector.region.getStore().workTiKVIdx
			selector.replicas[mi].store.labels = labels
			region, err = s.cache.LocateKey(s.bo, []byte("a"))
			s.Nil(err)
			s.NotNil(region)
			selector, err = newReplicaSelector(s.cache, region.Region, req, WithMatchLabels(labels))
			s.Nil(err)
			if leaderIdx != AccessIndex(mi) {
				// match label in follower
				for i := 1; i <= 3; i++ {
					rpcCtx, err := selector.next(bo)
					s.Nil(err)
					rc := s.cache.GetCachedRegionWithRLock(region.Region)
					s.NotNil(rc)
					if i == 1 {
						s.NotNil(rpcCtx)
						s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
						s.Equal(selector.targetIdx, AccessIndex(mi))
						replica := selector.targetReplica()
						s.True(replica.store.IsLabelsMatch(labels))
						s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
						s.Equal(replica.attempts, 1)
					} else if i == 2 {
						s.NotNil(rpcCtx)
						s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
						s.Equal(selector.targetIdx, leaderIdx)
						replica := selector.targetReplica()
						s.False(replica.store.IsLabelsMatch(labels))
						s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
						s.Equal(replica.attempts, 1)
					} else {
						s.Nil(rpcCtx)
					}
				}
			} else {
				// match label in leader
				for i := 1; i <= 3; i++ {
					rpcCtx, err := selector.next(bo)
					s.Nil(err)
					rc := s.cache.GetCachedRegionWithRLock(region.Region)
					s.NotNil(rc)
					if i == 1 {
						s.NotNil(rpcCtx)
						s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
						s.Equal(selector.targetIdx, leaderIdx)
						replica := selector.targetReplica()
						s.True(replica.store.IsLabelsMatch(labels))
						s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
						s.Equal(replica.attempts, 1)
					} else {
						if !staleRead {
							s.Nil(rpcCtx)
						} else {
							if i == 2 {
								// retry in leader.
								s.NotNil(rpcCtx)
								s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
								s.Equal(selector.targetIdx, leaderIdx)
								replica := selector.targetReplica()
								s.True(replica.store.IsLabelsMatch(labels))
								s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
								s.Equal(replica.attempts, 2)
							} else {
								s.Nil(rpcCtx)
							}
						}
					}
				}
			}
			selector.replicas[mi].store.labels = nil
		}
	}
}
