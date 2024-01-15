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
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadLeader, nil, kvrpcpb.Context{})
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
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, false)
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
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false) // stale read will always disable in follower.
			s.Equal(req.ReplicaRead, false)
		} else {
			s.Nil(rpcCtx)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorByFollower() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadFollower, nil, kvrpcpb.Context{})
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
		if i <= 2 {
			// try followers.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.targetReplica()
			s.True(replica.peer.Id != lastPeerId)
			s.True(replica.peer.Id != rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else if i == 3 {
			// try leader.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.targetReplica()
			s.True(replica.peer.Id == rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorByPreferLeader() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadPreferLeader, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i == 1 {
			// try leader first.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.targetReplica()
			s.True(replica.peer.Id == rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			s.Equal(replica.attempts, 1)
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
			//} else if i <= 3 { // bug: won't try follower currently.
			// try followers.
			//s.NotNil(rpcCtx, fmt.Sprintf("i: %v", i))
			//s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			//replica := selector.targetReplica()
			//s.True(replica.peer.Id != lastPeerId)
			//s.True(replica.peer.Id != rc.GetLeaderPeerID())
			//s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			//lastPeerId = replica.peer.Id
			//s.Equal(replica.attempts, 1)
			//rpcCtx.contextPatcher.applyTo(&req.Context)
			//s.Equal(req.StaleRead, false)
			//s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorByMixed() {
	// case-1: mixed read, no label.
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
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
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
		}
	}
	// case-2: mixed read, with label.
	labels := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "us-west-1",
		},
	}
	for mi := range selector.replicas {
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
		selector.replicas[mi].store.labels = labels
		region, err = s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		s.NotNil(region)
		selector, err = newReplicaSelector(s.cache, region.Region, req, WithMatchLabels(labels))
		s.Nil(err)
		leaderIdx := selector.region.getStore().workTiKVIdx
		if leaderIdx != AccessIndex(mi) {
			// match label in follower
			for i := 1; i <= 6; i++ {
				rpcCtx, err := selector.next(bo)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					// first try the match-label peer.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.Equal(selector.targetIdx, AccessIndex(mi))
					replica := selector.targetReplica()
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i == 2 {
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.Equal(selector.targetIdx, leaderIdx)
					replica := selector.targetReplica()
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
				}
			}
		} else {
			// match label in leader
			for i := 1; i <= 5; i++ {
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
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
				}
			}
		}
		selector.replicas[mi].store.labels = nil
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorByStaleRead() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	req.EnableStaleWithMixedReplicaRead()
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	leaderIdx := selector.region.getStore().workTiKVIdx
	firstTryInLeader := false
	lastPeerId := uint64(0)
	for i := 1; i <= 5; i++ {
		rpcCtx, err := selector.next(bo)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i == 1 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.targetReplica()
			firstTryInLeader = selector.targetIdx == leaderIdx
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, true)
			s.Equal(req.ReplicaRead, false)
		} else if i == 2 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.targetReplica()
			s.True(selector.targetIdx == leaderIdx)
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			if firstTryInLeader {
				s.Equal(replica.attempts, 2)
			} else {
				s.Equal(replica.attempts, 1)
			}
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, false)
		} else if i == 3 {
			if firstTryInLeader {
				s.NotNil(rpcCtx)
				s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
				replica := selector.targetReplica()
				s.True(selector.targetIdx != leaderIdx)
				s.NotEqual(rpcCtx.Peer.Id, lastPeerId)
				s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
				s.Equal(replica.attempts, 1)
				_, ok := selector.state.(*tryFollower)
				s.Equal(ok, true)
				rpcCtx.contextPatcher.applyTo(&req.Context)
				s.Equal(req.StaleRead, false)
				s.Equal(req.ReplicaRead, true)
			} else {
				s.NotNil(rpcCtx)
				s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
				replica := selector.targetReplica()
				s.True(selector.targetIdx == leaderIdx)
				s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
				s.Equal(replica.attempts, 2)
				rpcCtx.contextPatcher.applyTo(&req.Context)
				s.Equal(req.StaleRead, false)
				s.Equal(req.ReplicaRead, false)
			}
		} else if i == 4 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.targetReplica()
			s.True(selector.targetIdx != leaderIdx)
			s.NotEqual(rpcCtx.Peer.Id, lastPeerId)
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			s.Equal(replica.attempts, 1)
			_, ok := selector.state.(*tryFollower)
			s.Equal(ok, true)
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
		}
		if rpcCtx != nil {
		}
	}

	// test with label.
	labels := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "us-west-1",
		},
	}
	for mi := range selector.replicas {
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
		req.EnableStaleWithMixedReplicaRead()
		region, err = s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		s.NotNil(region)
		selector, err = newReplicaSelector(s.cache, region.Region, req, WithMatchLabels(labels))
		selector.replicas[mi].store.labels = labels
		leaderIdx := selector.region.getStore().workTiKVIdx
		s.Nil(err)
		if leaderIdx != AccessIndex(mi) {
			// match label in follower
			for i := 1; i <= 6; i++ {
				rpcCtx, err := selector.next(bo)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					// first try the match-label peer.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.Equal(selector.targetIdx, AccessIndex(mi))
					replica := selector.targetReplica()
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, true)
					s.Equal(req.ReplicaRead, false)
				} else if i == 2 {
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.Equal(selector.targetIdx, leaderIdx)
					replica := selector.targetReplica()
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, false)
				} else if i == 3 {
					// retry in leader again? this is not expected.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.Equal(selector.targetIdx, leaderIdx)
					replica := selector.targetReplica()
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 2)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, false)
				} else if i == 4 {
					// retry the match-label peer again. this is not expected?
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.Equal(selector.targetIdx, AccessIndex(mi))
					replica := selector.targetReplica()
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 2)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i == 5 {
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.NotEqual(selector.targetIdx, AccessIndex(mi))
					s.NotEqual(selector.targetIdx, leaderIdx)
					replica := selector.targetReplica()
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
				}
			}
		} else {
			// match label in leader
			for i := 1; i <= 5; i++ {
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
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, true)
					s.Equal(req.ReplicaRead, false)
				} else if i == 2 {
					// retry in leader.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.Equal(selector.targetIdx, leaderIdx)
					replica := selector.targetReplica()
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 2)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, false)
				} else if i <= 4 {
					// stale read will try follower.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					s.NotEqual(selector.targetIdx, leaderIdx)
					replica := selector.targetReplica()
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					rpcCtx.contextPatcher.applyTo(&req.Context)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
				}
			}
		}
		selector.replicas[mi].store.labels = nil
	}
}
