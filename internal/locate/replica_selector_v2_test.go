package locate

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByLeader() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadLeader, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelectorV2(s.cache, region.Region, req, tikvrpc.TiKV)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 13; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		leaderIdx := rc.getStore().workTiKVIdx
		leader := selector.replicas[leaderIdx]
		if i <= 10 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			s.Equal(rpcCtx.Peer.Id, rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Store.storeID, rc.GetLeaderStoreID())
			s.Equal(selector.target.attempts, i)
			s.Equal(selector.target.peer.Id, leader.peer.Id)
			//rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, false)
		} else if i <= 12 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			s.Equal(rpcCtx.Store.storeID, replica.store.storeID)
			s.Equal(selector.target.attempts, 1)
			s.NotEqual(selector.target.peer.Id, lastPeerId)
			lastPeerId = selector.target.peer.Id
			rpcCtx.contextPatcher.applyTo(&req.Context)
			s.Equal(req.StaleRead, false) // stale read will always disable in follower.
			s.Equal(req.ReplicaRead, false)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByFollower() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadFollower, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelectorV2(s.cache, region.Region, req, tikvrpc.TiKV)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i <= 2 {
			// try followers.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.True(replica.peer.Id != lastPeerId)
			s.True(replica.peer.Id != rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else if i == 3 {
			// try leader.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.True(replica.peer.Id == rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByPreferLeader() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadPreferLeader, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelectorV2(s.cache, region.Region, req, tikvrpc.TiKV)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i == 1 {
			// try leader first.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.True(replica.peer.Id == rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else if i <= 3 {
			//try followers.
			s.NotNil(rpcCtx, fmt.Sprintf("i: %v", i))
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.True(replica.peer.Id != lastPeerId)
			s.True(replica.peer.Id != rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByMixed() {
	// case-1: mixed read, no label.
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelectorV2(s.cache, region.Region, req, tikvrpc.TiKV)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i <= 3 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.True(replica.peer.Id != lastPeerId)
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			lastPeerId = replica.peer.Id
			s.Equal(replica.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
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
		selector, err = newReplicaSelectorV2(s.cache, region.Region, req, tikvrpc.TiKV, WithMatchLabels(labels))
		s.Nil(err)
		leaderIdx := selector.region.getStore().workTiKVIdx
		leader := selector.replicas[leaderIdx]
		matchReplica := selector.replicas[mi]
		if leaderIdx != AccessIndex(mi) {
			// match label in follower
			for i := 1; i <= 6; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					// first try the match-label peer.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.Equal(replica.peer.Id, matchReplica.peer.Id)
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i == 2 {
					// prefer leader in second try.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.Equal(replica.peer.Id, leader.peer.Id)
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i == 3 { // diff from v1
					// try last remain follower.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.NotEqual(replica.peer.Id, matchReplica.peer.Id)
					s.NotEqual(replica.peer.Id, leader.peer.Id)
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		} else {
			// match label in leader
			for i := 1; i <= 5; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					// try leader
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.Equal(replica.peer.Id, leader.peer.Id)
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i <= 3 {
					// try follower
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.NotEqual(replica.peer.Id, leader.peer.Id)
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		}
		selector.replicas[mi].store.labels = nil
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByStaleRead() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	req.EnableStaleWithMixedReplicaRead()
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := newReplicaSelectorV2(s.cache, region.Region, req, tikvrpc.TiKV)
	s.Nil(err)
	leaderIdx := selector.region.getStore().workTiKVIdx
	leader := selector.replicas[leaderIdx]
	firstTryInLeader := false
	lastPeerId := uint64(0)
	for i := 1; i <= 5; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i == 1 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			firstTryInLeader = replica.peer.Id == leader.peer.Id
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			s.Equal(replica.attempts, 1)
			s.Equal(req.StaleRead, true)
			s.Equal(req.ReplicaRead, false)
		} else if i == 2 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.True(replica.peer.Id == leader.peer.Id)
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			if firstTryInLeader {
				s.Equal(replica.attempts, 2)
			} else {
				s.Equal(replica.attempts, 1)
			}
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, false)
		} else if i == 3 { // diff from v1
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			replica := selector.target
			s.True(replica.peer.Id != leader.peer.Id)
			s.NotEqual(rpcCtx.Peer.Id, lastPeerId)
			s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
			s.Equal(replica.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else if i == 4 {
			if firstTryInLeader {
				s.NotNil(rpcCtx)
				s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
				replica := selector.target
				s.True(replica.peer.Id != leader.peer.Id)
				s.NotEqual(rpcCtx.Peer.Id, lastPeerId)
				s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
				s.Equal(replica.attempts, 1)
				s.Equal(req.StaleRead, false)
				s.Equal(req.ReplicaRead, true)
			} else {
				s.Nil(rpcCtx)
				s.Equal(rc.isValid(), false)
			}
		} else {
			s.Nil(rpcCtx)
		}
		if rpcCtx != nil {
			lastPeerId = rpcCtx.Peer.Id
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
		selector, err = newReplicaSelectorV2(s.cache, region.Region, req, tikvrpc.TiKV, WithMatchLabels(labels))
		s.Nil(err)
		selector.replicas[mi].store.labels = labels
		leaderIdx := selector.region.getStore().workTiKVIdx
		leader := selector.replicas[leaderIdx]
		matchReplica := selector.replicas[mi]
		if leaderIdx != AccessIndex(mi) {
			// match label in follower
			for i := 1; i <= 6; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					// first try the match-label peer.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.Equal(replica.peer.Id, matchReplica.peer.Id)
					s.NotEqual(replica.peer.Id, leader.peer.Id)
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, true)
					s.Equal(req.ReplicaRead, false)
				} else if i == 2 {
					// retry in leader by leader read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.Equal(replica.peer.Id, leader.peer.Id)
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, false)
				} else if i == 3 { // diff from v1
					// retry remain follower by follower read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.NotEqual(replica.peer.Id, matchReplica.peer.Id)
					s.NotEqual(replica.peer.Id, leader.peer.Id)
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		} else {
			// match label in leader
			for i := 1; i <= 5; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.Equal(replica.peer.Id, leader.peer.Id)
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, true)
					s.Equal(req.ReplicaRead, false)
				} else if i == 2 {
					// retry in leader without stale read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.Equal(replica.peer.Id, leader.peer.Id)
					s.True(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 2)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, false)
				} else if i <= 4 {
					// retry remain followers by follower read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					replica := selector.target
					s.NotEqual(replica.peer.Id, leader.peer.Id)
					s.False(replica.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, replica.peer.Id)
					s.Equal(replica.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		}
		selector.replicas[mi].store.labels = nil
	}
}
