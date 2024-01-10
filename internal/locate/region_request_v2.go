package locate

import (
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
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
	targetIdx   AccessIndex
}

func (s *ReplicaSelector) targetReplica() *replica {
	if s.targetIdx >= 0 && int(s.targetIdx) < len(s.replicas) {
		return s.replicas[s.targetIdx]
	}
	return nil
}

func (s *ReplicaSelector) onSendFailure(bo *retry.Backoffer, ctx *RPCContext, err error) {
	switch s.endpointTp {
	case tikvrpc.TiKV:
		metrics.RegionCacheCounterWithSendFail.Inc()
		replica := s.targetReplica()
		if replica == nil {
			return
		}
		store := replica.store
		if store.storeType != tikvrpc.TiKV {
			return
		}
		store.setLivenessState(unknown) // mark unknown and notify health worker to check it.
		store.startHealthCheckLoopIfNeeded(s.regionCache, unknown)
	case tikvrpc.TiFlash, tikvrpc.TiFlashCompute:
		s.regionCache.OnSendFail(bo, ctx, s.NeedReloadRegion(ctx), err)
	case tikvrpc.TiDB:
		return
	}
}

func (s *RegionRequestSender) onNotLeader(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, notLeader *errorpb.NotLeader,
) (shouldRetry bool, err error) {
	if ctx.Store.storeType != tikvrpc.TiKV {
		return
	}
	if notLeader.GetLeader() == nil {
		// The peer doesn't know who is the current leader. Generally it's because
		// the Raft group is in an election, but it's possible that the peer is
		// isolated and removed from the Raft group. So it's necessary to reload
		// the region from PD.
		//s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, NoLeader)
		if err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("not leader: %v, ctx: %v", notLeader, ctx)); err != nil {
			return false, err
		}
		//return false, nil
	} else {
		// don't backoff if a new leader is returned.
		s.regionCache.UpdateLeader(ctx.Region, notLeader.GetLeader(), ctx.AccessIdx)
	}
	return s.selector.onNotLeader(bo, ctx, notLeader)
}
func (s *ReplicaSelector) onNotLeader(bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader) (shouldRetry bool, err error) {
	switch s.endpointTp {
	case tikvrpc.TiKV:
		if notLeader.GetLeader() == nil {
			// s.onNoLeader()
			// region need mark to reload.
		}
		return true, nil
	default:
		s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, NoLeader)
		return false, nil
	}
}
