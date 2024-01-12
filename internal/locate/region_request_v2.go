package locate

import (
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"math/rand"
	"sync/atomic"
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
	replicas    []*Replica
	targetIdx   AccessIndex
}

func (s *ReplicaSelector) targetReplica() *Replica {
	if s.targetIdx >= 0 && int(s.targetIdx) < len(s.replicas) {
		return s.replicas[s.targetIdx]
	}
	return nil
}

type ReplicaSelectStrategy interface {
	//calculateScore(replica *Replica) int
	next() *RPCContext
}

const (
	maxLeaderReplicaAttempt   = 10
	maxFollowerReplicaAttempt = 1
)

type ReplicaSelectLeaderStrategy struct{}

func (s *ReplicaSelectLeaderStrategy) next(replicas []*Replica, region *Region) *Replica {
	leader := replicas[region.getStore().workTiKVIdx]
	if leader.store.getLivenessState() == reachable && leader.attempts < maxReplicaAttempt {
		return leader
	}
	// try idle replica?
	// try follower?
	return nil
}

type ReplicaSelectMixedStrategy struct {
	staleRead    bool
	tryLeader    bool
	preferLeader bool
	learnerOnly  bool
	labels       []*metapb.StoreLabel
	stores       []uint64
}

func (s *ReplicaSelectMixedStrategy) next(selector *ReplicaSelector, replicas []*Replica, region *Region) *Replica {
	leaderIdx := region.getStore().workTiKVIdx
	maxScore := -1
	maxScoreIdxes := make([]int, 0, len(replicas))
	reloadRegion := false
	for i, r := range replicas {
		epochStale := r.isEpochStale()
		liveness := r.store.getLivenessState()
		if epochStale && liveness == reachable && r.store.getResolveState() == resolved {
			reloadRegion = true
		}
		if epochStale || r.isExhausted(maxFollowerReplicaAttempt) || liveness == unreachable || r.deadlineErrUsingConfTimeout {
			// the replica is not available.
			continue
		}
		score := s.calculateScore(r, AccessIndex(i), leaderIdx)
		if score > maxScore {
			maxScore = score
			maxScoreIdxes = append(maxScoreIdxes[:0], i)
		} else if score == maxScore && score > -1 {
			maxScoreIdxes = append(maxScoreIdxes, i)
		}
	}
	if reloadRegion {
		selector.regionCache.scheduleReloadRegion(selector.region)
	}
	if len(maxScoreIdxes) == 1 {
		idx := maxScoreIdxes[0]
		return replicas[idx]
	} else if len(maxScoreIdxes) > 1 {
		// if there are more than one replica with the same max score, we will randomly select one
		idx := maxScoreIdxes[rand.Intn(len(maxScoreIdxes))]
		return replicas[idx]
	}

	leader := replicas[leaderIdx]
	leaderEpochStale := leader.isEpochStale()
	leaderUnreachable := leader.store.getLivenessState() != reachable
	leaderExhausted := s.IsLeaderExhausted(leader)
	leaderInvalid := leaderEpochStale || leaderUnreachable || leaderExhausted
	if leaderInvalid || leader.deadlineErrUsingConfTimeout {
		// todo: consider log in outside?
		//logutil.Logger(ctx).Warn("unable to find valid leader",
		//	zap.Uint64("region", region.GetID()),
		//	zap.Bool("epoch-stale", leaderEpochStale),
		//	zap.Bool("unreachable", leaderUnreachable),
		//	zap.Bool("exhausted", leaderExhausted),
		//	zap.Bool("kv-timeout", leader.deadlineErrUsingConfTimeout),
		//	zap.Bool("stale-read", s.staleRead))
		// In stale-read, the request will fallback to leader after the local follower failure.
		// If the leader is also unavailable, we can fallback to the follower and use replica-read flag again,
		// The remote follower not tried yet, and the local follower can retry without stale-read flag.
		// If leader tried and received deadline exceeded error, try follower.
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
		selector.invalidateRegion()
	}
	return leader
}

const (
	// define the score of priority.
	scoreOfLabelMatch   = 3
	scoreOfPreferLeader = 2
	scoreOfNormalPeer   = 1
	scoreOfNotSlow      = 1
)

func (s *ReplicaSelectMixedStrategy) calculateScore(r *Replica, idx, leaderIdx AccessIndex) int {
	score := 0
	if idx == leaderIdx {
		// is leader
		if s.preferLeader {
			score += scoreOfPreferLeader
		} else if s.tryLeader {
			score += scoreOfNormalPeer
		}
	} else {
		if s.learnerOnly {
			if r.peer.Role == metapb.PeerRole_Learner {
				score += scoreOfNormalPeer
			}
		} else {
			score += scoreOfNormalPeer
		}
	}
	// score = score + maxFollowerReplicaAttempt - r.attempts // must be score += 1 here.
	if r.store.IsStoreMatch(s.stores) && r.store.IsLabelsMatch(s.labels) {
		score += scoreOfLabelMatch
	}
	if !r.store.isSlow() {
		score += scoreOfNotSlow
	}
	return score
}

func (s *ReplicaSelectMixedStrategy) IsLeaderExhausted(leader *Replica) bool {
	// Allow another extra retry for the following case:
	// 1. The stale read is enabled and leader peer is selected as the target peer at first.
	// 2. Data is not ready is returned from the leader peer.
	// 3. Stale read flag is removed and processing falls back to snapshot read on the leader peer.
	// 4. The leader peer should be retried again using snapshot read.
	if s.staleRead {
		return leader.isExhausted(maxFollowerReplicaAttempt + 1)
	} else {
		return leader.isExhausted(maxFollowerReplicaAttempt)
	}
}

func (r *Replica) isEpochStale() bool {
	return r.epoch != atomic.LoadUint32(&r.store.epoch)
}

func (r *Replica) isExhausted(maxAttempt int) bool {
	return r.attempts >= maxAttempt
}

func (s *ReplicaSelector) buildRPCContext(bo *retry.Backoffer, r *Replica) (*RPCContext, error) {
	if r.isEpochStale() {
		// TODO(youjiali1995): Is it necessary to invalidate the region?
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("stale_store").Inc()
		s.invalidateRegion()
		return nil, nil
	}
	rpcCtx := &RPCContext{
		Region:     s.region.VerID(),
		Meta:       s.region.meta,
		Peer:       r.peer,
		Store:      r.store,
		AccessMode: tiKVOnly,
		TiKVNum:    len(s.replicas),
	}
	// Set leader addr
	addr, err := s.regionCache.getStoreAddr(bo, s.region, r.store)
	if err != nil {
		return nil, err
	}
	if len(addr) == 0 {
		return nil, nil
	}
	rpcCtx.Addr = addr
	return rpcCtx, nil
}

func (s *ReplicaSelector) invalidateRegion() {
	if s.region != nil {
		s.region.invalidate(Other)
	}
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

func (s *ReplicaSelector) NeedReloadRegion(ctx *RPCContext) bool {
	// TODO:...
	return true
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
