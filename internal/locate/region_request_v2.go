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

type ReplicaSelectorV2 struct {
	endpointTp      tikvrpc.EndpointType
	replicaReadType kv.ReplicaReadType
	isStaleRead     bool
	isReadOnlyReq   bool

	regionCache *RegionCache
	region      *Region
	replicas    []*Replica
	option      storeSelectorOp
	target      *Replica
	attempts    int
}

func newReplicaSelectorV2(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, et tikvrpc.EndpointType, opts ...StoreSelectorOption,
) (*ReplicaSelectorV2, error) {
	cachedRegion := regionCache.GetCachedRegionWithRLock(regionID)
	if cachedRegion == nil || !cachedRegion.isValid() {
		return nil, errors.New("cached region invalid")
	}

	regionStore := cachedRegion.getStore()
	replicas := make([]*Replica, 0, regionStore.accessStoreNum(tiKVOnly))
	for _, storeIdx := range regionStore.accessIndex[tiKVOnly] {
		replicas = append(
			replicas, &Replica{
				store:    regionStore.stores[storeIdx],
				peer:     cachedRegion.meta.Peers[storeIdx],
				epoch:    regionStore.storeEpochs[storeIdx],
				attempts: 0,
			},
		)
	}

	option := storeSelectorOp{}
	for _, op := range opts {
		op(&option)
	}
	isReadOnlyReq := false
	switch req.Type {
	case tikvrpc.CmdGet, tikvrpc.CmdBatchGet, tikvrpc.CmdScan,
		tikvrpc.CmdCop, tikvrpc.CmdBatchCop, tikvrpc.CmdCopStream:
		isReadOnlyReq = true
	}

	return &ReplicaSelectorV2{
		et,
		req.ReplicaReadType,
		req.StaleRead,
		isReadOnlyReq,
		regionCache,
		cachedRegion,
		replicas,
		option,
		nil,
		0,
	}, nil
}

func (s *ReplicaSelectorV2) next(bo *retry.Backoffer, req *tikvrpc.Request) (rpcCtx *RPCContext, err error) {
	if !s.region.isValid() {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalid").Inc()
		return nil, nil
	}

	s.attempts++
	s.target = nil
	switch s.replicaReadType {
	case kv.ReplicaReadLeader:
		strategy := ReplicaSelectLeaderStrategy{}
		s.target = strategy.next(s.replicas, s.region)
		if s.target == nil {
			strategy := ReplicaSelectMixedStrategy{}
			s.target = strategy.next(s, s.region)
		}
	default:
		if s.isStaleRead && s.attempts == 2 {
			// For stale read second retry, try leader by leader read.
			strategy := ReplicaSelectLeaderStrategy{}
			s.target = strategy.next(s.replicas, s.region)
			if s.target != nil {
				req.StaleRead = false
				req.ReplicaRead = false
			}
		}
		if s.target == nil {
			strategy := ReplicaSelectMixedStrategy{
				tryLeader:    req.ReplicaReadType == kv.ReplicaReadMixed || req.ReplicaReadType == kv.ReplicaReadPreferLeader,
				preferLeader: req.ReplicaReadType == kv.ReplicaReadPreferLeader,
				learnerOnly:  req.ReplicaReadType == kv.ReplicaReadLearner,
				labels:       s.option.labels,
				stores:       s.option.stores,
			}
			s.target = strategy.next(s, s.region)
			if s.target != nil {
				if s.isStaleRead && s.attempts == 1 {
					// stale-read will only be used when first access.
					req.StaleRead = true
					req.ReplicaRead = false
				} else {
					// always use replica.
					// need check req read/write type?
					req.StaleRead = false
					req.ReplicaRead = s.isReadOnlyReq
				}
			}
		}
	}
	if s.target == nil {
		return nil, nil
	}
	return s.buildRPCContext(bo, s.target)
}

const (
	maxLeaderReplicaAttempt   = 10
	maxFollowerReplicaAttempt = 1
)

type ReplicaSelectLeaderStrategy struct{}

func (s *ReplicaSelectLeaderStrategy) next(replicas []*Replica, region *Region) *Replica {
	leader := replicas[region.getStore().workTiKVIdx]
	if leader.store.getLivenessState() == reachable && leader.attempts < maxReplicaAttempt {
		if !leader.isEpochStale() { // check leader epoch here, if leader.epoch faild, we can try other replicas. instead of buildRPCContext faild and invalidate region then retry.
			return leader
		}
	}
	return nil
}

type ReplicaSelectMixedStrategy struct {
	tryLeader    bool
	preferLeader bool
	learnerOnly  bool
	labels       []*metapb.StoreLabel
	stores       []uint64
}

func (s *ReplicaSelectMixedStrategy) next(selector *ReplicaSelectorV2, region *Region) *Replica {
	leaderIdx := region.getStore().workTiKVIdx
	replicas := selector.replicas
	maxScoreIdxes := make([]int, 0, len(replicas))
	maxScore := -1
	reloadRegion := false
	for i, r := range replicas {
		epochStale := r.isEpochStale()
		liveness := r.store.getLivenessState()
		if epochStale && liveness == reachable && r.store.getResolveState() == resolved {
			reloadRegion = true
		}
		if epochStale || r.isExhausted(maxFollowerReplicaAttempt) || liveness == unreachable {
			// the replica is not available or attempts is exhausted, skip it.
			continue
		}
		score := s.calculateScore(r, AccessIndex(i) == leaderIdx)
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
		// todo: use store slow score to select a faster one.
		idx := maxScoreIdxes[rand.Intn(len(maxScoreIdxes))]
		return replicas[idx]
	}

	metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
	for _, r := range replicas {
		if r.deadlineErrUsingConfTimeout {
			// when meet deadline exceeded error, do fast retry without invalidate region cache.
			return nil
		}
	}
	selector.invalidateRegion() // is exhausted, Is need to invalidate the region?
	return nil
}

const (
	// define the score of priority.
	scoreOfLabelMatch   = 3
	scoreOfPreferLeader = 2
	scoreOfNormalPeer   = 1
	scoreOfNotSlow      = 1
)

func (s *ReplicaSelectMixedStrategy) calculateScore(r *Replica, isLeader bool) int {
	score := 0
	if isLeader {
		if s.preferLeader {
			score += scoreOfPreferLeader
		} else if s.tryLeader {
			if len(s.labels) > 0 {
				// when has match labels, prefer leader than not-matched peers.
				score += scoreOfPreferLeader
			} else {
				score += scoreOfNormalPeer
			}
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
	if r.store.IsStoreMatch(s.stores) && r.store.IsLabelsMatch(s.labels) {
		score += scoreOfLabelMatch
	}
	if !r.store.isSlow() {
		score += scoreOfNotSlow
	}
	return score
}

func (r *Replica) isEpochStale() bool {
	return r.epoch != atomic.LoadUint32(&r.store.epoch)
}

func (r *Replica) isExhausted(maxAttempt int) bool {
	return r.attempts >= maxAttempt
}

func (s *ReplicaSelectorV2) buildRPCContext(bo *retry.Backoffer, r *Replica) (*RPCContext, error) {
	if r.isEpochStale() {
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
	r.attempts++
	return rpcCtx, nil
}

func (s *ReplicaSelectorV2) invalidateRegion() {
	if s.region != nil {
		s.region.invalidate(Other)
	}
}

func (s *ReplicaSelectorV2) onSendFailure(bo *retry.Backoffer, ctx *RPCContext, err error) {
	switch s.endpointTp {
	case tikvrpc.TiKV:
		metrics.RegionCacheCounterWithSendFail.Inc()
		if s.target == nil {
			return
		}
		store := s.target.store
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

func (s *ReplicaSelectorV2) NeedReloadRegion(ctx *RPCContext) bool {
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
func (s *ReplicaSelectorV2) onNotLeader(bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader) (shouldRetry bool, err error) {
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
