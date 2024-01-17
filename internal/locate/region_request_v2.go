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
) (*ReplicaSelector, error) {
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
	if req.ReplicaReadType == kv.ReplicaReadPreferLeader {
		WithPerferLeader()(&option)
	}
	isReadOnlyReq := false
	switch req.Type {
	case tikvrpc.CmdGet, tikvrpc.CmdBatchGet, tikvrpc.CmdScan,
		tikvrpc.CmdCop, tikvrpc.CmdBatchCop, tikvrpc.CmdCopStream:
		isReadOnlyReq = true
	}

	return &ReplicaSelector{
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

func (s *ReplicaSelector) next(bo *retry.Backoffer, req *tikvrpc.Request) (rpcCtx *RPCContext, err error) {
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
			s.target = strategy.next(s, s.replicas, s.region)
		}
	default:
		if s.isStaleRead && s.attempts == 2 {
			// For stale read second retry, try leader by leader read first.
			strategy := ReplicaSelectLeaderStrategy{}
			s.target = strategy.next(s.replicas, s.region)
			if s.target != nil {
				req.StaleRead = false
				req.ReplicaRead = false
			}
		}
		if s.target == nil {
			strategy := ReplicaSelectMixedStrategy{
				staleRead:    s.isStaleRead,
				tryLeader:    req.ReplicaReadType == kv.ReplicaReadMixed || req.ReplicaReadType == kv.ReplicaReadPreferLeader,
				preferLeader: req.ReplicaReadType == kv.ReplicaReadPreferLeader,
				learnerOnly:  req.ReplicaReadType == kv.ReplicaReadLearner,
				labels:       s.option.labels,
				stores:       s.option.stores,
			}
			s.target = strategy.next(s, s.replicas, s.region)
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
		return leader
	}
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
	hasDeadlineExceededErr := false
	for i, r := range replicas {
		epochStale := r.isEpochStale()
		liveness := r.store.getLivenessState()
		if epochStale && liveness == reachable && r.store.getResolveState() == resolved {
			reloadRegion = true
		}
		hasDeadlineExceededErr = hasDeadlineExceededErr || r.deadlineErrUsingConfTimeout
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
		// todo: use store slow score to select a faster one.
		idx := maxScoreIdxes[rand.Intn(len(maxScoreIdxes))]
		return replicas[idx]
	}

	leader := replicas[leaderIdx]
	leaderEpochStale := leader.isEpochStale()
	leaderUnreachable := leader.store.getLivenessState() != reachable
	leaderExhausted := leader.isExhausted(maxFollowerReplicaAttempt)
	leaderInvalid := leaderEpochStale || leaderUnreachable || leaderExhausted
	if leaderInvalid {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
		if hasDeadlineExceededErr {
			// when meet deadline exceeded error, do fast retry without invalidate region cache.
			return nil
		}
		selector.invalidateRegion() // is exhausted, Is need to invalidate the region?
		return nil
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
	r.attempts++
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
