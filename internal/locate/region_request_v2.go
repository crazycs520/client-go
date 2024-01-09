package locate

import (
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
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
	state       selectorState
	// replicas[targetIdx] is the replica handling the request this time
	targetIdx AccessIndex
}

type selectorStateV2 interface {
	next(*retry.Backoffer, *replicaSelector) (*RPCContext, error)
	onSendSuccess(*replicaSelector)
	onSendFailure(*retry.Backoffer, *replicaSelector, error)
	onNoLeader(*replicaSelector)
}

func (s *RegionRequestSenderV2) SendReqCtx(
	bo *retry.Backoffer,
	req *tikvrpc.Request,
	regionID RegionVerID,
	timeout time.Duration,
	et tikvrpc.EndpointType,
	opts ...StoreSelectorOption,
) (
	resp *tikvrpc.Response,
	rpcCtx *RPCContext,
	retryTimes int,
	err error,
) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("regionRequest.SendReqCtx", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	if val, err := util.EvalFailpoint("tikvStoreSendReqResult"); err == nil {
		if s, ok := val.(string); ok {
			switch s {
			case "timeout":
				return nil, nil, 0, errors.New("timeout")
			case "GCNotLeader":
				if req.Type == tikvrpc.CmdGC {
					return &tikvrpc.Response{
						Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
					}, nil, 0, nil
				}
			case "PessimisticLockNotLeader":
				if req.Type == tikvrpc.CmdPessimisticLock {
					return &tikvrpc.Response{
						Resp: &kvrpcpb.PessimisticLockResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
					}, nil, 0, nil
				}
			case "GCServerIsBusy":
				if req.Type == tikvrpc.CmdGC {
					return &tikvrpc.Response{
						Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
					}, nil, 0, nil
				}
			case "busy":
				return &tikvrpc.Response{
					Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
				}, nil, 0, nil
			case "requestTiDBStoreError":
				if et == tikvrpc.TiDB {
					return nil, nil, 0, errors.WithStack(tikverr.ErrTiKVServerTimeout)
				}
			case "requestTiFlashError":
				if et == tikvrpc.TiFlash {
					return nil, nil, 0, errors.WithStack(tikverr.ErrTiFlashServerTimeout)
				}
			}
		}
	}

	// If the MaxExecutionDurationMs is not set yet, we set it to be the RPC timeout duration
	// so TiKV can give up the requests whose response TiDB cannot receive due to timeout.
	if req.Context.MaxExecutionDurationMs == 0 {
		req.Context.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
	}

	retryTimes = 0
	defer func() {
		if retryTimes > 0 {
			metrics.TiKVRequestRetryTimesHistogram.Observe(float64(retryTimes))
		}
	}()

	//var staleReadCollector *staleReadMetricsCollector
	//if req.StaleRead {
	//	staleReadCollector = &staleReadMetricsCollector{}
	//	defer func() {
	//		if retryTimes == 0 {
	//			metrics.StaleReadHitCounter.Add(1)
	//		} else {
	//			metrics.StaleReadMissCounter.Add(1)
	//		}
	//	}()
	//}

	for {
		if retryTimes > 0 {
			if retryTimes%100 == 0 {
				logutil.Logger(bo.GetCtx()).Warn(
					"retry",
					zap.Uint64("region", regionID.GetID()),
					zap.Int("times", retryTimes),
				)
			}
		}
	}

}

func (selector *replicaSelectorV2) next() {
	switch selector.state.(type) {
	case *accessKnownLeader:
	}
}
