package tikvrpc

import "C"
import (
	"github.com/pingcap/errors"
	"go.uber.org/atomic"
)

// EnableTiKVLocalCall indicates whether to enable TiKV local calls.
var EnableTiKVLocalCall = atomic.NewBool(false)

// when modify below constants, also modify files under generator
const (
	KvGet                       = 1
	KvScan                      = 2
	KvPrewrite                  = 3
	KvPessimisticLock           = 4
	KVPessimisticRollback       = 5
	KvTxnHeartBeat              = 6
	KvCheckTxnStatus            = 7
	KvCheckSecondaryLocks       = 8
	KvCommit                    = 9
	KvImport                    = 10
	KvCleanup                   = 11
	KvBatchGet                  = 12
	KvBatchRollback             = 13
	KvScanLock                  = 14
	KvResolveLock               = 15
	KvGC                        = 16
	KvDeleteRange               = 17
	KvPrepareFlashbackToVersion = 18
	KvFlashbackToVersion        = 19
	KvFlush                     = 20
	KvBufferBatchGet            = 21
	RawGet                      = 22
	RawBatchGet                 = 23
	RawPut                      = 24
	RawBatchPut                 = 25
	RawDelete                   = 26
	RawBatchDelete              = 27
	RawScan                     = 28
	RawDeleteRange              = 29
	RawBatchScan                = 30
	RawGetKeyTTL                = 31
	RawCompareAndSwap           = 32
	RawChecksum                 = 33
	UnsafeDestroyRange          = 34
	RegisterLockObserver        = 35
	CheckLockObserver           = 36
	RemoveLockObserver          = 37
	PhysicalScanLock            = 38
	Coprocessor                 = 39
	CoprocessorStream           = 0 // 40, streaming RPC, not supported yet
	BatchCoprocessor            = 0 // 41, streaming RPC, not supported yet
	RawCoprocessor              = 42
	Raft                        = 0 // 43, streaming RPC, not supported yet
	BatchRaft                   = 0 // 44, streaming RPC, not supported yet
	Snapshot                    = 0 // 45, streaming RPC, not supported yet
	TabletSnapshot              = 0 // 46, streaming RPC, not supported yet
	SplitRegion                 = 47
	ReadIndex                   = 48
	MvccGetByKey                = 49
	MvccGetByStartTs            = 50
	BatchCommands               = 0 // 51, streaming RPC, not supported yet
	DispatchMPPTask             = 0 // 52, mpp, not supported yet
	CancelMPPTask               = 0 // 53, mpp, not supported yet
	EstablishMPPConnection      = 0 // 54, mpp, not supported yet
	IsAlive                     = 0 // 55, mpp, not supported yet
	ReportMPPTaskStatus         = 0 // 56, mpp, not supported yet
	CheckLeader                 = 57
	GetStoreSafeTS              = 58
	GetLockWaitInfo             = 59
	Compact                     = 60
	GetLockWaitHistory          = 61
	GetTiFlashSystemTable       = 0 // 62, TiFlash related RPCs, not supported yet
	TryAddLock                  = 0 // 63, TiFlash related RPCs, not supported yet
	TryMarkDelete               = 0 // 64, TiFlash related RPCs, not supported yet
	EstablishDisaggTask         = 0 // 65, TiFlash related RPCs, not supported yet
	CancelDisaggTask            = 0 // 66, TiFlash related RPCs, not supported yet
	FetchDisaggPages            = 0 // 67, TiFlash related RPCs, not supported yet
	GetDisaggConfig             = 0 // 68, TiFlash related RPCs, not supported yet
	GetHealthFeedback           = 69
	BroadcastTxnStatus          = 70
)

var errEnumMapping = map[uint64]error{
	1: errors.New("TiKVNotReady"),
	2: errors.New("InvalidMethod"),
	3: errors.New("SerializationError"),
	4: errors.New("StorageError"),
	5: errors.New("ReadPoolSchedTooBusy"),
}

//go:generate go run generator/pkdb_gen_send_response.go generator/pkdb_type_mapping.go
//go:generate go run generator/pkdb_gen_call_ffi.go generator/pkdb_type_mapping.go
