package main

import "runtime"

const nonLinux = runtime.GOOS != "linux"

// Define method-response type mappings
var mappings = []struct {
	Method     string
	TypePrefix string
	CopyResp   bool
	UseChan    bool // if false, use event fd
}{
	{"KvGet", "kvrpcpb.Get", false, false},
	{"KvScan", "kvrpcpb.Scan", false, false},
	{"KvPrewrite", "kvrpcpb.Prewrite", false, false},
	{"KvPessimisticLock", "kvrpcpb.PessimisticLock", false, false},
	{"KVPessimisticRollback", "kvrpcpb.PessimisticRollback", false, false},
	{"KvTxnHeartBeat", "kvrpcpb.TxnHeartBeat", false, false},
	{"KvCheckTxnStatus", "kvrpcpb.CheckTxnStatus", false, false},
	{"KvCheckSecondaryLocks", "kvrpcpb.CheckSecondaryLocks", false, false},
	{"KvCommit", "kvrpcpb.Commit", false, false},
	{"KvImport", "kvrpcpb.Import", false, false},
	{"KvCleanup", "kvrpcpb.Cleanup", false, false},
	{"KvBatchGet", "kvrpcpb.BatchGet", false, false},
	{"KvBatchRollback", "kvrpcpb.BatchRollback", false, false},
	{"KvScanLock", "kvrpcpb.ScanLock", false, false},
	{"KvResolveLock", "kvrpcpb.ResolveLock", false, false},
	{"KvGC", "kvrpcpb.GC", false, false},
	{"KvDeleteRange", "kvrpcpb.DeleteRange", false, false},
	{"KvPrepareFlashbackToVersion", "kvrpcpb.PrepareFlashbackToVersion", false, false},
	{"KvFlashbackToVersion", "kvrpcpb.FlashbackToVersion", false, false},
	{"KvFlush", "kvrpcpb.Flush", false, false},
	{"KvBufferBatchGet", "kvrpcpb.BufferBatchGet", false, false},
	{"RawGet", "kvrpcpb.RawGet", false, false},
	{"RawBatchGet", "kvrpcpb.RawBatchGet", false, false},
	{"RawPut", "kvrpcpb.RawPut", false, false},
	{"RawBatchPut", "kvrpcpb.RawBatchPut", false, false},
	{"RawDelete", "kvrpcpb.RawDelete", false, false},
	{"RawBatchDelete", "kvrpcpb.RawBatchDelete", false, false},
	{"RawScan", "kvrpcpb.RawScan", false, false},
	{"RawDeleteRange", "kvrpcpb.RawDeleteRange", false, false},
	{"RawBatchScan", "kvrpcpb.RawBatchScan", false, false},
	{"RawGetKeyTTL", "kvrpcpb.RawGetKeyTTL", false, false},
	{"RawCompareAndSwap", "kvrpcpb.RawCAS", false, false},
	{"RawChecksum", "kvrpcpb.RawChecksum", false, false},
	{"UnsafeDestroyRange", "kvrpcpb.UnsafeDestroyRange", false, false},
	{"RegisterLockObserver", "kvrpcpb.RegisterLockObserver", false, false},
	{"CheckLockObserver", "kvrpcpb.CheckLockObserver", false, false},
	{"RemoveLockObserver", "kvrpcpb.RemoveLockObserver", false, false},
	{"PhysicalScanLock", "kvrpcpb.PhysicalScanLock", false, false},
	{"Coprocessor", "coprocessor.", true, false},
	{"RawCoprocessor", "kvrpcpb.RawCoprocessor", false, false},
	{"SplitRegion", "kvrpcpb.SplitRegion", false, false},
	{"ReadIndex", "kvrpcpb.ReadIndex", false, false},
	{"MvccGetByKey", "kvrpcpb.MvccGetByKey", false, false},
	{"MvccGetByStartTs", "kvrpcpb.MvccGetByStartTs", false, false},
	{"CheckLeader", "kvrpcpb.CheckLeader", false, false},
	{"GetStoreSafeTS", "kvrpcpb.StoreSafeTS", false, false},
	{"GetLockWaitInfo", "kvrpcpb.GetLockWaitInfo", false, false},
	{"Compact", "kvrpcpb.Compact", false, false},
	{"GetLockWaitHistory", "kvrpcpb.GetLockWaitHistory", false, false},
	{"GetHealthFeedback", "kvrpcpb.GetHealthFeedback", false, false},
	{"BroadcastTxnStatus", "kvrpcpb.BroadcastTxnStatus", false, false},
}

var method2Mappings = map[string]struct {
	TypePrefix string
	CopyResp   bool
	UseChan    bool
}{}

func init() {
	for i, _ := range mappings {
		// always use channel under non-linux
		mappings[i].UseChan = mappings[i].UseChan || nonLinux
	}
	for _, m := range mappings {
		method2Mappings[m.Method] = struct {
			TypePrefix string
			CopyResp   bool
			UseChan    bool
		}{m.TypePrefix, m.CopyResp, m.UseChan}
	}
}
