# How to invalid a region cache?

- `scheduleReload()`, set region.syncFlag to needSync.
  - For TiKV access path( replicaSelector != nil ), won't call `scheduleReload()`.
  - tryNewProxy may call it, but doesn't consider proxy(forwarding) case now.
  
- `invalidate()`, set lastAccess ts to -1, then the regino ttl check(10min) will fail when validate region.
  - replicaSelector
    - invalidateRegion call it.
      - accessFollower/tryFollower all replica is unavailable to access.
      - accessKnownLeader chose leader to build RPCContext, but leader replica isEpochStale, then call invalidateRegion.
        -  When/Why have isEpochStale?
          - store.epoch increase. 
            - replicaSelector.invalidateReplicaStore 
              - accessFollower/tryFollower/accessKnownLeader/tryNewProxy/accessByKnownProxy/tryIdleReplica onSendFailure, checkLiveness if target store is not reachable, call it.
              - tryNewProxy call it when no candidate ????
            - store.reResolve, if store is nil from PD or tombstone.
            - markRegionNeedBeRefill ???
    - updateLeader call it when new leader peer is not found.
  - insertRegionToCache may invalid old region.
  - onRegionError
    - OnRegionEpochNotMatch
    - MismatchPeerId
    - regionCache.UpdateLeader when meet NotLeader error.


replicaSelector.onSendFailure 目前会检查 store 是否可用，可以调整为将 store.liveness 设置为 unknown, 然后通知后台去检查，然后 replicaSelector 尽快切下一个 replica 去访问。

明确每个模块的职责范围，避免耦合。
- replicaSelector 只负责选择合适的 replica。
- regionCache 只负责缓存 region 和 region 的更新。
- storeCache 负责 store 的缓存和 store 的更新，状态检查
