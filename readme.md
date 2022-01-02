# MIT 6.824

## 更新日志
虽然到4B才想起来写...

### 2022.1.2
PASS: TestStaticShards
- **杀虫**:
  - query config时，可能master根本没有config，此时需要判断
- **Snapshot**: 
  - 追加OnCharge字段，为了在重启后记得负责过哪些shard
- **添加方法**
  - kv.readOnCharge():根据OnCharge更新readyShard和ready
- **ConfigIndex**:
  - 如果op.ci与kv.ci不匹配，说明在log replication的过程中config
  变化了， 该key可能易主，将result标记为redo，将请求打回重做
- **格式**：
  - shardState --> ss
  - index --> ci
  - sendSRHandler(), 把对给定queryIndex和shard的请求提取成方法


## 还需要考虑的点
- kv.mck.Query()时，是否需要kv.mu.Unlock()