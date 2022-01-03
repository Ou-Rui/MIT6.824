# MIT 6.824

## 更新日志
虽然到4B才想起来写...

### 2022.1.2
PASS: TestStaticShards
- **杀虫**
  - query config时，可能master根本没有config，此时需要判断
  - kv.mck.Query()时，需要kv.mu.Unlock()，否则如果sm死了，
    kv会永久死锁
- **Snapshot**
  - 追加OnCharge字段，为了在重启后记得负责过哪些shard
- **添加方法**
  - kv.readOnCharge():根据OnCharge更新readyShard和ready
- **ConfigIndex**
  - 如果op.ci与kv.ci不匹配，说明在log replication的过程中config
    变化了， 该key可能易主，将result标记为redo，将请求打回重做
- **格式**
  - shardState --> ss
  - ss.index --> ss.ci
  - sendSRHandler(), 把对给定queryIndex和shard的请求提取成方法

### 2022.1.3
PASS 1000 TestJoinLeave
- ***杀虫***
  - 将resultCond的唤醒添加到queryLoop中，避免了尴尬时点的睡死
  - redo打回后删除resultMap中的记录，否则永远都会被打回。。
- **ShardRequest**
  - sendSRHandler()中，更正了需要redo当前queryIndex的判据
    - 目标server的ci较小，说明他们还没更新到同一个config，
      等他们更新了再问一次
    - 没有联系到目标server的leader，具体原因有：
      - 网络错误
      - 还没有选出leader
      - 表现为 nwOrWlFailCnt == len(servers)
## 还需要考虑的点
