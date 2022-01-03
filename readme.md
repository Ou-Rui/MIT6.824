# MIT 6.824

## 更新日志
虽然到4B才想起来写...

### 2022.1.3
PASS 1000 TestJoinLeave  
DEBUG TestSnapshot
- ***杀虫***
  - 将resultCond的唤醒添加到queryLoop中，避免了尴尬时点的睡死
  - redo打回后删除resultMap中的记录，否则永远都会被打回。。
  - 请求shard如果请求到自己，说明自己负责过，可以下一个
  - 得到shard后的处理逻辑不对，改为：如果ready结束，如果unready
    请求下一个shard
- **ShardRequest**
  - sendSRHandler()中，更正了需要redo当前queryIndex的判据
    - 目标server的ci较小，说明他们还没更新到同一个config，
      等他们更新了再问一次
    - 没有联系到目标server的leader，具体原因有：
      - 网络错误
      - 还没有选出leader
      - 表现为 nwOrWlFailCnt == len(servers)
  - 不能找自己要shard，会死锁
- **Apply**
  - apply不应该检查op.ci与kv.ci的一致性
    - 因为各server仅在raft层保持一致
    - 对于raft层传上来的数据必须一视同仁
    - 具体而言：各server可能在不同的时间更新ci
- **Reply to Client**
  - 回复客户端的动作需要区分
  - 因为只有leader和client有交互，因此可以区分
  - op.ci与kv.ss.ci如果不匹配要打回重做

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


## 还需要考虑的点
- 不匹配ci的op到底要不要apply，如果在apply前后来了ShardRequest 
  不就不一致了吗