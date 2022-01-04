# MIT 6.824

## 还需要考虑的点
- 不能问到一个leader就不问其他人了，因为partition时可能有多个leader
  - 至少要问大多数人，能实现吗？还是要问所有人？
  - 如果只问大多数人，那必然是以Term最高为准，但这个人可能不是leader
    如何判断与Client的交互情况？
  - 或者大多数+包含最高Term的leader
- 先apply完所有，再向别人要shard，和给别人发shard
  - 要到全部shard后，ready开始处理请求
- 不匹配ci的op到底要不要apply，如果在apply前后来了ShardRequest
  不就不一致了吗


## 更新日志
虽然到4B才想起来写...

### 2022.1.4
- **找谁要shard？**
  - 原先：只问leader要shard，找到一个正确返回的leader就认为对了
  - 问题：Partition时可能会有多个leader
  - 改进：需要问过大多数server，且其中包含一个Term最大的leader
    - 正确性：大多数server中至少会有1个server在Quorom中，
      具有Quorom的Term。因此其他非法leader的Term不会是最大的，
      不会被误判为合法leader。
    - 可用性：只需要大多数server(包含合法leader)可达就可用
- **什么时候可以为别人提供shard**
  - 原先的条件：
    1. not killed
    2. leader
    3. configIndex一致
    4. on charge
  - 问题：有可能存在start但未apply的log，apply该log前后如果给不同
    server提供shard，将导致提供的shard不一致
  - 改进：添加了ExpCommitIndex数据结构，记录每个shard中已经start的
    最大logIndex，仅当该shard中所有start的log都apply了才能提供
    - 回复时不需要是leader，但不是leader不会被采用，用于辅助判断
    - ExpCommitIndex需要持久化
  - 待考虑：是否需要考虑ci，如果是，如何gc

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


