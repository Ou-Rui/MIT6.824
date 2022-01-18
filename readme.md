# MIT 6.824

## Lab4B 方案描述
### 只有leader负责要config
- 要到config后不直接更新，生成log，共识后更新
- 这样一来**kv.ss.ci**就是共识后的结果
- 此时，apply时就可以将op.ci与kv.ss.ci不匹配的log废弃，
  不apply，并将请求打回

### 只有leader负责要shard
- leader去要shard，要到之后压缩放在log里
- Commit之后再更新data, resultMap, onCharge...
- 因此给出的shard，一定是在Group中有多数共识的

### 被请求的server在什么情况下给出shard
- Not be killed
- ConfigIndex: ***kv.ss.ci >= QueryIndex***
- 自己可以不是leader
- OnCharge = QueryIndex

### leader如何确定要到的shard合法
- 因为收到的shard一定是共识过的结果，因此只要拿到一个就行
- 收到时检查：killed, ci

### 什么情况下，请求者应该继续等待该QueryIndex
- ci不对

### 或者说，什么时候该QueryIndex没救了
- 都说Not OnCharge
- 连续网络错误

### 什么时候开始提供服务
- ci与client匹配，OnCharge[shard] == ci

## Challenge1 方案
### 什么时候可以删除旧shard
- 新Owner的`ShardLog` Apply之后
  - go一个线程向旧Owner发送`GcRequest`
  - 需要在`ShardLog`中记录旧Owner的信息
  - 不需要记录还有谁没删除，因为重启后会重新Apply `ShardLog`，到时会再
    go一个线程

### 旧Owner怎么处理ShardGcRequest
- 收到`GcRequest`时检查：
  - ci, **是否已经删除**
  - 似乎不需要检查ci
- Start `GcLog`, Apply时删除Shard
  - 维护一个已删除的Shard列表：`DeletedShards`，val为删除的shard对应的
    ci
  - 如果`GcRequest`中的`DeleteCi` < `DeletedShards[Shard]`，说明
    已经删除过

### NOTE
- 注意不要给自己发删除请求


## 更新日志
虽然到4B才想起来写...

### 2022.1.18
PASS ALL TEST !!!

- 原来Challenge2已经实现了啊，那没事了

### 2022.1.17
PASS TestChallenge1Delete  
DEBUG TestChallenge1Concurrent

- 初步完成Challenge1
- **DEBUG**
  - 不应该删除resultMap
- `GcRequest()` RPC
  - 新owner向旧owner发送`GcRequest`, 让旧owner删除shard数据

### 2022.1.12
PASS 4B!!!  500次

#### **DEBUG**
- 修正了ShardMaster AlreadyDone的逻辑
- sm的config不一致，将计算config提前到rf.Start()之前
- 无法区分Killed和Network Error
  - 暂时方案：网络错误重复**10**次才觉得没戏
  - magic number...

### 2022.1.11
PASS Concurrent1/2  
DEBUG Unreliable1

#### **DEBUG**
- Config, Shard Log也要更新 CommitIndex, CommitTerm
- 开局询问config[0]的逻辑修正，第一次问到的时候可能leader还没选出来，
  导致log无法Start
- 不Apply已经OnCharge的ShardLog

#### 被kill的server会被切断网络连接
- 意味着被kill的server被调用SR RPC时，不会进入处理函数
- 返回的错误不会是ErrKilled，而是 ok = false
- 所以 !ok 时，invalid也应该++
- **以后Unreliable时可能会有问题？**

#### Snapshot
- kv.ss.ReadyShard也要进Snapshot持久化

#### Deprecated
- ShardRequest() Err: ErrKilled

### 2022.1.10
继续重构...  
PASS Snapshot JoinLeave Static

#### DEBUG
- unlock写错括号，导致死锁
- queryIndex应该从kv.ss.ci开始，而非kv.ss.ci-1
  - 因为可能重启前就是自己负责
- 修改了询问config 1的逻辑，configs[1] == nil 时询问

#### getShardData(shard int)
- 获取指定shard的data和resultMap

#### sendSRHandler()
- 发送ShardRequest的逻辑
- 如果是自己负责，只要判断 onCharge >= QueryIndex ?
- 如果是其他组负责，逐个server发送SR
  - 如果全部回复ErrWrongOwner，认为非法(没救了)，尝试下一个QueryIndex
  - 如果ErrWrongConfigIndex/ErrKilled/ErrNetwork，认为还有救
    - 特别地，如果请求者ci较小，中断请求
- 一旦收到1个OK的就可以，因为收到的Shard一定是共识过的结果

#### ShardRequest()
- killed check
- args.ci == kv.ss.ci check
- kv.ss.onCharge[args.Shard] == args.QueryIndex
- 通过上述检查，即可返回shardData

#### applyShard()
- ShardLog Apply
- 检查：log.ci == kv.ss.ci，检查不通过直接放弃
- 若检查通过，更新data, resultMap, onCharge, readyShard

#### 如何打回ci不匹配的
- applyLoop()中, 对于ci不匹配的op, 直接不调用applyOne()
- Get/PutAppend的等待loop中, 每次醒来检查args.ci与kv.ss.ci一致性,
  不一致返回ErrWrongGroup
- 打回过程不需要经过resultMap

#### Deprecated
- kv.ss.ready, kv.isReady()
- kv.readOnCharge()
- kv.ss.ExpCommitIndex
- kv.containsSnapshot()

### 2022.1.6
重构代码......
#### queryConfigLoop()
- 所有人都可以query config，并cache到kv.ss.configs
- 但不能更新kv.ss.ci
- 找到新的config时，尝试Start一个ConfigLog (OpType = ConfigType)
  - 如果不是Leader，会被Raft层打回
  - 因此只有Leader可以Start成功
- NOTE：可能会Start多个相同的ConfigLog，但不影响

#### applyLoop()
- CommandValid == true时，按照OpType分类
  - Get/Put/Append Type时，按原先的方案处理，封装到applyGetPutAppend
  - ConfigType时，调用applyConfig，更新kv.ss.ci

#### applyConfig()
- Called by applyLoop()
- 更新kv.ss.ci，调用newConfigHandler()更新kv.ss的信息

#### newConfigHandler()
- Called by applyConfig()
- 根据kv.ss.ci，更新readyShard和onCharge
- 依据kv.ss.ci分为两类处理：
  - 第一个config:
    - 负责的：  ready，onCharge = 1
    - 不负责的：unready
  - 后续的新config
    - 所有shard都标记为unready
    - 负责的: go shardRequestLoop(ci, shard)

#### shardRequestLoop()
- 输入：当前ci，目标shard
- 用于追踪config，向曾经负责过该shard的server发送ShardRequest RPC
- 不同的shard并行请求
- 改了一半

### 2022.1.4
PASS  JoinLeave && Static  
DEBUG Snapshot
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


