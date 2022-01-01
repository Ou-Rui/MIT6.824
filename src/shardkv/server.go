package shardkv

// import "../shardmaster"
import (
	"bytes"
	"log"
	"mymr/src/labrpc"
	"mymr/src/shardmaster"
	"sync/atomic"
	"time"
)
import "mymr/src/raft"
import "sync"
import "mymr/src/labgob"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Result struct {
	OpType OpType
	Key    string
	Value  string
	Err    Err
	Status string
}

const (
	Done   = "Done"
	Undone = "Undone"
)

type ShardState struct {
	configs    map[int]*shardmaster.Config
	index      int
	ready      bool
	readyShard []bool // index: shard
	onCharge   []int  // index: shard, value: configIndex
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd // make_end(serverName) ==> ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd // shardMasters, dont call their rpc directly, instead, create a shardMaster clerk
	maxraftstate int                 // snapshot if log grows this big

	// Your definitions here.
	dead        int32
	Data        map[string]string // kv data
	ResultMap   map[string]Result // key: requestId, value: result
	resultCond  *sync.Cond
	CommitIndex int
	CommitTerm  int

	persister  *raft.Persister
	mck        *shardmaster.Clerk // one mck communicate with all shardMasters
	shardState ShardState
}

// check kv.shardState.ready according to readyShard and Config
func (kv *ShardKV) isReady() bool {
	config := kv.shardState.configs[kv.shardState.index]
	for shard, ready := range kv.shardState.readyShard {
		// responsible but not ready, return false
		if config.Shards[shard] == kv.gid && !ready {
			return false
		}
	}
	return true
}

func (kv *ShardKV) containsConfig() bool {
	return kv.shardState.index >= 1
}

// queryConfigLoop
// query for latest Config by kv.mck.Query(-1) every 100ms
func (kv *ShardKV) queryConfigLoop() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		// must first get config1
		if !kv.containsConfig() {
			DPrintf("[KV %v-%v]: query for first config...",
				kv.gid, kv.me)
			kv.mu.Unlock()
			config := kv.mck.Query(1)
			kv.mu.Lock()
			kv.shardState.index = 1
			kv.newConfigHandler(config)
		}
		config := kv.mck.Query(-1)
		if config.Num < kv.shardState.index {
			DPrintf("[KV %v-%v]: getConfig = %v", kv.gid, kv.me, config)
			DPrintf("[KV %v-%v]: out-of-date.. queryConfigIndex = %v, curIndex = %v",
				kv.gid, kv.me, config.Num, kv.shardState.index)
			if kv.shardState.configs[config.Num] != nil {
				kv.shardState.configs[config.Num] = &config
				DPrintf("[KV %v-%v]: cache.., config = %v",
					kv.gid, kv.me, kv.shardState.configs)
			}
		} else if config.Num == kv.shardState.index {
			// common situation, do nothing
		} else {
			DPrintf("[KV %v-%v]: getConfig = %v, up-to-date!", kv.gid, kv.me, config)
			kv.shardState.configs[config.Num] = &config
			kv.shardState.index = config.Num
			kv.newConfigHandler(config)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// newConfigHandler
// called by queryConfigLoop when get a new config
func (kv *ShardKV) newConfigHandler(config shardmaster.Config) {
	//config := kv.shardState.configs[kv.shardState.index]
	if kv.shardState.index == 1 {
		// first config
		kv.shardState.ready = true
		for i := 0; i < shardmaster.NShards; i++ {
			kv.shardState.readyShard[i] = true
			kv.shardState.onCharge[i] = kv.shardState.index
		}
	} else {
		// new config
		kv.shardState.ready = false
		for i := 0; i < shardmaster.NShards; i++ {
			if config.Shards[i] == kv.gid && kv.shardState.readyShard[i] {
				// responsible && ready in last config
				kv.shardState.readyShard[i] = true
				kv.shardState.onCharge[i] = kv.shardState.index
			} else {
				kv.shardState.readyShard[i] = false
			}
		}
		go kv.shardRequestLoop(kv.shardState.index)
	}
	DPrintf("[KV %v-%v]: newConfigHandler, config = %v, readyShard = %v, onCharge = %v",
		kv.gid, kv.me, kv.shardState.configs[kv.shardState.index], kv.shardState.readyShard, kv.shardState.onCharge)
}

// shardRequestLoop, called by newConfigHandler
// request shards for a given configIndex
func (kv *ShardKV) shardRequestLoop(configIndex int) {
	for {
		kv.mu.Lock()
		if kv.killed() || kv.shardState.index != configIndex {
			DPrintf("[KV %v-%v]: shardRequestLoop exit(-1), curConfig.Num = %v, target = %v",
				kv.gid, kv.me, kv.shardState.index, configIndex)
			kv.mu.Unlock()
			return
		}
		// shard loop
		for shard := range kv.shardState.readyShard {
			// unready && responsible
			if !kv.shardState.readyShard[shard] && kv.shardState.configs[configIndex].Shards[shard] == kv.gid {
				// index of config for shard i , start from curIndex-1, decrease..
				queryIndex := configIndex - 1
				// queryIndex loop
				for queryIndex >= 0 {
					// if not cached, query..
					if kv.shardState.configs[queryIndex] == nil {
						config := kv.mck.Query(queryIndex)
						DPrintf("[KV %v-%v]: query for config%v, config = %v",
							kv.gid, kv.me, queryIndex, config)
						kv.shardState.configs[queryIndex] = &config
					}
					config := kv.shardState.configs[queryIndex]
					gid := config.Shards[shard]
					servers := config.Groups[gid]
					DPrintf("[KV %v-%v]: in config %v, group %v is responsible for shard %v, config = %v",
						kv.gid, kv.me, queryIndex, gid, shard, config)
					// server loop
					for _, server := range servers {
						ok, reply := kv.sendShardRequest(server, shard, queryIndex)
						if ok {
							// receive shardRequest reply, check alive && configIndex
							if kv.killed() || kv.shardState.index != configIndex {
								DPrintf("[KV %v-%v]: shardRequestLoop exit(-1), curConfig.Num = %v, target = %v",
									kv.gid, kv.me, kv.shardState.index, configIndex)
								kv.mu.Unlock()
								return
							}
							if reply.Err == OK {
								for key, value := range reply.Data {
									kv.Data[key] = value
								}
								for id, result := range reply.ResultMap {
									newResult := Result{}
									deepCopy(newResult, result)
									kv.ResultMap[id] = newResult
								}
								kv.shardState.onCharge[shard] = kv.shardState.index
								kv.shardState.readyShard[shard] = true
								kv.shardState.ready = kv.isReady()
								DPrintf("[KV %v-%v]: getShard %v, ready = %v, readyShard = %v, onCharge = %v",
									kv.gid, kv.me, shard, kv.shardState.ready, kv.shardState.readyShard, kv.shardState.onCharge)
							} else if reply.Err == ErrKilled || reply.Err == ErrWrongLeader {
								// ask next server, nothing to do..
							} else if reply.Err == ErrWrongConfigIndex {
								DPrintf("[KV %v-%v]: shard %v Request failed, %v, reply.ci = %v, kv.ci = %v ",
									kv.gid, kv.me, shard, reply.Err, reply.ConfigIndex, kv.shardState.index)
								if reply.ConfigIndex > kv.shardState.index {
									return
								}
							} else if reply.Err == ErrWrongOwner {
								DPrintf("[KV %v-%v]: shard %v Request failed, %v, break ",
									kv.gid, kv.me, shard, reply.Err)
								break
							}
						} else {
							// network failed, nothing to do..
						}
					}
					queryIndex--
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// ShardRequest, called by shardRequestLoop
// send ShardRequest RPC to server in other group
func (kv *ShardKV) sendShardRequest(server string, shard int, queryIndex int) (ok bool, reply *ShardReply) {
	args := &ShardArgs{
		Shard:       shard,
		ConfigIndex: kv.shardState.index,
		Gid:         kv.gid,
		Server:      kv.me,
		QueryIndex:  queryIndex,
	}

	reply = &ShardReply{
		ConfigIndex: 0,
		Data:        nil,
		ResultMap:   nil,
		Err:         "",
	}

	clientEnd := kv.make_end(server)
	kv.mu.Unlock()
	ok = clientEnd.Call("ShardKV.ShardRequest", args, reply)
	kv.mu.Lock()

	return
}

// ShardRequest RPC
// called by other ShardKV Servers for Requesting Shards
func (kv *ShardKV) ShardRequest(args *ShardArgs, reply *ShardReply) {
	_, isLeader := kv.rf.GetState()

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.killed() {
		DPrintf("[KV %v-%v]: ShardRequest from KV %v-%v, Error, I'm killed..",
			kv.gid, kv.me, args.Gid, args.Server)
		reply.Err = ErrKilled
		return
	}

	// leadership check
	// only leader can offer shards, because follower may have committed by not applied logs
	if !isLeader {
		DPrintf("[KV %v-%v]: ShardRequest from KV %v-%v, Error, I'm not Leader..",
			kv.gid, kv.me, args.Gid, args.Server)
		reply.Err = ErrWrongLeader
		return
	}

	// configIndex check, maybe too strict??
	if args.ConfigIndex != kv.shardState.index {
		DPrintf("[KV %v-%v]: ShardRequest from KV %v-%v, Error, configIndex match failed, his = %v, mine = %v",
			kv.gid, kv.me, args.Gid, args.Server, args.ConfigIndex, kv.shardState.index)
		reply.Err = ErrWrongConfigIndex
		reply.ConfigIndex = maxInt(args.ConfigIndex, kv.shardState.index)
		return
	}

	// shard ownership check
	if kv.shardState.onCharge[args.Shard] < args.QueryIndex {
		DPrintf("[KV %v-%v]: ShardRequest from KV %v-%v, Error, ownership check failed, require = %v, mine = %v",
			kv.gid, kv.me, args.Gid, args.Server, args.QueryIndex, kv.shardState.onCharge[args.Shard])
		reply.Err = ErrWrongOwner
		return
	}

	reply.ConfigIndex = kv.shardState.index
	reply.Err = OK
	// pack up data
	reply.Data = make(map[string]string)
	for key, value := range kv.Data {
		if key2shard(key) == args.Shard {
			reply.Data[key] = value
		}
	}
	// pack up resultMap
	reply.ResultMap = make(map[string]Result)
	for id, result := range kv.ResultMap {
		if key2shard(result.Key) == args.Shard {
			newResult := Result{}
			deepCopy(newResult, result)
			reply.ResultMap[id] = newResult
		}
	}
	return
}

// removePrevious
// goroutine, called when a new request complete
// free memory
func (kv *ShardKV) removePrevious(requestIndex int, clientId int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//DPrintf("[KV %v-%v]: removePrevious", kv.gid, kv.me)
	for id, _ := range kv.ResultMap {
		_, tIndex, tId := parseRequestId(id)
		if tId == clientId && tIndex < requestIndex {
			delete(kv.ResultMap, id) // remove previous result
		}
	}
}

// applyLoop
// goroutine for listening applyCh and do "apply" action
func (kv *ShardKV) applyLoop() {
	for {
		if kv.killed() {
			return
		}
		applyMsg := <-kv.applyCh // keep watching applyCh
		DPrintf("[KV %v-%v]: applyMsg = %v", kv.gid, kv.me, applyMsg)
		kv.mu.Lock()
		if applyMsg.CommandValid {
			// common log apply
			op, _ := applyMsg.Command.(Op)
			id := op.Id
			DPrintf("[KV %v-%v]: receive applyMsg, commitIndex = %v, commandIndex = %v, id = %v, status = %v",
				kv.gid, kv.me, kv.CommitIndex, applyMsg.CommandIndex, id, kv.ResultMap[id].Status)
			if applyMsg.CommandIndex >= kv.CommitIndex {
				kv.CommitIndex = applyMsg.CommandIndex // update commitIndex, for stale command check
				kv.CommitTerm = applyMsg.CommandTerm
				if kv.ResultMap[id].Status == "" {
					result := kv.applyOne(op) // apply
					kv.ResultMap[id] = result
				}
			} else {
				DPrintf("[KV %v-%v]: already Applied Command.. commitIndex = %v, applyIndex = %v",
					kv.gid, kv.me, kv.CommitIndex, applyMsg.CommandIndex)
			}
			_, requestIndex, clientId := parseRequestId(id)
			go kv.removePrevious(requestIndex, clientId)
		} else {
			if applyMsg.CommandTerm == -10 {
				DPrintf("[KV %v-%v]: toFollower apply", kv.gid, kv.me)
			} else {
				// snapshot apply
				DPrintf("[KV %v-%v]: snapshot apply, curData = %v, resultMap = %v, commitIndex = %v, commitTerm = %v",
					kv.gid, kv.me, kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm)
				snapshot, _ := applyMsg.Command.([]byte)
				if len(snapshot) > 0 {
					kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm = DecodeSnapshot(snapshot)
				} else {
					kv.Data = make(map[string]string)
					kv.ResultMap = make(map[string]Result)
					kv.CommitIndex = 0
					kv.CommitTerm = 0
				}
				DPrintf("[KV %v-%v]: newData = %v, resultMap = %v, commitIndex = %v, commitTerm = %v",
					kv.gid, kv.me, kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm)
			}
		}
		kv.mu.Unlock()
		kv.resultCond.Broadcast()
	}
}

// applyOne operation to key-value database
func (kv *ShardKV) applyOne(op Op) (result Result) {
	result = Result{
		OpType: "op.OpType",
		Key:    op.Key,
		Value:  "",
		Err:    "",
		Status: Done,
	}
	if op.OpType == GetType {
		value, ok := kv.Data[op.Key]
		if ok {
			result.Value = value
			result.Err = OK
		} else {
			result.Value = ""
			result.Err = ErrNoKey
		}
	} else if op.OpType == PutType {
		kv.Data[op.Key] = op.Value
		result.Err = OK
	} else if op.OpType == AppendType {
		value, ok := kv.Data[op.Key]
		if ok {
			kv.Data[op.Key] = value + op.Value // append
		} else {
			kv.Data[op.Key] = op.Value // put
		}
		result.Err = OK
	}
	DPrintf("[KV %v-%v]: applyOne, id = %v, key = %v, value = %v, newValue = %v",
		kv.gid, kv.me, op.Id, op.Key, op.Value, kv.Data[op.Key])
	return
}

func (kv *ShardKV) snapshotLoop() {
	for {
		kv.mu.Lock()
		DPrintf("[KV %v-%v]: snapshotLoop lock", kv.gid, kv.me)
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		//kv.resultCond.Wait()
		if kv.persister.RaftStateSize() > kv.maxraftstate {
			DPrintf("[KV %v-%v]: RaftStateSize is too large, Snapshot!, size = %v",
				kv.gid, kv.me, kv.persister.RaftStateSize())
			if kv.CommitIndex == 0 {
				DPrintf("[KV %v-%v]: commitIndex == 0, nothing to snapshot...",
					kv.gid, kv.me)
			} else {
				snapshot := kv.generateSnapshot()
				commitIndex := kv.CommitIndex
				commitTerm := kv.CommitTerm
				kv.mu.Unlock()
				kv.rf.SaveSnapshot(snapshot, commitIndex, commitTerm)
				kv.mu.Lock()
			}
			//DPrintf("[KV %v-%v]: Snapshot Done, size = %v",
			//	kv.gid, kv.me, kv.persister.RaftStateSize())
			//kv.persister.SaveStateAndSnapshot(kv.persister.ReadRaftState(), snapshot)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// generateSnapshot
// encode state data of KV server, return a snapshot in []byte
func (kv *ShardKV) generateSnapshot() []byte {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	DPrintf("[KV %v-%v]: data = %v, commitIndex = %v", kv.gid, kv.me, kv.Data, kv.CommitIndex)
	encoder.Encode(kv.Data)

	encoder.Encode(kv.ResultMap)
	encoder.Encode(kv.CommitIndex)
	encoder.Encode(kv.CommitTerm)

	data := writer.Bytes()
	DPrintf("[KV %v-%v]: snapshot = %v", kv.gid, kv.me, data)
	return data
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v-%v]: Get request receive.. id = %v, key = %v",
		kv.gid, kv.me, args.Id, args.Key)

	//if !kv.containsConfig() {
	//	DPrintf("[KV %v-%v]: no config yet.. wait..", kv.gid, kv.me)
	//	return
	//}
	//curConfig := kv.shardState.configs[len(kv.shardState.configs)-1]
	if args.ConfigIndex != kv.shardState.index {
		DPrintf("[KV %v-%v]: WrongGroup, args.ci = %v, kv.ci = %v",
			kv.gid, kv.me, args.ConfigIndex, kv.shardState.index)
		reply.Err = ErrWrongGroup
		return
	} else if !kv.shardState.ready {
		DPrintf("[KV %v-%v]: NotReady yet", kv.gid, kv.me)
		reply.Err = ErrNotReady
		return
	}

	if kv.ResultMap[args.Id].Status != "" {
		DPrintf("[KV %v-%v]: started..", kv.gid, kv.me)
		reply.Value = kv.ResultMap[args.Id].Value
		reply.Err = ErrAlreadyDone
		return
	}
	op := Op{
		OpType:      GetType,
		Key:         args.Key,
		Value:       "",
		Id:          args.Id,
		ConfigIndex: kv.shardState.index,
	}
	kv.mu.Unlock()
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for kv.ResultMap[op.Id].Status != Done {
		DPrintf("[KV %v-%v]: sleep..", kv.gid, kv.me)
		kv.resultCond.Wait()
		kv.mu.Unlock()
		//time.Sleep(10 * time.Millisecond)
		// check Leadership and Term
		curTerm, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		DPrintf("[KV %v-%v]: wakeup id = %v, status = %v, curTerm = %v, isLeader = %v",
			kv.gid, kv.me, op.Id, kv.ResultMap[op.Id].Status, curTerm, isLeader)
		if !isLeader || kv.killed() {
			reply.Err = ErrWrongLeader
			return
		} else if term != curTerm {
			reply.Err = ErrNewTerm
			return
		}
	}
	result := kv.ResultMap[op.Id]
	reply.Err = result.Err
	reply.Value = result.Value
	DPrintf("[KV %v-%v]: Get request Done! id = %v, key = %v, reply = %v, status = %v",
		kv.gid, kv.me, op.Id, args.Key, reply, kv.ResultMap[op.Id].Status)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v-%v]: PutAppend request receive.. id = %v, type = %v, key = %v, value = %v",
		kv.gid, kv.me, args.Id, args.Op, args.Key, args.Value)

	//if !kv.containsConfig() {
	//	DPrintf("[KV %v-%v]: no config yet.. wait..", kv.gid, kv.me)
	//	return
	//}
	//curConfig := kv.shardState.configs[len(kv.shardState.configs)-1]
	if args.ConfigIndex != kv.shardState.index {
		DPrintf("[KV %v-%v]: WrongGroup, args.ci = %v, kv.ci = %v",
			kv.gid, kv.me, args.ConfigIndex, kv.shardState.index)
		reply.Err = ErrWrongGroup
		return
	} else if !kv.shardState.ready {
		DPrintf("[KV %v-%v]: NotReady yet", kv.gid, kv.me)
		reply.Err = ErrNotReady
		return
	}

	if kv.ResultMap[args.Id].Status != "" {
		DPrintf("[KV %v-%v]: started..", kv.gid, kv.me)
		reply.Err = ErrAlreadyDone
		return
	}

	op := Op{
		OpType:      OpType(args.Op),
		Key:         args.Key,
		Value:       args.Value,
		Id:          args.Id,
		ConfigIndex: kv.shardState.index,
	}

	kv.mu.Unlock()
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for kv.ResultMap[op.Id].Status != Done {
		DPrintf("[KV %v-%v]: sleep..", kv.gid, kv.me)
		kv.resultCond.Wait()
		kv.mu.Unlock()
		//time.Sleep(10 * time.Millisecond)
		curTerm, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		DPrintf("[KV %v-%v]: wakeup id = %v, status = %v, curTerm = %v, isLeader = %v",
			kv.gid, kv.me, op.Id, kv.ResultMap[op.Id].Status, curTerm, isLeader)
		if !isLeader || kv.killed() {
			reply.Err = ErrWrongLeader
			return
		} else if term != curTerm {
			reply.Err = ErrNewTerm
			return
		}
	}
	result := kv.ResultMap[op.Id]
	reply.Err = result.Err
	DPrintf("[KV %v-%v]: PutAppend request Done! id = %v, reply = %v, status = %v",
		kv.gid, kv.me, op.Id, reply, kv.ResultMap[op.Id].Status)
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v-%v]: killed..", kv.gid, kv.me)
	kv.resultCond.Broadcast()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.Data = make(map[string]string)
	kv.ResultMap = make(map[string]Result)
	kv.resultCond = sync.NewCond(&kv.mu)
	kv.CommitIndex = 0
	kv.CommitTerm = 0
	kv.persister = persister

	snapshot := persister.ReadSnapshot()
	if len(snapshot) != 0 {
		kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm = DecodeSnapshot(snapshot)
		DPrintf("[KV %v-%v] read from persister, data = %v, commitIndex = %v", kv.gid, kv.me, kv.Data, kv.CommitIndex)
	}

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shardState = ShardState{
		index:      0,
		configs:    make(map[int]*shardmaster.Config),
		ready:      false,
		readyShard: make([]bool, shardmaster.NShards),
		onCharge:   make([]int, shardmaster.NShards),
	}
	for i := range kv.shardState.onCharge {
		kv.shardState.onCharge[i] = -1
	}

	// Goroutines
	go kv.applyLoop()
	if maxraftstate > 0 {
		go kv.snapshotLoop()
	}
	go kv.queryConfigLoop()

	return kv
}
