package shardkv

// import "../shardmaster"
import (
	"bytes"
	"log"
	"mymr/src/labrpc"
	"mymr/src/shardmaster"
	"strconv"
	"strings"
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

type OpType string

const (
	GetType    OpType = "Get"
	PutType    OpType = "Put"
	AppendType OpType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     string
	OpType OpType // string, Get/Put/Append

	Key   string
	Value string
}

type Result struct {
	OpType OpType
	Value  string
	Err    Err
	Status string
}

const (
	Done   = "Done"
	Undone = "Undone"
)

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

	persister *raft.Persister
	mck       *shardmaster.Clerk // one mck communicate with all shardMasters
	config    shardmaster.Config
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
		config := kv.mck.Query(-1)
		DPrintf("[KV %v-%v]: getConfig = %v", kv.gid, kv.me, config)
		if config.Num < kv.config.Num {
			DPrintf("[KV %v-%v]: getConfig ERROR!! queryConfigIndex = %v, curIndex = %v",
				kv.gid, kv.me, config.Num, kv.config.Num)
		} else if config.Num == kv.config.Num {
			// do nothing
		} else {
			DPrintf("[KV %v-%v]: update config! lastConfig = %v", kv.gid, kv.me, kv.config)
			kv.config = config
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// removePrevious
// goroutine, called when a new request complete
// free memory
func (kv *ShardKV) removePrevious(requestIndex int, clientId int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v-%v]: removePrevious", kv.gid, kv.me)
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

	if args.ConfigIndex != kv.config.Num {
		DPrintf("[KV %v-%v]: WrongGroup, args.ci = %v, kv.ci = %v",
			kv.gid, kv.me, args.ConfigIndex, kv.config.Num)
		reply.Err = ErrWrongGroup
		return
	}

	if kv.ResultMap[args.Id].Status != "" {
		DPrintf("[KV %v-%v]: started..", kv.gid, kv.me)
		reply.Value = kv.ResultMap[args.Id].Value
		reply.Err = ErrAlreadyDone
		return
	}
	op := Op{
		OpType: GetType,
		Key:    args.Key,
		Value:  "",
		Id:     args.Id,
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

	if args.ConfigIndex != kv.config.Num {
		DPrintf("[KV %v-%v]: WrongGroup, args.ci = %v, kv.ci = %v",
			kv.gid, kv.me, args.ConfigIndex, kv.config.Num)
		reply.Err = ErrWrongGroup
		return
	}

	if kv.ResultMap[args.Id].Status != "" {
		DPrintf("[KV %v-%v]: started..", kv.gid, kv.me)
		reply.Err = ErrAlreadyDone
		return
	}

	op := Op{
		OpType: OpType(args.Op),
		Key:    args.Key,
		Value:  args.Value,
		Id:     args.Id,
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

	// Goroutines
	go kv.applyLoop()

	if maxraftstate > 0 {
		go kv.snapshotLoop()
	}

	go kv.queryConfigLoop()

	return kv
}

func parseRequestId(id string) (opType OpType, requestIndex int, clientId int) {
	t := strings.Split(id, "+")
	if len(t) == 3 {
		opType = OpType(t[0])
		requestIndex, _ = strconv.Atoi(t[1])
		clientId, _ = strconv.Atoi(t[2])
		//DPrintf("parseRequestId succeed, id = %v, opType = %v, requestIndex = %v, clientId = %v",
		//	id, opType, requestIndex, clientId)
	} else {
		DPrintf("parseRequestId error??? id = %v", id)
	}
	return
}

func DecodeSnapshot(snapshot []byte) (
	Data map[string]string, ResultMap map[string]Result, CommitIndex int, CommitTerm int) {
	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)
	if decoder.Decode(&Data) != nil ||
		decoder.Decode(&ResultMap) != nil ||
		decoder.Decode(&CommitIndex) != nil ||
		decoder.Decode(&CommitTerm) != nil {
		DPrintf("Decode snapshot error...")
	}
	return
}
