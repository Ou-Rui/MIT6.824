package kvraft

import (
	"log"
	"mymr/src/labgob"
	"mymr/src/labrpc"
	"mymr/src/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	GetType    = "Get"
	PutType    = "Put"
	AppendType = "Append"
)

// Op
// command that send to raft
// describe Get/Put/Append operation
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType OpType // string, Get/Put/Append
	Key    string
	Value  string

	Id string // severID + term + index
}

type Result struct {
	opType OpType
	value  string
	err    Err
	status string
}

const (
	Done   = "Done"
	Undone = "Undone"
)

type KVServer struct {
	mu      			sync.Mutex
	me      			int
	rf      			*raft.Raft
	applyCh 			chan raft.ApplyMsg
	dead    			int32 // set by Kill()

	maxraftstate 		int // snapshot if log grows this big

	// Your definitions here.
	data        		map[string]string	// kv data
	resultMap   		map[string]Result 	// key: requestId, value: result
	resultCond  		*sync.Cond
	commitIndex 		int
}

func (kv *KVServer) applyLoop() {
	for {
		if kv.killed() {
			return
		}
		applyMsg := <-kv.applyCh // keep watching applyCh
		DPrintf("[KV %v]: applyMsg = %v",
			kv.me, applyMsg)
		kv.mu.Lock()
		op, _ := applyMsg.Command.(Op)
		id := op.Id
		DPrintf("[KV %v]: receive applyMsg, commitIndex = %v, commandIndex = %v, id = %v, status = %v",
			kv.me, kv.commitIndex, applyMsg.CommandIndex, id, kv.resultMap[id].status)

		if applyMsg.CommandIndex >= kv.commitIndex {
			kv.commitIndex = applyMsg.CommandIndex // update commitIndex, for stale command check
			if kv.resultMap[id].status == "" {
				result := kv.applyOne(op)          // apply
				kv.resultMap[id] = result
			}
			//kv.resultCond.Broadcast()
			//DPrintf("[KV %v]: wakeup!!", kv.me)
		}else {
			DPrintf("[KV %v]: already Applied Command.. commitIndex = %v, applyIndex = %v",
				kv.me, kv.commitIndex, applyMsg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applyOne(op Op) (result Result) {
	//id := op.Id // key of resultMap
	result = Result{
		opType: "op.OpType",
		value:  "",
		err:    "",
		status: Done,
	}
	if op.OpType == GetType {
		value, ok := kv.data[op.Key]
		if ok {
			result.value = value
			result.err = OK
		} else {
			result.value = ""
			result.err = ErrNoKey
		}
	} else if op.OpType == PutType {
		kv.data[op.Key] = op.Value
		result.err = OK
	} else if op.OpType == AppendType {
		value, ok := kv.data[op.Key]
		if ok {
			kv.data[op.Key] = value + op.Value // append
		} else {
			kv.data[op.Key] = op.Value // put
		}
		result.err = OK
	}
	DPrintf("[KV %v]: applyOne, id = %v, key = %v, value = %v",
		kv.me, op.Id, op.Key, op.Value)
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[KV %v]: Get request receive.. id = %v, key = %v",
		kv.me, args.Id, args.Key)
	if kv.resultMap[args.Id].status != "" {
		DPrintf("[KV %v]: started..", kv.me)
		reply.Value = kv.resultMap[args.Id].value
		reply.Err = ErrAlreadyDone
		return
	}

	op := Op{
		OpType: 		GetType,
		Key:   			args.Key,
		Value:  		"",
		Id:     		args.Id,
	}
	kv.mu.Unlock()
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	//DPrintf("[KV %v]: Get request start", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//kv.resultMap[op.Id] = Result{
	//	opType: "",
	//	value:  "",
	//	err:    "",
	//	status: "",
	//}

	for kv.resultMap[op.Id].status != Done {
		//DPrintf("[KV %v]: Get wait here id = %v",
		//	kv.me, op.Id)
		kv.mu.Unlock()

		time.Sleep(50 * time.Millisecond)

		curTerm, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		//DPrintf("[KV %v]: Get wakeup! id = %v, isLeader = %v",
			//kv.me, op.Id, isLeader)
		if !isLeader {
			//kv.resultMap[op.Id] = Result{}
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm{
			//kv.resultMap[op.Id] = Result{}
			reply.Err = ErrNewTerm
			return
		}

	}
	result := kv.resultMap[op.Id]
	reply.Err = result.err
	reply.Value = result.value
	DPrintf("[KV %v]: Get request Done! id = %v, key = %v, reply = %v, status = %v",
		kv.me, op.Id, args.Key, reply, kv.resultMap[op.Id].status)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v]: PutAppend request receive.. id = %v, type = %v, key = %v, value = %v",
		kv.me, args.Id, args.Op, args.Key, args.Value)

	if kv.resultMap[args.Id].status != "" {
		DPrintf("[KV %v]: started..", kv.me)
		reply.Err = ErrAlreadyDone
		return
	}
	op := Op{
		OpType: 		OpType(args.Op),
		Key:    		args.Key,
		Value:  		args.Value,
		Id:     		args.Id,
	}
	kv.mu.Unlock()
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	//DPrintf("[KV %v]: PutAppend request start", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//kv.resultMap[op.Id] = Result{
	//	opType: "",
	//	value:  "",
	//	err:    "",
	//	status: "",
	//}

	for kv.resultMap[op.Id].status != Done {
		//DPrintf("[KV %v]: PutAppend wait here id = %v",
		//	kv.me, op.Id)
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		curTerm, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		//DPrintf("[KV %v]: PutAppend wakeup! id = %v, isLeader = %v",
		//	kv.me, op.Id, isLeader)
		if !isLeader {
			//kv.resultMap[op.Id] = Result{}
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm {
			//kv.resultMap[op.Id] = Result{}
			reply.Err = ErrNewTerm
			return
		}
	}
	result := kv.resultMap[op.Id]
	reply.Err = result.err
	DPrintf("[KV %v]: PutAppend request Done! id = %v, reply = %v, status = %v",
		kv.me, op.Id, reply, kv.resultMap[op.Id].status)
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v]: killed..", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.resultMap = make(map[string]Result)
	//DPrintf("%v", kv.resultMap["xx"].status)
	kv.resultCond = sync.NewCond(&kv.mu)
	kv.commitIndex = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyLoop()

	return kv
}
