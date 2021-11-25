package kvraft

import (
	"bytes"
	"log"
	"mymr/src/labgob"
	"mymr/src/labrpc"
	"mymr/src/raft"
	"strconv"
	"strings"
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
	GetType    	OpType = "Get"
	PutType    	OpType = "Put"
	AppendType 	OpType = "Append"
)


// Op
// command that send to raft
// describe Get/Put/Append operation
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id 			string
	OpType 		OpType // string, Get/Put/Append

	Key    		string
	Value  		string
}

type Result struct {
	OpType 		OpType
	Value  		string
	Err    		Err
	Status 		string
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
	Data        		map[string]string	// kv data
	ResultMap   		map[string]Result 	// key: requestId, value: result
	resultCond  		*sync.Cond
	CommitIndex 		int
	CommitTerm			int

	persister 			*raft.Persister
}

// removePrevious
// goroutine, called when a new request complete
// free memory
func (kv *KVServer) removePrevious(requestIndex int, clientId int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v]: removePrevious", kv.me)
	for id, _ := range kv.ResultMap {
		_, tIndex, tId := parseRequestId(id)
		if tId == clientId && tIndex < requestIndex {

			delete(kv.ResultMap, id)			// remove previous result
		}
	}
}

// applyLoop
// goroutine for listening applyCh and do "apply" action
func (kv *KVServer) applyLoop() {
	for {
		if kv.killed() {
			return
		}
		applyMsg := <-kv.applyCh // keep watching applyCh
		DPrintf("[KV %v]: applyMsg = %v",
			kv.me, applyMsg)
		kv.mu.Lock()
		if applyMsg.CommandValid {
			// common log apply
			op, _ := applyMsg.Command.(Op)
			id := op.Id
			DPrintf("[KV %v]: receive applyMsg, commitIndex = %v, commandIndex = %v, id = %v, status = %v",
				kv.me, kv.CommitIndex, applyMsg.CommandIndex, id, kv.ResultMap[id].Status)
			if applyMsg.CommandIndex >= kv.CommitIndex {
				kv.CommitIndex = applyMsg.CommandIndex // update commitIndex, for stale command check
				kv.CommitTerm = applyMsg.CommandTerm
				if kv.ResultMap[id].Status == "" {
					result := kv.applyOne(op)          // apply
					kv.ResultMap[id] = result
				}
			}else {
				DPrintf("[KV %v]: already Applied Command.. commitIndex = %v, applyIndex = %v",
					kv.me, kv.CommitIndex, applyMsg.CommandIndex)
			}
			_, requestIndex, clientId := parseRequestId(id)
			go kv.removePrevious(requestIndex, clientId)
		}else {
			if applyMsg.CommandTerm == -10 {
				DPrintf("[KV %v]: toFollower apply", kv.me)
			}else {
				// snapshot apply
				DPrintf("[KV %v]: snapshot apply, curData = %v, resultMap = %v, commitIndex = %v, commitTerm = %v",
					kv.me, kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm)
				snapshot, _ := applyMsg.Command.([]byte)
				if len(snapshot) > 0 {
					kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm = DecodeSnapshot(snapshot)
				}else {
					kv.Data = make(map[string]string)
					kv.ResultMap = make(map[string]Result)
					kv.CommitIndex = 0
					kv.CommitTerm = 0
				}

				DPrintf("[KV %v]: newData = %v, resultMap = %v, commitIndex = %v, commitTerm = %v",
					kv.me, kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm)
			}
		}
		kv.mu.Unlock()
		kv.resultCond.Broadcast()
	}
}

// applyOne operation to key-value database
func (kv *KVServer) applyOne(op Op) (result Result) {
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
	DPrintf("[KV %v]: applyOne, id = %v, key = %v, value = %v, newValue = %v",
		kv.me, op.Id, op.Key, op.Value, kv.Data[op.Key])
	return
}

func (kv *KVServer) snapshotLoop() {
	for  {
		kv.mu.Lock()
		DPrintf("[KV %v]: snapshotLoop lock", kv.me)
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		//kv.resultCond.Wait()
		if kv.persister.RaftStateSize() > kv.maxraftstate {
			DPrintf("[KV %v]: RaftStateSize is too large, Snapshot!, size = %v",
				kv.me, kv.persister.RaftStateSize())
			if kv.CommitIndex == 0 {
				DPrintf("[KV %v]: commitIndex == 0, nothing to snapshot...",
					kv.me)
			}else{
				snapshot := kv.generateSnapshot()
				commitIndex := kv.CommitIndex
				commitTerm := kv.CommitTerm
				kv.mu.Unlock()
				kv.rf.SaveSnapshot(snapshot, commitIndex, commitTerm)
				kv.mu.Lock()
			}

			//DPrintf("[KV %v]: Snapshot Done, size = %v",
			//	kv.me, kv.persister.RaftStateSize())
			//kv.persister.SaveStateAndSnapshot(kv.persister.ReadRaftState(), snapshot)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// generateSnapshot
// encode state data of KV server, return a snapshot in []byte
func (kv *KVServer) generateSnapshot() []byte {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	DPrintf("[KV %v]: data = %v, commitIndex = %v", kv.me, kv.Data, kv.CommitIndex)
	encoder.Encode(kv.Data)

	encoder.Encode(kv.ResultMap)
	encoder.Encode(kv.CommitIndex)
	encoder.Encode(kv.CommitTerm)

	data := writer.Bytes()
	DPrintf("[KV %v]: snapshot = %v", kv.me, data)
	return data
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v]: Get request receive.. id = %v, key = %v",
		kv.me, args.Id, args.Key)
	if kv.ResultMap[args.Id].Status != "" {
		DPrintf("[KV %v]: started..", kv.me)
		reply.Value = kv.ResultMap[args.Id].Value
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

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for kv.ResultMap[op.Id].Status != Done {
		DPrintf("[KV %v]: sleep..", kv.me)
		kv.resultCond.Wait()
		kv.mu.Unlock()
		//time.Sleep(10 * time.Millisecond)
		// check Leadership and Term
		curTerm, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		DPrintf("[KV %v]: wakeup id = %v, status = %v, curTerm = %v, isLeader = %v",
			kv.me, op.Id, kv.ResultMap[op.Id].Status, curTerm, isLeader)
		if !isLeader || kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm{
			reply.Err = ErrNewTerm
			return
		}
	}
	result := kv.ResultMap[op.Id]
	reply.Err = result.Err
	reply.Value = result.Value
	DPrintf("[KV %v]: Get request Done! id = %v, key = %v, reply = %v, status = %v",
		kv.me, op.Id, args.Key, reply, kv.ResultMap[op.Id].Status)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV %v]: PutAppend request receive.. id = %v, type = %v, key = %v, value = %v",
		kv.me, args.Id, args.Op, args.Key, args.Value)
	if kv.ResultMap[args.Id].Status != "" {
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

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for kv.ResultMap[op.Id].Status != Done {
		DPrintf("[KV %v]: sleep..", kv.me)
		kv.resultCond.Wait()
		kv.mu.Unlock()
		//time.Sleep(10 * time.Millisecond)
		curTerm, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		DPrintf("[KV %v]: wakeup id = %v, status = %v, curTerm = %v, isLeader = %v",
			kv.me, op.Id, kv.ResultMap[op.Id].Status, curTerm, isLeader)
		if !isLeader || kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm {
			reply.Err = ErrNewTerm
			return
		}
	}
	result := kv.ResultMap[op.Id]
	reply.Err = result.Err
	DPrintf("[KV %v]: PutAppend request Done! id = %v, reply = %v, status = %v",
		kv.me, op.Id, reply, kv.ResultMap[op.Id].Status)
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
	kv.resultCond.Broadcast()
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
	kv.Data = make(map[string]string)
	kv.ResultMap = make(map[string]Result)
	kv.resultCond = sync.NewCond(&kv.mu)
	kv.CommitIndex = 0
	kv.CommitTerm = 0
	kv.persister = persister

	snapshot := persister.ReadSnapshot()
	if len(snapshot) != 0 {
		kv.Data, kv.ResultMap, kv.CommitIndex, kv.CommitTerm = DecodeSnapshot(snapshot)
		DPrintf("[KV %v] read from persister, data = %v, commitIndex = %v", kv.me, kv.Data, kv.CommitIndex)
	}


	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyLoop()

	if maxraftstate > 0 {
		go kv.snapshotLoop()
	}
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
	}else {
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

func (op *Op) encode() []byte {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)

	encoder.Encode(op.Id)
	encoder.Encode(op.OpType)
	encoder.Encode(op.Key)
	encoder.Encode(op.Value)

	data := writer.Bytes()
	return data
}

func (op *Op) decode(data []byte) {
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	if decoder.Decode(&op.Id) != nil ||
		decoder.Decode(&op.OpType) != nil ||
		decoder.Decode(&op.Key) != nil ||
		decoder.Decode(&op.Value) != nil {
		DPrintf("Decode Op error...")
	}
	return
}