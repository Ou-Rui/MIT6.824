package shardmaster

import (
	"bytes"
	"encoding/gob"
	"log"
	"mymr/src/raft"
	"strconv"
	"strings"
	"sync/atomic"
)
import "mymr/src/labrpc"
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
	JoinType		OpType = "Join"
	LeaveType    	OpType = "Leave"
	MoveType 		OpType = "Move"
	QueryType    	OpType = "Query"
)

type Op struct {
	// Your data here.
	// for All
	OpType 		OpType
	Id 			string
	// for Get/Put/Append
	Key    		string
	Value  		string
	// for Join
	Servers    	map[int][]string
	// for Leave
	GIDs 		[]int
	// for Move
	Shard 		int
	GID   		int
	// for Query
	Num 		int
}

type Result struct {
	// for All
	OpType 		OpType
	Err    		Err
	Done 		bool
	// for Get/Put/Append
	Value  		string
	// for Join

	// for Leave

	// for Move

	// for Query
	Config 		Config
}

const (
	Done   = "Done"
	Undone = "Undone"
)

// ShardMaster
// a special KV server
type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    			int32 // set by Kill()

	maxraftstate 		int // snapshot if log grows this big

	// Your definitions here.
	Data        		map[string]string	// kv data
	ResultMap   		map[string]Result 	// key: requestId, value: result
	resultCond  		*sync.Cond
	CommitIndex 		int
	CommitTerm			int

	persister 			*raft.Persister


	configs []Config // indexed by config num
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	DPrintf("[SM %v] Join request receive.. id = %v, Servers = %v",
		sm.me, args.Id, args.Servers)
	if sm.ResultMap[args.Id].Done {
		DPrintf("[SM %v]: started..", sm.me)
		reply.Err = ErrAlreadyDone
		return
	}
	op := Op{
		OpType: 		JoinType,
		Id:     		args.Id,
		Servers:   		args.Servers,
	}
	sm.mu.Unlock()
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Lock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for !sm.ResultMap[op.Id].Done {
		DPrintf("[SM %v]: sleep..", sm.me)
		sm.resultCond.Wait()
		sm.mu.Unlock()
		// check Leadership and Term
		curTerm, isLeader := sm.rf.GetState()
		sm.mu.Lock()
		DPrintf("[SM %v]: wakeup id = %v, Done = %v, curTerm = %v, isLeader = %v",
			sm.me, op.Id, sm.ResultMap[op.Id].Done, curTerm, isLeader)
		if !isLeader || sm.killed() {
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm{
			reply.Err = ErrNewTerm
			return
		}
	}
	result := sm.ResultMap[op.Id]
	reply.Err = result.Err
	DPrintf("[SM %v]: Join request Done! id = %v, Server = %v",
		sm.me, op.Id, args.Servers)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	DPrintf("[SM %v] Leave request receive.. id = %v, GIDs = %v",
		sm.me, args.Id, args.GIDs)
	if sm.ResultMap[args.Id].Done {
		DPrintf("[SM %v]: started..", sm.me)
		reply.Err = ErrAlreadyDone
		return
	}
	op := Op{
		OpType: 		LeaveType,
		Id:     		args.Id,
		GIDs:   		args.GIDs,
	}
	sm.mu.Unlock()
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Lock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for !sm.ResultMap[op.Id].Done {
		DPrintf("[SM %v]: sleep..", sm.me)
		sm.resultCond.Wait()
		sm.mu.Unlock()
		// check Leadership and Term
		curTerm, isLeader := sm.rf.GetState()
		sm.mu.Lock()
		DPrintf("[SM %v]: wakeup id = %v, Done = %v, curTerm = %v, isLeader = %v",
			sm.me, op.Id, sm.ResultMap[op.Id].Done, curTerm, isLeader)
		if !isLeader || sm.killed() {
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm{
			reply.Err = ErrNewTerm
			return
		}
	}
	result := sm.ResultMap[op.Id]
	reply.Err = result.Err
	DPrintf("[SM %v]: Leave request Done! id = %v, GIDs = %v",
		sm.me, op.Id, args.GIDs)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	DPrintf("[SM %v] Move request receive.. id = %v, Shard = %v, GID = %v",
		sm.me, args.Id, args.Shard, args.GID)
	if sm.ResultMap[args.Id].Done {
		DPrintf("[SM %v]: started..", sm.me)
		reply.Err = ErrAlreadyDone
		return
	}
	op := Op{
		OpType: 		MoveType,
		Id:     		args.Id,
		Shard:   		args.Shard,
		GID:			args.GID,
	}
	sm.mu.Unlock()
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Lock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for !sm.ResultMap[op.Id].Done {
		DPrintf("[SM %v]: sleep..", sm.me)
		sm.resultCond.Wait()
		sm.mu.Unlock()
		// check Leadership and Term
		curTerm, isLeader := sm.rf.GetState()
		sm.mu.Lock()
		DPrintf("[SM %v]: wakeup id = %v, Done = %v, curTerm = %v, isLeader = %v",
			sm.me, op.Id, sm.ResultMap[op.Id].Done, curTerm, isLeader)
		if !isLeader || sm.killed() {
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm{
			reply.Err = ErrNewTerm
			return
		}
	}
	result := sm.ResultMap[op.Id]
	reply.Err = result.Err
	DPrintf("[SM %v]: Move request Done! id = %v, Shard = %v, GID = %v",
		sm.me, op.Id, args.Shard, args.GID)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	DPrintf("[SM %v] Query request receive.. id = %v, Num = %v",
		sm.me, args.Id, args.Num)
	if sm.ResultMap[args.Id].Done {
		DPrintf("[SM %v]: started..", sm.me)
		reply.Err = ErrAlreadyDone
		return
	}
	op := Op{
		OpType: 		QueryType,
		Id:     		args.Id,
		Num:   			args.Num,
	}
	sm.mu.Unlock()
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Lock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for !sm.ResultMap[op.Id].Done {
		DPrintf("[SM %v]: sleep..", sm.me)
		sm.resultCond.Wait()
		sm.mu.Unlock()
		// check Leadership and Term
		curTerm, isLeader := sm.rf.GetState()
		sm.mu.Lock()
		DPrintf("[SM %v]: wakeup id = %v, Done = %v, curTerm = %v, isLeader = %v",
			sm.me, op.Id, sm.ResultMap[op.Id].Done, curTerm, isLeader)
		if !isLeader || sm.killed() {
			reply.Err = ErrWrongLeader
			return
		}else if term != curTerm{
			reply.Err = ErrNewTerm
			return
		}
	}
	result := sm.ResultMap[op.Id]
	reply.Err = result.Err
	reply.Config = result.Config
	DPrintf("[SM %v]: Join request Done! id = %v, Config = %v",
		sm.me, op.Id, reply.Config)
}


// removePrevious
// goroutine, called when a new request complete
// free memory
func (sm *ShardMaster) removePrevious(requestIndex int, clientId int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	DPrintf("[SM %v]: removePrevious", sm.me)
	for id, _ := range sm.ResultMap {
		_, tIndex, tId := parseRequestId(id)
		if tId == clientId && tIndex < requestIndex {
			delete(sm.ResultMap, id)			// remove previous result
		}
	}
}

// applyLoop
// goroutine for listening applyCh and do "apply" action
func (sm *ShardMaster) applyLoop() {
	for {
		if sm.killed() {
			return
		}
		applyMsg := <-sm.applyCh // keep watching applyCh
		DPrintf("[SM %v]: applyMsg = %v",
			sm.me, applyMsg)
		sm.mu.Lock()
		if applyMsg.CommandValid {
			// common log apply
			op, _ := applyMsg.Command.(Op)
			id := op.Id
			DPrintf("[SM %v]: receive applyMsg, commitIndex = %v, commandIndex = %v, id = %v, Done = %v",
				sm.me, sm.CommitIndex, applyMsg.CommandIndex, id, sm.ResultMap[id].Done)
			if applyMsg.CommandIndex >= sm.CommitIndex {
				sm.CommitIndex = applyMsg.CommandIndex // update commitIndex, for stale command check
				sm.CommitTerm = applyMsg.CommandTerm
				if !sm.ResultMap[id].Done {
					result := sm.applyOne(op)          // apply
					sm.ResultMap[id] = result
				}
			}else {
				DPrintf("[SM %v]: already Applied Command.. commitIndex = %v, applyIndex = %v",
					sm.me, sm.CommitIndex, applyMsg.CommandIndex)
			}
			_, requestIndex, clientId := parseRequestId(id)
			go sm.removePrevious(requestIndex, clientId)
		}else {
			if applyMsg.CommandTerm == -10 {
				DPrintf("[SM %v]: toFollower apply", sm.me)
			}else {
				// snapshot apply
				DPrintf("[SM %v]: snapshot apply, curData = %v, resultMap = %v, commitIndex = %v, commitTerm = %v",
					sm.me, sm.Data, sm.ResultMap, sm.CommitIndex, sm.CommitTerm)
				snapshot, _ := applyMsg.Command.([]byte)
				if len(snapshot) > 0 {
					sm.Data, sm.ResultMap, sm.CommitIndex, sm.CommitTerm = DecodeSnapshot(snapshot)
				}else {
					sm.Data = make(map[string]string)
					sm.ResultMap = make(map[string]Result)
					sm.CommitIndex = 0
					sm.CommitTerm = 0
				}
				DPrintf("[SM %v]: newData = %v, resultMap = %v, commitIndex = %v, commitTerm = %v",
					sm.me, sm.Data, sm.ResultMap, sm.CommitIndex, sm.CommitTerm)
			}
		}
		sm.mu.Unlock()
		sm.resultCond.Broadcast()
	}
}

// applyOne operation to key-value database
func (sm *ShardMaster) applyOne(op Op) (result Result) {
	result = Result{
		OpType: op.OpType,
		Err:    "",
		Done: true,
	}
	switch op.OpType {
	case JoinType:
	case LeaveType:
	case MoveType:
	case QueryType:
	}

	DPrintf("[SM %v]: applyOne, id = %v", sm.me, op.Id)
	return
}

func (sm *ShardMaster) applyJoin(op Op) {
	var newConfig Config
	err := deepCopy(newConfig, sm.configs[len(sm.configs)-1])
	if err != nil {
		DPrintf("[SM %v]: deepCopy ERROR", sm.me)
	}
}

// Kill
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	DPrintf("[KV %v]: killed..", sm.me)
	sm.resultCond.Broadcast()
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// Raft needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.Data = make(map[string]string)
	sm.ResultMap = make(map[string]Result)
	sm.resultCond = sync.NewCond(&sm.mu)
	sm.CommitIndex = 0
	sm.CommitTerm = 0
	sm.persister = persister

	go sm.applyLoop()

	DPrintf("[SM %v]: Initialized!!!", sm.me)

	return sm
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

func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}