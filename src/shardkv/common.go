package shardkv

import (
	"bytes"
	"encoding/gob"
	"mymr/src/labgob"
	"strconv"
	"strings"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

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

	ConfigIndex int
}

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrAlreadyDone      = "ErrAlreadyDone"
	ErrNewTerm          = "ErrNewTerm"
	ErrNotReady         = "ErrNotReady"
	ErrWrongConfigIndex = "ErrWrongConfigIndex"
	ErrWrongOwner       = "ErrWrongOwner"
	ErrKilled           = "ErrKilled"
	ErrContinue         = "ErrContinue"
	ErrExit             = "ErrExit"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id          string
	ConfigIndex int // for server to check the belonging of key
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id          string
	ConfigIndex int // for server to check the belonging of key
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardArgs struct {
	Shard       int // Shard Index
	ConfigIndex int // Caller's current ConfigIndex
	Gid         int
	Server      int
	QueryIndex  int // target ConfigIndex of the Shard
}

type ShardReply struct {
	ConfigIndex int
	Data        map[string]string
	ResultMap   map[string]Result
	Err         Err
}

//type CommitArgs struct {
//	Shard       int
//	Term        int
//	ConfigIndex int
//}
//
//type CommitReply struct {
//	Term        int
//	ConfigIndex int
//	Err         Err
//}

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
	Data map[string]string, ResultMap map[string]Result, CommitIndex int, CommitTerm int, OnCharge []int) {
	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)
	if decoder.Decode(&Data) != nil ||
		decoder.Decode(&ResultMap) != nil ||
		decoder.Decode(&CommitIndex) != nil ||
		decoder.Decode(&CommitTerm) != nil ||
		decoder.Decode(&OnCharge) != nil {
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

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}
