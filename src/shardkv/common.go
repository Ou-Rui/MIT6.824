package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrAlreadyDone = "ErrAlreadyDone"
	ErrNewTerm     = "ErrNewTerm"
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
	Term        int
	ConfigIndex int
}

type ShardReply struct {
	Term        int
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
