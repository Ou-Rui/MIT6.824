package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards The number of shards.
const NShards = 10

// Config A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    			int              // config number
	Shards 			[NShards]int     // shard -> gid
	Groups 			map[int][]string // gid -> servers[]
}

func (config *Config) reBalanceShards()  {

	// gid(int) -> shards([]int)
	gid2shards := make(map[int][]int)
	gid2shards[0] = make([]int,0)				// GID = 0 ---> UnAllocated
	for gid := range config.Groups {
		gid2shards[gid] = make([]int,0)
	}
	for i, gid := range config.Shards {
		gid2shards[gid] = append(gid2shards[gid], i)
	}
	groupNum := len(gid2shards)-1
	shardPerGroup := len(config.Shards) / groupNum
	DPrintf("reBalance(), gid2shards = %v, groupNum = %v, shardPerGroup = %v",
		gid2shards, groupNum, shardPerGroup)
	for {
		if config.isBalanced(gid2shards, shardPerGroup) {
			break
		}
		config.moveOne(gid2shards)
		// move the last shard in group maxGID
		//shard := gid2shards[maxGID][len(gid2shards[maxGID]) - 1]
		//gid2shards[maxGID] = gid2shards[maxGID] [ : len(gid2shards[maxGID]) - 1 ]
		//gid2shards[minGID] = append(gid2shards[maxGID], shard)
		//config.Shards[shard] = minGID
	}
}

func (config *Config) isBalanced(gid2shards map[int][]int, shardPerGroup int) bool {
	max, min := -1, NShards+1
	// unallocated shards remain
	if len(gid2shards[0]) != 0 {
		return false
	}
	for gid, shards := range gid2shards {
		if gid != 0 && len(shards) > max {
			max = len(shards)
		}
		if gid != 0 && len(shards) < min {
			min = len(shards)
		}
	}
	balanced := min == shardPerGroup && (max == min || max == min+1)
	return balanced
}

func (config *Config) moveOne(gid2shards map[int][]int) {
	//
	toGID := -1
	minShard := NShards + 1
	for gid, shards := range gid2shards {
		// toGID, group that contains the least shards && joining in the config
		if len(shards) < minShard && gid != 0 {
			minShard = len(shards)
			toGID = gid
		}
	}

	fromGID := -1
	if len(gid2shards[0]) != 0 {
		fromGID = 0
	}else {
		maxShard := -1
		for gid, shards := range gid2shards {
			// fromGID, group that contains the most shards && except 0
			if len(shards) > maxShard {
				maxShard = len(shards)
				fromGID = gid
			}
		}
	}
	// move the last shard in group maxGID
	shard := gid2shards[fromGID][len(gid2shards[fromGID]) - 1]
	gid2shards[fromGID] = gid2shards[fromGID] [ : len(gid2shards[fromGID]) - 1 ]
	gid2shards[toGID] = append(gid2shards[toGID], shard)
	config.Shards[shard] = toGID
	DPrintf("gid2shards = %v, Shards = %v, fromGID = %v, toGID = %v",
		gid2shards, config.Shards, fromGID, toGID)
}

const (
	OK = "OK"
	ErrNoKey       	= 	"ErrNoKey"
	ErrWrongLeader 	= 	"ErrWrongLeader"
	ErrAlreadyDone  = 	"ErrAlreadyDone"
	ErrNewTerm		=	"ErrNewTerm"
)

type Err string

type JoinArgs struct {
	Id 				string
	Servers 		map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err         	Err
}

type LeaveArgs struct {
	Id 				string
	GIDs 			[]int
}

type LeaveReply struct {
	Err         	Err
}

type MoveArgs struct {
	Id 				string
	Shard 			int
	GID   			int
}

type MoveReply struct {
	Err         	Err
}

type QueryArgs struct {
	Id 				string
	Num 			int // desired config number
}

type QueryReply struct {
	Err         	Err
	Config      	Config
}




