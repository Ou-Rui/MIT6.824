package shardmaster

//
// Shardmaster clerk.
//

import (
	"fmt"
	"mymr/src/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu   				sync.Mutex
	leaderId			int			// remember last leader
	clientId			int64
	requestIndex		int			// assume: one client send just one request at a time

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.requestIndex = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// args
	args := &QueryArgs{}
	id := fmt.Sprintf("%v+%v+%v", QueryType, ck.requestIndex, ck.clientId)
	args.Id = id
	args.Num = num

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.Err == OK {
				ck.requestIndex++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// args
	args := &JoinArgs{}
	id := fmt.Sprintf("%v+%v+%v", JoinType, ck.requestIndex, ck.clientId)
	args.Id = id
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.Err == OK {
				ck.requestIndex++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// args
	args := &LeaveArgs{}
	id := fmt.Sprintf("%v+%v+%v", LeaveType, ck.requestIndex, ck.clientId)
	args.Id = id
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.Err == OK {
				ck.requestIndex++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// args
	args := &MoveArgs{}
	id := fmt.Sprintf("%v+%v+%v", MoveType, ck.requestIndex, ck.clientId)
	args.Id = id
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.Err == OK {
				ck.requestIndex++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
