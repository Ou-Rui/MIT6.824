package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"fmt"
	"mymr/src/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"
import "mymr/src/shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	//leaderId			int			// remember last leader
	clientId     int64
	requestIndex int // assume: one client send just one request at a time
}

// MakeClerk
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestIndex = 0
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	id := fmt.Sprintf("%v+%v+%v", "Get", ck.requestIndex, ck.clientId)

	args := GetArgs{}
	args.Key = key
	args.Id = id
	args.configIndex = ck.config.Num

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("[CK]: Get succeed, Err = %v, server = %v-%v, id = %v, key = %v, value = %v",
						reply.Err, gid, si, id, key, reply.Value)
					ck.requestIndex++
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("[CK]: Get WrongGroup, configIndex = %v, server = %v-%v, id = %v, key = %v",
						ck.config.Num, gid, si, id, key)
					// sleep and query for latest config
					break
				}
				// ... not ok, or ErrWrongLeader, try other servers

			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// PutAppend
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	id := fmt.Sprintf("%v+%v+%v", op, ck.requestIndex, ck.clientId)

	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Id = id
	args.configIndex = ck.config.Num

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("[CK]: PutAppend succeed, server = %v-%v, id = %v, key = %v, value = %v",
						gid, si, id, key, args.Value)
					ck.requestIndex++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("[CK]: Get WrongGroup, configIndex = %v, server = %v-%v, id = %v, key = %v",
						ck.config.Num, gid, si, id, key)
					// sleep and query for latest config
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
