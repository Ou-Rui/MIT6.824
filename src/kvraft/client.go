package kvraft

import (
	"fmt"
	"mymr/src/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers 			[]*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand() % 1000000
	ck.requestIndex = 0
	return ck
}

// GetRequest
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) GetRequest(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	id := fmt.Sprintf("%v+%v+%v","Get", ck.requestIndex, ck.clientId)
	//id := fmt.Sprintf("%v+%v", "Get", nrand())
	for  {
		server := ck.getServerIndex()

		args := GetArgs{
			Key: key,
			Id: id,
		}
		reply := GetReply{
			Err:   "",
			Value: "",
		}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			// network failed  OR  wrong leader
			switch ok {
			case true:
				//DPrintf("[CK]: Get failed..wrong leader, retrying")
			case false:
				DPrintf("[CK]: Get network failed.. retrying")
			}
			ck.leaderId = -1
		}else if ok && (reply.Err == OK || reply.Err == ErrAlreadyDone || reply.Err == ErrNoKey) {
			switch reply.Err {
			case OK:
				DPrintf("[CK]: Get succeed, leaderId = %v, key = %v, value = %v, requestIndex = %v",
					ck.leaderId, args.Key, reply.Value, ck.requestIndex)
			case ErrAlreadyDone:
				DPrintf("[CK]: Get AlreadyDone, leaderId = %v, key = %v, value = %v, requestIndex = %v",
					ck.leaderId, args.Key, reply.Value, ck.requestIndex)
			case ErrNoKey:
				DPrintf("[CK]: Get failed.. No key! return null")
			}
			ck.leaderId = server
			ck.requestIndex++
			return reply.Value
		}else if ok && reply.Err == ErrNewTerm {			// term has changed, the log may not be committed infinitely, retry!
			ck.leaderId = server
			DPrintf("[CK]: Get NewTerm, leaderId = %v, id = %v, key = %v,",
				ck.leaderId, id, args.Key)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// PutAppendRequest
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppendRequest(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	id := fmt.Sprintf("%v+%v+%v", op, ck.requestIndex, ck.clientId)
	//id := fmt.Sprintf("%v+%v", op, nrand())
	for  {
		server := ck.getServerIndex()

		args := PutAppendArgs{
			Key:   		key,
			Value: 		value,
			Op:    		op,
			Id: 		id,
		}
		reply := PutAppendReply{
			Err: "",
		}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			// network failed  OR  wrong leader
			switch ok {
			case true:
				//DPrintf("[CK]: PutAppend failed.. wrong leader, retrying.. id = %v", id)
			case false:
				DPrintf("[CK]: PutAppend network failed.. retrying.. id = %v", id)
			}
			ck.leaderId = -1
		}else if ok && (reply.Err == OK || reply.Err == ErrAlreadyDone) {
			switch reply.Err {
			case OK:
				DPrintf("[CK]: PutAppend succeed, leaderId = %v, id = %v, key = %v, value = %v requestIndex = %v",
					server, id, args.Key, args.Value, ck.requestIndex)
			case ErrAlreadyDone:
				DPrintf("[CK]: PutAppend AlreadyDone, leaderId = %v, id = %v, key = %v, requestIndex = %v",
					server, id, args.Key, ck.requestIndex)
			}
			ck.leaderId = server
			ck.requestIndex++
			return
		}else if ok && reply.Err == ErrNewTerm {			// term has changed, the log may not be committed infinitely, retry!
			ck.leaderId = server
			DPrintf("[CK]: PutAppend NewTerm.. retrying.. leaderId = %v, id = %v, key = %v,",
				ck.leaderId, id, args.Key)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) PutRequest(key string, value string) {
	ck.PutAppendRequest(key, value, "Put")
}
func (ck *Clerk) AppendRequest(key string, value string) {
	ck.PutAppendRequest(key, value, "Append")
}

func (ck *Clerk) getServerIndex() int {
	server := ck.leaderId
	if server == -1 {
		server = int(nrand() % int64(len(ck.servers)))
	}
	return server
}