package kvraft

import (
	"fmt"
	"mymr/src/labrpc"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers 			[]*labrpc.ClientEnd
	// You will have to modify this struct.

	LeaderId			int			// remember last leader
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
	id := fmt.Sprintf("%v+%v","Get", nrand())
	for  {
		server := ck.LeaderId
		if server == -1 {
			server = int(nrand() % int64(len(ck.servers)))
		}

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
			if !ok {
				//DPrintf("[CK]: Get network failed.. retrying")
			}else {
				//DPrintf("[CK]: Get failed..wrong leader, retrying")
			}
			ck.LeaderId = -1
		}else if ok && reply.Err == ErrNoKey {
			DPrintf("[CK]: Get failed.. No key! return null")
			return ""
		}else if ok && reply.Err == OK {
			DPrintf("[CK]: Get succeed, leaderId = %v, key = %v, value = %v",
				ck.LeaderId, args.Key, reply.Value)
			ck.LeaderId = server
			return reply.Value
		}else if ok && reply.Err == ErrAlreadyDone {
			DPrintf("[CK]: Get AlreadyDone, leaderId = %v, key = %v, value = %v",
				ck.LeaderId, args.Key, reply.Value)
			ck.LeaderId = server
			return reply.Value
		}
		//time.Sleep(10 * time.Millisecond)
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
	id := fmt.Sprintf("%v+%v", op, nrand())
	for  {
		server := ck.LeaderId
		if server == -1 {
			server = int(nrand() % int64(len(ck.servers)))
		}

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
		if !ok || reply.Err == ErrWrongLeader{
			// network failed  OR  wrong leader
			if !ok {
				//DPrintf("[CK]: PutAppend network failed.. retrying.. type = %v", op)
			}else {
				//DPrintf("[CK]: PutAppend failed.. wrong leader, retrying.. type = %v", op)
			}
			ck.LeaderId = -1
		}else if ok && reply.Err == OK {
			ck.LeaderId = server
			DPrintf("[CK]: PutAppend succeed, leaderId = %v, type = %v, key = %v,",
				ck.LeaderId, op, args.Key)
			return
		}else if ok && reply.Err == ErrAlreadyDone {		// if first RPC replyMsg lost in network, it'll get to ErrAlreadyDone
			ck.LeaderId = server
			DPrintf("[CK]: PutAppend AlreadyDone, leaderId = %v, type = %v, key = %v,",
				ck.LeaderId, op, args.Key)
			return
		}
		//time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) PutRequest(key string, value string) {
	ck.PutAppendRequest(key, value, "Put")
}
func (ck *Clerk) AppendRequest(key string, value string) {
	ck.PutAppendRequest(key, value, "Append")
}
