package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"




type RpcGetArgs struct {

}

type RpcGetReply struct {
	TaskType 		string    // map or reduce or wait
	TaskId			int
	FileName 		string
	NReduce			int
}

type RpcPostArgs struct {
	TaskType 		string    // map or reduce or wait
	FileName 		string
}

type RpcPostReply struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
