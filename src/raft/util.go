package raft

import (
	"log"
)

// Debug Debugging
// Debug = 0时，TestSnapshotRecover3B，logSize会过大？？？？似乎是因为snapshot不够频繁
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
