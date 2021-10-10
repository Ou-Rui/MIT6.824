package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "mymr/src/labrpc"

// import "bytes"
// import "../labgob"

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type raftState int

const (
	follower	raftState = 0
	candidate	raftState = 1
	leader		raftState = 2
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        					sync.Mutex          // Lock to protect shared access to this peer's state
	peers     					[]*labrpc.ClientEnd // RPC end points of all peers
	persister 					*Persister          // Object to hold this peer's persisted state
	me        					int                 // this peer's index into peers[]
	dead     				 	int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state						raftState  	// leader/follower/candidate
	currentTerm					int			// current Term number

	election					election
}

type election struct {
	// timeout
	timeout						int			// random timeout 500 ~ 1000 (ms)
	curTimeout					int			// if curTimeout > electionTimeout => timeout

	// votes
	votedFor					int			// candidateId, -1 for unvoted
	votes						int			// votes count, valid for state = candidate
	respond						int			// respond count, valid for state = candidate
	votesCond					*sync.Cond	// watch the change of votes count
}

func (e *election) reset() {
	e.timeout = getRandomTimeout()
	e.curTimeout = 0

	e.votedFor = -1
	e.votes = 0
	e.respond = 0
}

// timeoutLoop
// Goroutine, start by Make()
// handle election timeout
func (rf *Raft) timeoutLoop() {
	for  {
		rf.mu.Lock()

		if rf.killed() {
			rf.election.curTimeout = 0
			rf.mu.Unlock()
			return
		}
		rf.election.curTimeout += 10
		// if timeout, start a new election
		if rf.election.curTimeout > rf.election.timeout {
			rf.newElection()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForVoteLoop
// Goroutine, start by newElection()
func (rf *Raft) waitForVoteLoop() {
	for  {
		rf.mu.Lock()
		if rf.killed() || rf.state != candidate {
			rf.mu.Unlock()
			return
		}
		total := len(rf.peers)
		half := int(math.Ceil(float64(total) / 2))
		DPrintf("total = %v, half = %v", total, half)
		if rf.election.votes < half && rf.election.respond < total  {
			rf.election.votesCond.Wait()
		}
		DPrintf("[Raft %v]: Term = %v, Get respond = %v, votes = %v \n",
			rf.me, rf.currentTerm, rf.election.respond, rf.election.votes)
		if rf.election.votes >= half {
			// win the election
			DPrintf("[Raft %v]: Election Win!! Term = %v, Get respond = %v, votes = %v \n",
				rf.me, rf.currentTerm, rf.election.respond, rf.election.votes)
			rf.state = leader
			rf.election.reset()
			rf.mu.Unlock()
			return
		}else {
			// lose
			DPrintf("[Raft %v]: Election Lose.. Term = %v, Get respond = %v, votes = %v \n",
				rf.me, rf.currentTerm, rf.election.respond, rf.election.votes)
			rf.state = follower
			rf.election.reset()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// AppendEntriesLoop
// Goroutine, start by Make()
func (rf *Raft) AppendEntriesLoop() {
	for  {
		rf.mu.Lock()

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// send HeartBeat Msg
		if rf.state == leader {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sendAppendEntries(i)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// newElection require rf.mu.Lock()
func (rf *Raft) newElection() {
	rf.state = candidate
	rf.currentTerm++
	DPrintf("[Raft %v]: Election Timeout!! New Election Begins! Term = %v", rf.me, rf.currentTerm)
	rf.election.timeout = getRandomTimeout()
	rf.election.curTimeout = 0
	rf.election.votedFor = rf.me		// vote for myself
	rf.election.votes = 1
	rf.election.respond = 1

	// broadcast RequestVote
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestVote(i)
		}
	}

	go rf.waitForVoteLoop()
}


// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	}else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term  			int			// candidate current term
	CandidateId		int			// candidate who request for votes
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted		bool
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// Thread
func (rf *Raft) sendRequestVote(server int) {
	rf.mu.Lock()
	if rf.killed() || rf.state != candidate{
		rf.mu.Unlock()
		return
	}
	rvArgs := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
	}
	rvReply := RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", &rvArgs, &rvReply)
	rf.mu.Lock()
	if !ok {
		//DPrintf("[Raft %v]: RequestVote to Raft %v Failed.. Retrying.. \n",
		//	rf.me, server)
		//go rf.sendRequestVote(server)
		//rf.mu.Unlock()
		//return
	}
	// handle reply
	if rf.killed() || rf.state != candidate{
		rf.mu.Unlock()
		return
	}
	DPrintf("[Raft %v]: RequestVote to Raft %v Success!",
		rf.me, server)
	rf.election.respond++
	if rvReply.Term > rf.currentTerm {
		rf.currentTerm = rvReply.Term
		rf.state = follower
		rf.election.reset()
	}
	if rvReply.VoteGranted {
		DPrintf("Get One Vote!!")
		// get one vote!
		rf.election.votes++
	}
	DPrintf("respond = %v, votes = %v \n", rf.election.respond, rf.election.votes)
	rf.election.votesCond.Signal()
	rf.mu.Unlock()
	return
}

// RequestVote
// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	DPrintf("[Raft %v]: RequestVote from Raft %v. myState = %v, myTerm = %v, hisTerm = %v, myVoteFor = %v \n",
		rf.me, args.CandidateId, rf.state, rf.currentTerm, args.Term, rf.election.votedFor)

	if args.Term >= rf.currentTerm {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.state = follower
		rf.election.reset()
		if rf.election.votedFor == -1  {
			reply.VoteGranted = true
			rf.election.votedFor = args.CandidateId
		}
		rf.election.curTimeout = 0
	}else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}



type AppendEntriesArgs struct {
	Term 				int
	LeaderId			int
}

type AppendEntriesReply struct {
	Term 				int
	Success				bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Raft %v]: AppendEntries from Raft %v. myState = %v, myTerm = %v, hisTerm = %v\n",
		rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.election.reset()
		reply.Success = true
		reply.Term = args.Term
	}else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}

}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	aeArgs := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	aeReply := AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", &aeArgs, &aeReply)
	rf.mu.Lock()
	if !ok {
		DPrintf("[Raft %v]: AppendEntries to Raft %v Failed.. Retrying.. \n",
			rf.me, server)
		//go rf.sendAppendEntries(server)
		rf.mu.Unlock()
		return
	}
	// handle ae reply
	rf.election.curTimeout = 0
	//DPrintf("[Raft %v]: AppendEntries to Raft %v Success! Term = %v \n",
	//	rf.me, server, rf.currentTerm)
	if aeReply.Term > rf.currentTerm {
		rf.currentTerm = aeReply.Term
		rf.state = follower
		rf.election.reset()
	}
	rf.mu.Unlock()
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("[Raft %v]: be killed...", rf.me)
	time.Sleep(1*time.Second)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.currentTerm = 0
	// initialize election field (2A)
	rf.election.votesCond = sync.NewCond(&rf.mu)
	rf.election.reset()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("[Raft %v]: Initialized!", rf.me)

	go rf.timeoutLoop()

	go rf.AppendEntriesLoop()

	return rf
}

// getRandomTimeout generate random timeout between [500 ~ 1000) ms
func getRandomTimeout() int {
	return rand.Intn(1000) + 1000
}
