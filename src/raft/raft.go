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
	leaderId					int
	applyCh						chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state						raftState  	// leader/follower/candidate
	curTerm						int			// current Term number

	election					election
	logState					logState
}

// election related info, part of Raft
type election struct {
	// timeout
	timeout						int			// random timeout 500 ~ 1000 (ms)
	curTimeout					int			// if curTimeout > electionTimeout => timeout

	// votes
	votedFor					int			// candidateId, -1 for unvoted
	votes						int			// votes count, valid for state = candidate
	respond						int			// respond count, valid for state = candidate
	votesCond					*sync.Cond	// watch the change of votes count
	votesSendWg					*sync.WaitGroup
}

// logState related info, a part of Raft (2B)
type logState struct {
	logs						[]logEntry				// slice of log entries
	commitIndex					int					// highest index of logs that has been committed
	lastApplied					int					// highest index of logs that applied to state machine

	// valid when rf.state == leader
	nextIndex					[]int				// index of next log to send to each server
	matchIndex					[]int				// highest index of log known to be replicated for each server
	commitCond					*sync.Cond
}

// log entry struct
type logEntry struct {
	Command 					interface{}			// command executed by state machine
	Term 						int					// the term when the log was received by leader
}

// lock needed
func (e *election) reset() {
	e.timeout = getRandomTimeout()
	e.curTimeout = 0

	e.votedFor = -1
	e.votes = 0
	e.respond = 0

	e.votesSendWg = &sync.WaitGroup{}
}
// lock needed
func (rf *Raft) toLeader()  {
	DPrintf("[Raft %v]: to Leader, preState = %v, Term = %v \n",
		rf.me, rf.state, rf.curTerm)
	rf.state = leader
	rf.leaderId = rf.me
	rf.election.reset()


	// reset nextIndex[] and matchIndex[]
	rf.logState.nextIndex = make([]int, len(rf.peers))
	for i := range rf.logState.nextIndex {
		rf.logState.nextIndex[i] = len(rf.logState.logs)
	}
	rf.logState.matchIndex = make([]int, len(rf.peers))
	for i := range rf.logState.matchIndex {
		rf.logState.matchIndex[i] = 0
	}
	DPrintf("[Raft %v]: reset nextIndex = %v, matchIndex = %v \n",
		rf.me, rf.logState.nextIndex, rf.logState.matchIndex)

	rf.logState.commitCond = sync.NewCond(&rf.mu)
	go rf.updateCommitLoop(rf.curTerm)
}
// lock needed
func (rf *Raft) toCandidate()  {
	DPrintf("[Raft %v]: to Candidate, preState = %v, preTerm = %v \n",
		rf.me, rf.state, rf.curTerm)
	rf.state = candidate
	rf.curTerm++
	DPrintf("[Raft %v]: Election Timeout!! New Election Begins! Term = %v", rf.me, rf.curTerm)
	rf.election.timeout = getRandomTimeout()
	rf.election.curTimeout = 0
	rf.election.votedFor = rf.me		// vote for myself
	rf.election.votes = 1
	rf.election.respond = 1
	rf.newElection()
}
// lock needed
func (rf *Raft) toFollower(term int)  {
	DPrintf("[Raft %v]: to Follower, preState = %v, preTerm = %v, NewTerm = %v \n",
		rf.me, rf.state, rf.curTerm, term)
	rf.state = follower
	rf.curTerm = term
	rf.election.reset()
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
			rf.toCandidate()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForVoteLoop
// Goroutine, start by newElection()
func (rf *Raft) waitForVoteLoop(term int) {
	rf.election.votesSendWg.Wait()
	for  {
		rf.mu.Lock()
		if rf.killed() || rf.state != candidate {
			rf.mu.Unlock()
			return
		}
		total := len(rf.peers)
		half := int(math.Ceil(float64(total) / 2))
		//DPrintf("total = %v, half = %v", total, half)
		if rf.election.votes < half && rf.election.respond < total  {
			rf.election.votesCond.Wait()
		}
		if rf.curTerm != term {
			DPrintf("[Raft %v]: Election Wakeup, but Term dont match... electionTerm = %v, curTerm = %v \n",
				rf.me, term, rf.curTerm)
			rf.mu.Unlock()
			return
		}
		DPrintf("[Raft %v]: Term = %v, Get respond = %v, votes = %v \n",
			rf.me, rf.curTerm, rf.election.respond, rf.election.votes)
		if rf.election.votes >= half {
			// win the election
			DPrintf("[Raft %v]: Election Win!! Term = %v, Get respond = %v, votes = %v \n",
				rf.me, rf.curTerm, rf.election.respond, rf.election.votes)
			rf.toLeader()		// become Leader!!
			rf.mu.Unlock()
			return
		}else {
			// lose
			DPrintf("[Raft %v]: Election Lose.. Trans to follower, Term = %v, Get respond = %v, votes = %v \n",
				rf.me, rf.curTerm, rf.election.respond, rf.election.votes)
			rf.toFollower(rf.curTerm)		// election lose, to follower
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
			rf.election.curTimeout = 0		// if leader, never timeout
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

func (rf *Raft) updateCommitLoop(term int) {
	for  {
		rf.mu.Lock()
		if rf.killed() || rf.state != leader || term != rf.curTerm {
			DPrintf("[Raft %v]: updateCommitLoop of term %v return.., state = %v, curTerm = %v",
				rf.me, term, rf.state, rf.curTerm)
			rf.mu.Unlock()
			return
		}
		rf.logState.commitCond.Wait()
		if rf.killed() || rf.state != leader || term != rf.curTerm{
			DPrintf("[Raft %v]: updateCommitLoop of term %v return.., state = %v, curTerm = %v",
				rf.me, term, rf.state, rf.curTerm)
			rf.mu.Unlock()
			return
		}

		total := len(rf.peers)
		half := int(math.Ceil(float64(total) / 2))
		nextCommit := rf.logState.commitIndex
		cnt := 0
		for {
			for i := 0; i < total; i++ {
				if rf.logState.matchIndex[i] > nextCommit {
					cnt++
				}
			}
			if cnt >= half {
				nextCommit++
			}else {
				break
			}
			cnt = 0
		}
		if nextCommit > rf.logState.commitIndex && rf.logState.logs[nextCommit].Term == rf.curTerm {
			for i := rf.logState.commitIndex + 1; i <= nextCommit; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logState.logs[i].Command,
					CommandIndex: i,
				}
				rf.applyCh <- applyMsg
				DPrintf("[Raft %v]: Leader apply: %v",
					rf.me, applyMsg)
			}

			rf.logState.commitIndex = nextCommit
			DPrintf("[Raft %v]: Leader update commitIndex = %v, curTerm = %v",
				rf.me, rf.logState.commitIndex, rf.curTerm)

		}
		rf.mu.Unlock()
	}
}

// newElection require rf.mu.Lock()
// call by timeoutLoop() when timeout occur
func (rf *Raft) newElection() {

	// broadcast RequestVote
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.election.votesSendWg.Add(1)
			go rf.sendRequestVote(i)
		}
	}
	go rf.waitForVoteLoop(rf.curTerm)
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.curTerm
	if rf.state == leader {
		isLeader = true
	}else {
		isLeader = false
	}

	return term, isLeader
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

	// 2B leader restriction
	LastLogIndex	int			// candidate's last log index
	LastLogTerm		int			// candidate's term of last log
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
// Goroutine
func (rf *Raft) sendRequestVote(server int) {
	rf.mu.Lock()
	if rf.killed() || rf.state != candidate{
		rf.mu.Unlock()
		return
	}
	rvArgs := RequestVoteArgs{
		Term:         rf.curTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logState.logs) - 1,
		LastLogTerm:  rf.logState.logs[len(rf.logState.logs) - 1].Term,
	}
	DPrintf("[Raft %v]: send RequestVote to Raft %v, args = %v \n",
		rf.me, server, rvArgs)
	rvReply := RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
	rf.election.votesSendWg.Done()
	rf.mu.Unlock()

	// RPC Call
	ok := rf.peers[server].Call("Raft.RequestVote", &rvArgs, &rvReply)

	// handle reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		//DPrintf("[Raft %v]: RequestVote to Raft %v Failed.. Retrying.. \n",
		//	rf.me, server)
		//go rf.sendRequestVote(server)
		//rf.mu.Unlock()
		//return
	}
	if rf.killed() || rf.state != candidate{
		return
	}
	DPrintf("[Raft %v]: RequestVote to Raft %v Success!",
		rf.me, server)
	rf.election.respond++
	if rvReply.Term > rf.curTerm {
		rf.toFollower(rvReply.Term)
	}
	if rvReply.VoteGranted {
		DPrintf("Get One Vote!!")
		// get one vote!
		rf.election.votes++
	}
	DPrintf("respond = %v, votes = %v \n", rf.election.respond, rf.election.votes)
	rf.election.votesCond.Broadcast()
	return
}

// RequestVote
// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	DPrintf("[Raft %v]: RequestVote", rf.me)
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}

	DPrintf("[Raft %v]: RequestVote from Raft %v. myState = %v, myTerm = %v, hisTerm = %v, myVoteFor = %v \n",
		rf.me, args.CandidateId, rf.state, rf.curTerm, args.Term, rf.election.votedFor)
	// leader restriction (2B)
	lastLogIndex := len(rf.logState.logs)-1
	lastLogTerm := rf.logState.logs[lastLogIndex].Term
	logFlag := false
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		logFlag = true
	}
	DPrintf("[Raft %v]: logFlag = %v, hisLastLogTerm = %v, myLastLogTerm = %v, hisLastLogIndex = %v, myLastLogIndex = %v \n",
		rf.me, logFlag, args.LastLogTerm, lastLogTerm, args.LastLogIndex, lastLogIndex)

	if args.Term > rf.curTerm {
		rf.toFollower(args.Term)

		reply.Term = args.Term
		if rf.election.votedFor == -1 && logFlag {
			reply.VoteGranted = true
			rf.election.votedFor = args.CandidateId
		}else {
			reply.VoteGranted = false
		}
	}else {
		reply.Term = rf.curTerm
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term 				int
	LeaderId			int
	// log replication (2B)
	PreLogIndex			int					// for checking Log Matching
	PreLogTerm			int					// for checking Log Matching
	Entries				[]logEntry			// logEntries for followers to append, according to nextIndex[]
	LeaderCommit		int
}

type AppendEntriesReply struct {
	Term 				int
	Success				bool
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.killed() || rf.state != leader{
		rf.mu.Unlock()
		return
	}
	nextLogIndex := rf.logState.nextIndex[server]
	preLogIndex := nextLogIndex - 1
	preLogTerm := rf.logState.logs[preLogIndex].Term
	entries := rf.logState.logs[nextLogIndex : ]
	aeArgs := AppendEntriesArgs{
		Term:         rf.curTerm,
		LeaderId:     rf.me,
		PreLogIndex:  preLogIndex,
		PreLogTerm:   preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.logState.commitIndex,
	}
	aeReply := AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	rf.mu.Unlock()
	// RPC Call
	ok := rf.peers[server].Call("Raft.AppendEntries", &aeArgs, &aeReply)

	// handle ae reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		DPrintf("[Raft %v]: AppendEntries to Raft %v Failed.. myState = %v \n",
			rf.me, server, rf.state)
		//go rf.sendAppendEntries(server)
		return
	}
	//rf.election.curTimeout = 0

	if aeReply.Success {
		rf.logState.nextIndex[server] = nextLogIndex + len(entries)
		rf.logState.matchIndex[server] = nextLogIndex + len(entries) - 1
		rf.logState.commitCond.Signal()
		DPrintf("[Raft %v]: AppendEntries to Raft %v Success! \n",
			rf.me, server)
	}else {
		DPrintf("[Raft %v]: AppendEntries to Raft %v Failed... \n",
			rf.me, server)
		if aeReply.Term > rf.curTerm {
			DPrintf("[Raft %v]: myTerm = %v, hisTerm = %v, toFollower() \n",
				rf.me, rf.curTerm, aeReply.Term)
			rf.toFollower(aeReply.Term)
		}else {
			rf.logState.nextIndex[server]--
			DPrintf("[Raft %v]: still Leader, Log Matching error, new nextIndex = %v\n",
				rf.me, rf.logState.nextIndex)
		}
	}

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	DPrintf("[Raft %v]: AppendEntries from Raft %v. myState = %v, myTerm = %v, hisTerm = %v\n",
		rf.me, args.LeaderId, rf.state, rf.curTerm, args.Term)
	if args.Term >= rf.curTerm {
		if args.Term > rf.curTerm {
			rf.toFollower(args.Term)
		}else {
			rf.election.reset()
		}
		rf.leaderId = args.LeaderId
		reply.Success = true
		reply.Term = args.Term
	}else {
		reply.Success = false
		reply.Term = rf.curTerm
	}

	// 2B
	if len(rf.logState.logs) <= args.PreLogIndex ||
		rf.logState.logs[args.PreLogIndex].Term != args.PreLogTerm {
		// preLog not match
		reply.Success = false
	}

	for i, entry := range args.Entries {
		if len(rf.logState.logs) - 1 >= args.PreLogIndex + i + 1 &&
			rf.logState.logs[args.PreLogIndex + i + 1].Term != entry.Term {
			rf.logState.logs = rf.logState.logs[ : args.PreLogIndex + i + 1]
			DPrintf("[Raft %v]: delete conflict logs, new logs = %v",
				rf.me, rf.logState.logs)
		}
		rf.logState.logs = append(rf.logState.logs, entry)
		DPrintf("[Raft %v]: all logs appended, new logs = %v",
			rf.me, rf.logState.logs)
	}

	// update commitIndex
	if args.LeaderCommit > rf.logState.commitIndex {
		tmpIndex := 0
		if args.LeaderCommit > len(rf.logState.logs)-1 {
			tmpIndex = len(rf.logState.logs)-1
		}else {
			tmpIndex = args.LeaderCommit
		}
		for i := rf.logState.commitIndex + 1; i <= tmpIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logState.logs[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- applyMsg
			DPrintf("[Raft %v]: Follower apply: %v",
				rf.me, applyMsg)
		}
		rf.logState.commitIndex = tmpIndex
		DPrintf("[Raft %v]: Follower update commitIndex = %v, curTerm = %v",
			rf.me, rf.logState.commitIndex, rf.curTerm)
	}
}

// Start
// lock required
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
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
	term, isLeader = rf.GetState()
	if isLeader && !rf.killed() {
		rf.logState.logs = append(rf.logState.logs, logEntry{
			Command: command,
			Term:    term,
		})
		index = len(rf.logState.logs) - 1
		DPrintf("[Raft %v]: Receive Command, Leader! append to log! command = %v, term = %v, index = %v",
			rf.me, command, term, index)
	}else {
		DPrintf("[Raft %v]: Receive Command, Im not Leader.. ignore.. command = %v",
			rf.me, command)
	}

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
	DPrintf("[Raft %v]: be killed... logs = %v", rf.me, rf.logState.logs)
	//time.Sleep(1*time.Second)
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.curTerm = 0
	// initialize election fields (2A)
	rf.election.votesCond = sync.NewCond(&rf.mu)
	rf.election.reset()

	// initialize logState fields (2B)
	rf.logState.logs = make([]logEntry, 0)
	rf.logState.logs = append(rf.logState.logs, logEntry{
		Command: nil,
		Term:    0,
	})
	rf.logState.commitIndex = 0
	rf.logState.lastApplied = 0
	rf.logState.nextIndex = make([]int, len(rf.peers))
	rf.logState.matchIndex = make([]int, len(rf.peers))


	// initialize from state persisted before a crash (2C)
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
