package raft

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
	// fast backup
	XTerm				int
	XIndex				int
	XLen				int
	// snapshot
	Snapshot            bool
}

type InstallSnapshotArgs struct {
	Term 				int
	LeaderId  			int
	LastIncludeIndex	int
	LastIncludeTerm		int
	Data 				[]byte
}

type InstallSnapshotReply struct {
	Term 				int
	NextIndex			int
}
