package raft

import (
	"sync"
	"time"

	"raft/labrpc"
	"raft/raftapi"
	tester "raft/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers
	currentTerm int        // current term
	votedFor    int        // vote for in current term
	log         []LogEntry // log entries

	// Volatile state on all servers
	commitIndex int // the highest log index known to be committed
	lastApplied int // the highest log index applied to the state machine

	// Volatile state on leaders
	nextIndex  []int // the next log index to send to each follower
	matchIndex []int // the highest log index known to be replicated on each follower

	// Additional state
	state             ServerState // Follower, Candidate, or Leader
	lastHeartbeatTime time.Time   // last time received heartbeat (for Follower/Candidate)
	lastHeartbeatSent time.Time   // last time sent heartbeat (for Leader)

	applyCh   chan raftapi.ApplyMsg // channel for applying logs
	applyCond *sync.Cond            // condition variable for efficient log application

	// For read optimization
	readConfirmations map[int64]*ReadConfirmation // track pending read confirmations
	nextReadId        int64                       // monotonic read ID generator
	readMu            sync.Mutex                  // protects read-related fields

}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ServerState represents the state of a Raft server.
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate ID
	LastLogIndex int // candidate's last log entry index
	LastLogTerm  int // candidate's last log entry term
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true if candidate received a vote
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // Leader ID
	PrevLogIndex int        // Previous log entry index
	PrevLogTerm  int        // Previous log entry term
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commit index
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term          int  // Current term
	Success       bool // If Follower contains matching prevLogIndex and prevLogTerm, return true
	ConflictTerm  int  // Term of the conflicting entry (for fast backup)
	ConflictIndex int  // Index of first entry with ConflictTerm (for fast backup)
}

// Track the amount of reads on clients to finally execute on leader locally
type ReadConfirmation struct {
	term     int
	response int
	needed   int
	done     chan bool
	mu       sync.Mutex
}
