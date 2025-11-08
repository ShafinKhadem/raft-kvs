package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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

}

type LogEntry struct {
	Term    int
	Command interface{}
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
	// return term, isleader
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("failed to read persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate ID
	LastLogIndex int // candidate's last log entry index
	LastLogTerm  int // candidate's last log entry term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true if candidate received a vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If the term in the request is less than the current term, reject the vote
	if args.Term < rf.currentTerm {
		return
	}

	// If the term in the request is greater than the current term, update the term and become a Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Check if the vote can be granted
	// 1. Not voted yet or already voted for this candidate
	// 2. Candidate's log is at least as up-to-date as own log
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeatTime = time.Now() // reset election timer
		rf.persist()
	}
}

func (rf *Raft) isLogUpToDate(candidateIndex, candidateTerm int) bool {
	lastIndex := len(rf.log) - 1
	lastTerm := 0
	if lastIndex >= 0 {
		lastTerm = rf.log[lastIndex].Term
	}

	// If the last log's term is different, update to the term with the larger value
	if candidateTerm != lastTerm {
		return candidateTerm > lastTerm
	}
	// If the terms are the same, update to the log with the larger index
	return candidateIndex >= lastIndex
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()

	// Immediately broadcast to replicate the new entry
	rf.broadcastAppendEntries()

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// applier goroutine applies committed log entries to the state machine
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// Wait until there are entries to apply
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		start := rf.lastApplied + 1
		end := rf.commitIndex
		entries := make([]LogEntry, 0)
		if end >= start && start >= 0 && end < len(rf.log) {
			entries = append(entries, rf.log[start:end+1]...)
		}
		rf.mu.Unlock()

		// Apply entries without holding the lock
		for i, e := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: start + i,
			}
			rf.mu.Lock()
			rf.lastApplied = start + i
			rf.mu.Unlock()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // Leader ID
	PrevLogIndex int        // Previous log entry index
	PrevLogTerm  int        // Previous log entry term
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commit index
}

type AppendEntriesReply struct {
	Term          int  // Current term
	Success       bool // If Follower contains matching prevLogIndex and prevLogTerm, return true
	ConflictTerm  int  // Term of the conflicting entry (for fast backup)
	ConflictIndex int  // Index of first entry with ConflictTerm (for fast backup)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// If the request's term is less than the current term, reject it
	if args.Term < rf.currentTerm {
		return
	}

	// If the request's term is greater than or equal to the current term, reset the election timer
	rf.lastHeartbeatTime = time.Now()

	// If a higher term is found, convert to Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// If the same term is found and the server is Candidate, convert to Follower
	if rf.state == Candidate {
		rf.becomeFollower(args.Term)
	}

	// Check if log is long enough
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}

	// Check if log has entry at PrevLogIndex with matching term
	if args.PrevLogIndex >= 0 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Conflict: log doesn't match
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			// Find first index with ConflictTerm
			i := args.PrevLogIndex
			for i >= 0 && rf.log[i].Term == reply.ConflictTerm {
				i--
			}
			reply.ConflictIndex = i + 1
			return
		}
	}

	// Log matches up to PrevLogIndex, now append new entries
	insertIdx := args.PrevLogIndex + 1
	if insertIdx < len(rf.log) {
		// Truncate any conflicting entries
		rf.log = append([]LogEntry{}, rf.log[:insertIdx]...)
	}
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}

	// Update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Signal()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower, Candidate:
			// check election timeout
			rf.mu.Lock()
			elapsed := time.Since(rf.lastHeartbeatTime)
			electionTimeout := time.Duration(300+rand.Int63()%200) * time.Millisecond // 300-500ms
			rf.mu.Unlock()

			if elapsed >= electionTimeout {
				rf.startElection()
			}
		case Leader:
			// send heartbeats periodically
			rf.mu.Lock()
			timeSinceLastHeartbeat := time.Since(rf.lastHeartbeatSent)
			rf.mu.Unlock()

			// Send heartbeat every 100ms (10 times per second)
			if timeSinceLastHeartbeat >= 100*time.Millisecond {
				rf.sendHeartbeats()
				rf.mu.Lock()
				rf.lastHeartbeatSent = time.Now()
				rf.mu.Unlock()
			}
		}

		// pause for a short time
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	rf.lastHeartbeatTime = time.Now() // reset election timer
	rf.persist()

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	rf.mu.Unlock()

	votes := 1 // vote for self
	var mu sync.Mutex

	// Send RequestVote RPC to all other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Check if the term has changed
			if rf.currentTerm != currentTerm || rf.state != Candidate {
				return
			}

			// If a higher term is found, convert to Follower
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			// Count votes
			if reply.VoteGranted {
				mu.Lock()
				votes++
				mu.Unlock()

				// If a majority of votes are received, become Leader
				if votes > len(rf.peers)/2 && rf.state == Candidate {
					rf.becomeLeader()
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	// Initialize nextIndex and matchIndex
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	last := rf.lastLogIndex() + 1
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = last
		rf.matchIndex[i] = -1
	}
	// Leader's own matchIndex and nextIndex
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.nextIndex[rf.me] = rf.lastLogIndex() + 1
	// Initialize last heartbeat sent time and immediately broadcast
	rf.lastHeartbeatSent = time.Time{} // zero time to trigger immediate heartbeat
	rf.broadcastAppendEntries()
}

// Become Follower
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		rf.applyCh <- raftapi.ApplyMsg{
			SteppedDown: true,
			Command:     rf.log[i].Command,
		}
	}
}

// Helper methods
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) firstIndexOfTerm(term int) int {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			return i
		}
	}
	return -1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// replicateTo handles the response from an AppendEntries RPC and retries if necessary
func (rf *Raft) replicateTo(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if !rf.sendAppendEntries(server, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check if we're still the leader and the term hasn't changed
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	// If the reply has a higher term, convert to follower
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		// Update matchIndex and nextIndex
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = max(rf.matchIndex[server], match)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// Try to advance commit index
		rf.advanceCommitIndex()
	} else {
		// Log replication failed, use conflict information to back up
		if reply.ConflictTerm != -1 {
			// Find the last entry with ConflictTerm
			idx := rf.firstIndexOfTerm(reply.ConflictTerm)
			if idx == -1 {
				// We don't have any entries with ConflictTerm
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				// We have entries with ConflictTerm, set nextIndex to after them
				rf.nextIndex[server] = idx
			}
		} else {
			// No ConflictTerm, just use ConflictIndex
			rf.nextIndex[server] = max(0, reply.ConflictIndex)
		}

		// Retry with updated nextIndex
		nextIdx := rf.nextIndex[server]
		prevIdx := nextIdx - 1
		prevTerm := 0
		if prevIdx >= 0 && prevIdx < len(rf.log) {
			prevTerm = rf.log[prevIdx].Term
		}
		var entries []LogEntry
		if nextIdx <= rf.lastLogIndex() {
			entries = append(entries, rf.log[nextIdx:]...)
		} else {
			entries = []LogEntry{}
		}
		newArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		newReply := &AppendEntriesReply{}
		go rf.replicateTo(server, newArgs, newReply)
	}
}

// advanceCommitIndex checks if we can advance the commit index
func (rf *Raft) advanceCommitIndex() {
	for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
		// Only commit entries from current term (safety requirement)
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		// Count how many servers have replicated this entry
		count := 1 // Count self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		// If a majority has replicated this entry, commit it
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break
		}
	}
}

// broadcastAppendEntries sends AppendEntries RPCs to all peers
func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}
	term := rf.currentTerm
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		server := i
		nextIdx := rf.nextIndex[server]
		prevIdx := nextIdx - 1
		prevTerm := 0
		if prevIdx >= 0 && prevIdx < len(rf.log) {
			prevTerm = rf.log[prevIdx].Term
		}
		var entries []LogEntry
		if nextIdx <= rf.lastLogIndex() {
			entries = append(entries, rf.log[nextIdx:]...)
		} else {
			entries = []LogEntry{}
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		go rf.replicateTo(server, args, reply)
	}
}

// Send heartbeats (wrapper for broadcastAppendEntries)
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.broadcastAppendEntries()
	rf.mu.Unlock()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.lastHeartbeatTime = time.Now()
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to apply committed entries
	go rf.applier()

	return rf
}
