package raft

import (
	"sync"
	"time"
)

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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

// isLogUpToDate checks if the candidate's log is at least as up-to-date as this server's log.
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

// sendRequestVote sends a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// startElection initiates a new election.
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

// becomeLeader transitions this server to the Leader state.
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

// becomeFollower transitions this server to the Follower state.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}
