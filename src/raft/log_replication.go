package raft

import (
	"time"

	"raft/raftapi"
)

// AppendEntries RPC handler.
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
	rf.leaderId = args.LeaderId

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
		for i := insertIdx; i < len(rf.log); i++ {
			rf.applyCh <- raftapi.ApplyMsg{
				Aborted: true,
				Command: rf.log[i].Command,
			}
		}
		rf.log = rf.log[:insertIdx]
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

// sendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// replicateTo handles the response from an AppendEntries RPC and retries if necessary.
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

// advanceCommitIndex checks if we can advance the commit index.
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

// broadcastAppendEntries sends AppendEntries RPCs to all peers.
// It's only called when rf.mu is locked.
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

// sendHeartbeats sends heartbeats to all peers (wrapper for broadcastAppendEntries).
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.broadcastAppendEntries()
	rf.mu.Unlock()
}
