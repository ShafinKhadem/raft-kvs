package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft/labrpc"
	"raft/raftapi"
	tester "raft/tester1"
)

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start initiates agreement on the next command to be appended to Raft's log.
// If this server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
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

// abortPendingCommands sends abort messages for all uncommitted log entries.
func (rf *Raft) abortPendingCommands() {
	for i := rf.lastApplied + 1; i < len(rf.log); i++ {
		rf.applyCh <- raftapi.ApplyMsg{
			Aborted: true,
			Command: rf.log[i].Command,
		}
	}
}

// Kill is called by the tester to shut down this Raft instance.
// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.abortPendingCommands()
}

// killed checks whether this Raft instance has been killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker is the main loop that handles elections and heartbeats.
func (rf *Raft) ticker() {
	for !rf.killed() {

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

// applier goroutine applies committed log entries to the state machine.
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

// Make creates a new Raft server instance.
// The ports of all the Raft servers (including this one) are in peers[].
// This server's port is peers[me]. All the servers' peers[] arrays
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

	// Initialize Raft state
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

	// For batch read confirmation
	rf.readConfirmations = make(map[int64]*ReadConfirmation)
	rf.nextReadId = 0

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start ticker goroutine to start elections
	go rf.ticker()

	// Start applier goroutine to apply committed entries
	go rf.applier()

	return rf
}
