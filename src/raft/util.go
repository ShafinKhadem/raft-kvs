package raft

import "log"

// Debugging
const Debug = false

// DPrintf prints debug messages if Debug is enabled.
func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// Helper function to get the last log index.
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// Helper function to find the first index of a given term.
func (rf *Raft) firstIndexOfTerm(term int) int {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			return i
		}
	}
	return -1
}

// Helper function to return minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Helper function to return maximum of two integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
