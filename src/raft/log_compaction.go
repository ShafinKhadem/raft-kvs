package raft

// Snapshot is called by the service to indicate that it has created a snapshot
// that has all info up to and including index. This means the service no longer
// needs the log through (and including) that index. Raft should now trim its
// log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}
