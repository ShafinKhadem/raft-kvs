package rsm

import (
	"math/rand/v2"
	"sync"
	"time"

	"raft/labrpc"
	raft "raft/raft"
	"raft/raftapi"
	"raft/rpc"
	tester "raft/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int64
	Me  int
	Req any
}

type ReadRequest struct {
	req    any // The Read op
	term   int // Term when this was executed
	result any
	done   chan rpc.Err // Ok or ErrWrongLeader
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	opChs map[int64]chan any

	readBatch     []*ReadRequest // Collect all pending reads
	readBatchMu   sync.Mutex     // Batch read own lock
	batchInterval time.Duration  // Flush period
	stopBatcher   chan struct{}  // Signal to stop flushing

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		opChs:        make(map[int64]chan any),

		// Initialize batch fields
		readBatch:     make([]*ReadRequest, 0),
		batchInterval: 10 * time.Millisecond,
		stopBatcher:   make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// reader goroutine
	go func() {
		for msg := range rsm.applyCh {
			if msg.CommandValid {
				// apply msg.Command to rsm.sm
				opResult := rsm.sm.DoOp(msg.Command.(Op).Req)
				rsm.mu.Lock()
				ch, ok := rsm.opChs[msg.Command.(Op).Id]
				rsm.mu.Unlock()
				if ok {
					ch <- opResult
				}
			} else if msg.SnapshotValid {
				// install snapshot msg.Snapshot to rsm.sm
				rsm.sm.Restore(msg.Snapshot)
			} else if msg.Aborted {
				rsm.mu.Lock()
				ch, ok := rsm.opChs[msg.Command.(Op).Id]
				rsm.mu.Unlock()
				if ok {
					ch <- "aborted"
				}
			}
		}
	}()

	// Batch flushing routine
	go rsm.batchFlusher()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	id := rand.Int64()
	ch := make(chan any)
	rsm.opChs[id] = ch
	rsm.mu.Unlock()

	// Clear the opCh when done
	defer func() {
		rsm.mu.Lock()
		delete(rsm.opChs, id)
		rsm.mu.Unlock()
	}()

	op := Op{Me: rsm.me, Id: id, Req: req}
	_, termBeforeOp, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	opResult := <-ch

	if opResult == "aborted" {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	termAfterOp, isLeader := rsm.rf.GetState()
	if isLeader && termBeforeOp == termAfterOp {
		return rpc.OK, opResult
	}

	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

// SubmitRead executes a read-only operation with leadership confirmation.
// Executes READ locally without going through the Raft log.
// NOT BEING USED - was for testing local read execution
func (rsm *RSM) SubmitRead(req any) (rpc.Err, any) {
	// Check if we're the leader
	termBefore, isLeader := rsm.rf.GetState()
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	// Execute the read locally without Raft log
	result := rsm.sm.DoOp(req)

	// Sending heartbeat to confirm leadership
	readId, ok := rsm.rf.ConfirmLeadership()
	if !ok {
		return rpc.ErrWrongLeader, nil
	}

	// Wait for majority to respond
	confirmed := rsm.rf.WaitForConfirmation(readId)
	if !confirmed {
		return rpc.ErrWrongLeader, nil
	}

	// Verify server is still leader in the same term
	termAfter, isLeader := rsm.rf.GetState()
	if !isLeader || termBefore != termAfter {
		return rpc.ErrWrongLeader, nil // Term changed, discard result
	}

	return rpc.OK, result
}

// SubmitReadBatched adds a read to the current batch and waits for batch confirmation.
// Amortizes confirmation cost across multiple reads for higher throughput.
func (rsm *RSM) SubmitReadBatched(req any) (rpc.Err, any) {
	// Check if we're the leader
	termBefore, isLeader := rsm.rf.GetState()
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	// Execute read locally
	result := rsm.sm.DoOp(req)

	// Create read request and add to batch
	readReq := &ReadRequest{
		req:    req,
		term:   termBefore,
		result: result,
		done:   make(chan rpc.Err, 1),
	}

	rsm.readBatchMu.Lock()
	rsm.readBatch = append(rsm.readBatch, readReq)
	rsm.readBatchMu.Unlock()

	// Wait for batch flusher to confirm this read
	err := <-readReq.done

	// Return result or error
	if err != rpc.OK {
		return err, nil
	}

	return rpc.OK, result
}

// Runs in background and periodically flushes the read batch
func (rsm *RSM) batchFlusher() {
	ticker := time.NewTicker(rsm.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rsm.flushReadBatch()

		case <-rsm.stopBatcher:
			return
		}
	}
}

// Confirms leadership for all pending reads in one round
func (rsm *RSM) flushReadBatch() {
	// Take current batch
	rsm.readBatchMu.Lock()
	if len(rsm.readBatch) == 0 {
		rsm.readBatchMu.Unlock()
		return
	}

	batch := rsm.readBatch
	rsm.readBatch = make([]*ReadRequest, 0)
	rsm.readBatchMu.Unlock()

	// Confirm leadership once for entire batch
	readId, ok := rsm.rf.ConfirmLeadership()
	if !ok {
		for _, req := range batch {
			req.done <- rpc.ErrWrongLeader
		}
		// Leader confirm failed for entire batch
		return
	}

	// Wait for confirmation
	confirmed := rsm.rf.WaitForConfirmation(readId)

	// Get current state
	termAfter, isLeader := rsm.rf.GetState()

	// Check each read's term and respond
	for _, req := range batch {
		if !confirmed || !isLeader || req.term != termAfter {
			req.done <- rpc.ErrWrongLeader
		} else {
			req.done <- rpc.OK
		}
	}
}
