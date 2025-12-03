package kvraft

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"raft/kvraft1/rsm"
	"raft/labgob"
	"raft/labrpc"
	"raft/rpc"
	tester "raft/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu          sync.Mutex
	kv          map[string]string       // key -> value
	versions    map[string]rpc.Tversion // key -> current version
	syncVersion rpc.Tversion
	syncCh      chan bool

	getQueue    chan Op
	getQueueLen int32

	opChs   map[int64]chan any
	opChsMu sync.Mutex
}

type Op struct {
	OpType  string
	GetArgs rpc.GetArgs
	PutArgs rpc.PutArgs
	Id      int64
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	op := req.(Op)
	switch op.OpType {
	case "Get":
		key := op.GetArgs.Key
		if val, ok := kv.kv[key]; ok {
			return rpc.GetReply{Value: val, Version: kv.versions[key], Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
	case "Put":
		// fmt.Println("server", kv.me, "processing Put for key", op.PutArgs.Key, "with value", op.PutArgs.Value, "with version", op.PutArgs.Version)
		key := op.PutArgs.Key
		var opResult rpc.PutReply
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if key == "sync" {
			// Dummy Put to sync Get queue
			opResult = rpc.PutReply{Err: rpc.OK}

			// Now process the Get queue up to the length recorded before this Put
			var queuelen, syncServer int
			fmt.Sscanf(op.PutArgs.Value, "%d %d", &syncServer, &queuelen)
			if syncServer != kv.me {
				// Not for us
				queuelen = 0
			} else {
				expectedVersion := op.PutArgs.Version
				currentVersion := kv.syncVersion
				if expectedVersion == currentVersion {
					kv.syncVersion = rpc.Tversion(rand.Uint64())
					// fmt.Println("server", kv.me, "processing get queue of length", queuelen, "with sync version", kv.syncVersion)
					atomic.AddInt32(&kv.getQueueLen, -int32(queuelen))
					for i := 0; i < queuelen; i++ {
						getOp := <-kv.getQueue
						kv.opChsMu.Lock()
						getCh, ok := kv.opChs[getOp.Id]
						kv.opChsMu.Unlock()
						if !ok {
							panic("getCh not found")
						}
						getResult := kv.DoOp(getOp)
						// fmt.Println("server", kv.me, "processed get for key", getOp.GetArgs.Key, "returning", getResult.(rpc.GetReply))
						getCh <- getResult
					}
				} else {
					// Version mismatch, do not process
					// fmt.Println("sync version mismatch:", expectedVersion, currentVersion)
				}
			}
		} else {
			expectedVersion := op.PutArgs.Version
			currentVersion, exists := kv.versions[key]
			if !exists && expectedVersion == 0 {
				// New key, version starts at 0
				kv.kv[key] = op.PutArgs.Value
				kv.versions[key] = 1
				opResult = rpc.PutReply{Err: rpc.OK}
			} else if exists && expectedVersion == currentVersion {
				kv.kv[key] = op.PutArgs.Value
				kv.versions[key] = currentVersion + 1
				opResult = rpc.PutReply{Err: rpc.OK}
			} else {
				opResult = rpc.PutReply{Err: rpc.ErrVersion}
			}
		}
		return opResult

	default:
		// Invalid operation
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kv)
	e.Encode(kv.versions)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var versionsMap map[string]rpc.Tversion
	if d.Decode(&kvMap) != nil || d.Decode(&versionsMap) != nil {
		// Handle decode error, but for simplicity, panic or log
		panic("failed to decode snapshot")
	}
	kv.kv = kvMap
	kv.versions = versionsMap
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	id := rand.Int64()
	ch := make(chan any)
	kv.opChsMu.Lock()
	kv.opChs[id] = ch
	kv.opChsMu.Unlock()
	defer func() {
		kv.opChsMu.Lock()
		delete(kv.opChs, id)
		kv.opChsMu.Unlock()
	}()
	atomic.AddInt32(&kv.getQueueLen, 1)
	op := Op{OpType: "Get", GetArgs: *args, Id: id}
	kv.getQueue <- op
	opResult := <-ch
	*reply = opResult.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	op := Op{OpType: "Put", PutArgs: *args}
	rpcResult, opResult := kv.rsm.Submit(op)
	if rpcResult != rpc.OK {
		*reply = rpc.PutReply{Err: rpc.ErrWrongLeader}
	} else {
		*reply = opResult.(rpc.PutReply)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:          me,
		kv:          make(map[string]string),
		versions:    make(map[string]rpc.Tversion),
		getQueue:    make(chan Op, 10000),
		opChs:       make(map[int64]chan any),
		syncVersion: rpc.Tversion(rand.Uint64()),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.

	// Start a goroutine to process Get requests from getQueue by sending a dummy Put to "sync"
	go func() {
		// Loop indefinitely, executing the desired action on each tick
		for !kv.killed() {
			// This may be the leader, and there may be Get requests waiting in other replicas
			queuelen := int(atomic.LoadInt32(&kv.getQueueLen))
			if queuelen > 0 {
				// fmt.Println("syncing get queue of length", queuelen, "at server", me, "with sync version", kv.syncVersion)
				version := kv.syncVersion
				putargs := rpc.PutArgs{Key: "sync", Value: fmt.Sprintf("%d %d", me, queuelen), Version: version}
				reply := rpc.PutReply{}
				leaderId := kv.rsm.Raft().LeaderId()
				if leaderId == me {
					// We are the leader, no need to send RPC
					kv.Put(&putargs, &reply)
				} else {
					// Send RPC to leader
					servers[leaderId].Call("KVServer.Put", &putargs, &reply)
				}
				if reply.Err == rpc.OK {
					for kv.syncVersion == version {
						time.Sleep(1 * time.Millisecond)
					}
				} else {
					kv.mu.Lock()
					kv.syncVersion = rpc.Tversion(rand.Uint64())
					kv.mu.Unlock()
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	return []tester.IService{kv, kv.rsm.Raft()}
}
