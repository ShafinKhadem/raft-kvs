package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

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
	mu       sync.Mutex
	kv       map[string]string       // key -> value
	versions map[string]rpc.Tversion // key -> current version
}

type Op struct {
	OpType  string
	GetArgs rpc.GetArgs
	PutArgs rpc.PutArgs
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
		key := op.PutArgs.Key
		expectedVersion := op.PutArgs.Version
		currentVersion, exists := kv.versions[key]
		if !exists && expectedVersion == 0 {
			// New key, version starts at 0
			kv.kv[key] = op.PutArgs.Value
			kv.versions[key] = 1
			return rpc.PutReply{Err: rpc.OK}
		} else if exists && expectedVersion == currentVersion {
			kv.kv[key] = op.PutArgs.Value
			kv.versions[key] = currentVersion + 1
			return rpc.PutReply{Err: rpc.OK}
		} else {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
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
	op := Op{OpType: "Get", GetArgs: *args}
	rpcResult, opResult := kv.rsm.SubmitReadBatched(op)
	if rpcResult != rpc.OK {
		*reply = rpc.GetReply{Err: rpc.ErrWrongLeader}
		return
	}
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
		return
	}
	*reply = opResult.(rpc.PutReply)
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
		me:       me,
		kv:       make(map[string]string),
		versions: make(map[string]rpc.Tversion),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
