package kvraft

import (
	"sync"
	"time"

	kvtest "raft/kvtest1"
	"raft/rpc"
	tester "raft/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for {
		ck.mu.Lock()
		servers := append([]string{}, ck.servers...)
		ck.mu.Unlock()
		for idx, srv := range servers {
			reply := rpc.GetReply{}
			ch := make(chan bool, 1)
			go func() {
				ch <- ck.clnt.Call(srv, "KVServer.Get", &args, &reply)
			}()
			select {
			case ok := <-ch:
				if ok {
					if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
						ck.moveServerToFront(servers, idx)
					}
					if reply.Err == rpc.OK {
						return reply.Value, reply.Version, rpc.OK
					} else if reply.Err == rpc.ErrNoKey {
						return "", 0, rpc.ErrNoKey
					}
					// If ErrWrongLeader or other errors, try next server
				}
			case <-time.After(10 * time.Second):
			}
			// If call failed (network error), try next server
		}
		// If all servers failed, loop again (keeps trying forever)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	firstAttempt := true
	for {
		ck.mu.Lock()
		servers := append([]string{}, ck.servers...)
		ck.mu.Unlock()
		for idx, srv := range servers {
			reply := rpc.PutReply{}
			ch := make(chan bool, 1)
			go func() {
				ch <- ck.clnt.Call(srv, "KVServer.Put", &args, &reply)
			}()
			select {
			case ok := <-ch:
				if ok {
					if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
						ck.moveServerToFront(servers, idx)
					}
					if reply.Err == rpc.OK {
						return rpc.OK
					} else if reply.Err == rpc.ErrVersion {
						if firstAttempt {
							return rpc.ErrVersion
						} else {
							return rpc.ErrMaybe
						}
					}
					// If ErrWrongLeader or other errors, try next server
				}
			case <-time.After(10 * time.Second):
				// Upon retrying, we don't want to begin trying with the same server
				ck.moveServerToBack(servers, idx)
				return rpc.ErrMaybe
			}
			// If call failed (network error), try next server
		}
		firstAttempt = false
		// If all servers failed, loop again
	}
}

func (ck *Clerk) moveServerToFront(servers []string, idx int) {
	if idx == 0 {
		return
	}
	ck.mu.Lock()
	defer ck.mu.Unlock()
	srv := servers[idx]
	ck.servers = append(servers[:idx], servers[idx+1:]...)
	ck.servers = append([]string{srv}, ck.servers...)
}

func (ck *Clerk) moveServerToBack(servers []string, idx int) {
	if idx == len(servers)-1 {
		return
	}
	ck.mu.Lock()
	defer ck.mu.Unlock()
	srv := servers[idx]
	ck.servers = append(servers[:idx], servers[idx+1:]...)
	ck.servers = append(ck.servers, srv)
}
