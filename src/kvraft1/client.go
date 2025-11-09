package kvraft

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
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
		for _, srv := range ck.servers {
			reply := rpc.GetReply{}
			ok := ck.clnt.Call(srv, "KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == rpc.OK {
					return reply.Value, reply.Version, rpc.OK
				} else if reply.Err == rpc.ErrNoKey {
					return "", 0, rpc.ErrNoKey
				}
				// If ErrWrongLeader or other errors, try next server
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
		for _, srv := range ck.servers {
			reply := rpc.PutReply{}
			ok := ck.clnt.Call(srv, "KVServer.Put", &args, &reply)
			if ok {
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
			// If call failed (network error), try next server
		}
		firstAttempt = false
		// If all servers failed, loop again
	}
}
