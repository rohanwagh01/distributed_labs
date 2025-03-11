package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	kvmap map[string]*Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvmap = make(map[string]*Value)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock() //obtain lock for the kvserver
	currValue, exists := kv.kvmap[args.Key]
	if exists {
		reply.Value = currValue.Value
		reply.Version = currValue.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock() //obtain lock for the kvserver
	currValue, exists := kv.kvmap[args.Key]
	if exists { // attempting to overwrite existing key in server
		if args.Version == currValue.Version { // matching version
			kv.kvmap[args.Key] = &Value{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else { //mismatches version
			reply.Err = rpc.ErrVersion
		}
	} else { // attempting to create new key
		if args.Version == 0 { //asked for new key and the new key version is 0
			kv.kvmap[args.Key] = &Value{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey //passed in >0 but key does not exist
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
