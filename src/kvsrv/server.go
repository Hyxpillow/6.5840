package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	kvMap map[string]string
	tokenMap map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvMap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value := args.Value
	token := args.Token
	if v, ok := kv.tokenMap[token]; ok {
		reply.Value = v
		return
	}
	reply.Value = value
	kv.tokenMap[token] = value
	kv.kvMap[key] = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value := args.Value
	token := args.Token

	if v, ok := kv.tokenMap[token]; ok {
		reply.Value = v
		return
	}
	oldValue := kv.kvMap[key]
	reply.Value = oldValue
	kv.tokenMap[token] = oldValue
	kv.kvMap[key] += value
}

func (kv *KVServer) PutAppendCallback(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.tokenMap, args.Token)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)
	kv.tokenMap = make(map[int64]string)
	return kv
}
