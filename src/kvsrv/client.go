package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	server *labrpc.ClientEnd
	uniqueId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	reply := GetReply{}
	for !ck.server.Call("KVServer.Get", &args, &reply) {}
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	token := nrand()
	args := PutAppendArgs{Key: key, Value: value, Token: token}
	reply := PutAppendReply{}
	for !ck.server.Call("KVServer."+op, &args, &reply) {}
	callbackargs := PutAppendArgs{Token: token}
	callbackreply := PutAppendReply{}
	for !ck.server.Call("KVServer.PutAppendCallback", &callbackargs, &callbackreply) {}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
