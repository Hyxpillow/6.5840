package kvsrv

// Put or Append
type PutAppendArgs struct {
	Token int64
	Key   string
	Value string
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
