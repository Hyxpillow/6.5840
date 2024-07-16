package mr

import "os"
import "strconv"

type AllocTaskArgs struct {
}

type AllocTaskReply struct {
	TaskType int // 0:none 1:map 2:reduce
	TaskId int 
	Filename string // used by map
	NReduce int // used by map
}


type CallBackMapTaskArgs struct {
	TaskId int
}
type CallBackMapTaskReply struct {
}

type CallBackReduceTaskArgs struct {
	TaskId int
}
type CallBackReduceTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}