package raft


import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.5840/labgob"
	"6.5840/labrpc"

	"fmt"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
    FOLLOWER  int = 0
    CANDIDATE int = 1
    LEADER    int = 2
)

type LogEntry struct {
	Command interface{}
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg

	electionTimer RaftTimer
	heartBeatTimer RaftTimer
	applyTimer RaftTimer

	state int
	currentTerm int    // 当前任期
	// votedFor int	// ？

	voteReplyCount int    // 选举时收到的票数

	logs []LogEntry    // 日志
	commitIndex int    // commitIndex之前的日志都已经commited
	lastApplied int    // lastApplied之前的日志都已经applied

	// leader日志专用
	nextIndex []int    // 需要发给每个主机的日志起始索引
	// matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term int    // 候选人任期
	CandidateId int    // 候选人ID
	LastLogIndex int    // 候选人最后的日志索引
	LastLogTerm int    // 候选人最后的日志任期
}

type RequestVoteReply struct {
	Term int	// 候选人任期 防止延迟消息重入
	VoteGranted bool    // 是否赞同
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	reply.Term = args.Term
	// 如果参选者任期低于等于自己，拒绝
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
	} else {
		// 如果参选者任期高于自己 不管是什么状态都要退回follower
		rf.state = FOLLOWER    
		rf.currentTerm = args.Term

		// 如果两个日志的任期号不同，任期号大的更新
		if args.LastLogTerm > rf.logs[len(rf.logs) - 1].Term {
			rf.electionTimer.reset()

			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.logs[len(rf.logs) - 1].Term && args.LastLogIndex >= len(rf.logs) - 1 {
			rf.electionTimer.reset()

			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	// 如果参选者的任期低于等于自己，日志比自己新（false）
	// 如果参选者的任期高于自己，日志比自己新 (follower true)
	// 如果参选者的任期高于自己，日志比自己旧 (follower false)
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		DPrint3A(rf.me, fmt.Sprintf("%v投票为%v", server,reply.VoteGranted))
	} else {
		DPrint3A(rf.me, fmt.Sprintf("%v超时", server))
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果当前已经是Leader了，就不需要看票型了，直接返回。防止延迟消息到来
	if rf.state == LEADER || reply.Term < rf.currentTerm {    
		return ok
	}
	// 统计投票
	if reply.VoteGranted {
		rf.voteReplyCount += 1
	}
	if rf.voteReplyCount > len(rf.peers) / 2 {
		rf.state = LEADER
		DPrint3A(rf.me, fmt.Sprintf("当选"))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.commitIndex + 1
		}
	}
	return ok
}


type AppendEntriesArgs struct {
	Term 			int
	LeaderId 		int

	PrevLogIndex	int
	PrevLogTerm 	int
	Entries				[]LogEntry

	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term	int
	Success	bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.reset()

	// reply.CommitIndex = rf.commitIndex  // TestBackup3B 加速一致性检查

	// 如果收到了旧任期的心跳(旧leader恢复正常)，返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// 如果收到了新任期的心跳，退回follower，更新本机任期
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER    
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm

	// 一致性检查
	if args.PrevLogIndex > len(rf.logs) - 1 || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Success = false
		if args.PrevLogIndex < len(rf.logs) {
			rf.logs = rf.logs[:args.PrevLogIndex]
		}
		return
	}
	reply.Success = true

	// 如果leader已提交的日志多于本机已提交的日志，就把本机min(LeaderCommit, idx of last log)之前的都给提交了
	if args.PrevLogIndex == len(rf.logs) - 1 && args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(len(rf.logs) - 1, args.LeaderCommit)
	}
	if len(args.Entries) == 0 {return}
	// 更新日志
	if args.PrevLogIndex == len(rf.logs) - 1 {
		rf.logs = append(rf.logs, args.Entries...)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrint3A(rf.me, fmt.Sprintf("%v无心跳反馈", server))
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果心跳返回更新的任期，说明当前leader已经过时了，退回follower
	if reply.Term > rf.currentTerm {    
		DPrint3A(rf.me, fmt.Sprintf("%v任期更高,回退follower", server))
		rf.state = FOLLOWER    // 不管是什么状态都要退回follower
		rf.currentTerm = reply.Term
		return ok
	}
	// 如果没有发送日志，直接返回
	if len(args.Entries) == 0 {return ok}
	// 如果发送了日志，且follower返回成功，更新leader对该follower的记录
	if reply.Success {
		rf.nextIndex[server] += len(args.Entries)
	} else {
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server] -= 1
		}
	}
	return ok
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}
	logEntry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, logEntry)

	return len(rf.logs) - 1, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) doElection() {
	rf.mu.Lock()
	// LEADER不会发起选举请求
	if rf.state == LEADER {
		rf.mu.Unlock()
		return
	}
	rf.state = CANDIDATE    // 转化为candidate
	rf.currentTerm += 1    // 任期提升
	rf.voteReplyCount = 1    //清空票数，默认自己给自己投票
	rf.mu.Unlock()
	DPrint3A(rf.me, "竞选")
	args := RequestVoteArgs{Term: rf.currentTerm, 
							CandidateId: rf.me,
							LastLogIndex: len(rf.logs) - 1,
							LastLogTerm: rf.logs[len(rf.logs) - 1].Term}
	replyList := make([]RequestVoteReply, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		} else {
			go rf.sendRequestVote(i, &args, &replyList[i])
		}
	}
}

func (rf *Raft) doHeartBeat() {
	rf.mu.Lock()
	// 非leader不会发起心跳
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	replyList := make([]AppendEntriesReply, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {continue}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex    // leader已提交的索引，让所有follower也提交该索引以下的日志

		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

		if len(rf.logs) > rf.nextIndex[i] {
			args.Entries = append(args.Entries, rf.logs[rf.nextIndex[i]:]...)
		}
		go rf.sendAppendEntries(i, &args, &replyList[i])
	}
	// 如果当前commitIndex 已经被大部分follower收到了，就+1
	replyCount := 1
	for peer := range rf.peers {
		if peer == rf.me {continue}
		if rf.nextIndex[peer] > rf.commitIndex + 1 {
			replyCount += 1
		} 
	}
	if replyCount > len(rf.peers) / 2 {
		rf.commitIndex += 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) doApply() {
	rf.mu.Lock()
	DPrint3A(rf.me, fmt.Sprintf("提交log%v~%v", rf.lastApplied, rf.commitIndex))

	tmp := []int{}
	for i := range rf.logs {
		tmp = append(tmp, rf.logs[i].Term)
	}
	DPrint3B(rf.me, fmt.Sprintf("%v%v commitIdx:%v term:%v",rf.state==LEADER,tmp,rf.commitIndex, rf.currentTerm))

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		DPrint3A(rf.me, fmt.Sprintf("提交log%v", rf.lastApplied))
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	go rf.electionTimer.begin()
	go rf.heartBeatTimer.begin()
	go rf.applyTimer.begin()

	for rf.killed() == false {
		select {
		case <-rf.electionTimer.getChan():
			rf.doElection()
		case <-rf.heartBeatTimer.getChan():
			rf.doHeartBeat()
		case <-rf.applyTimer.getChan():
			rf.doApply()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化
	rf.applyCh = applyCh
	rf.state = FOLLOWER	// 初始化为follower

	rf.electionTimer.init(500, 200)
	rf.heartBeatTimer.init(100, 1)
	rf.applyTimer.init(500, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = []LogEntry{LogEntry{}}
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 1
	}
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}


func min(a, b int) int {
	if a < b {return a}
	return b
}