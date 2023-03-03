package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type raftState int

const (
	Follower raftState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//状态
	State raftState
	//当前任期【持久化】
	CurrentTerm int
	//把票投给了谁，要么是投给了谁的candidateID要么是空【持久化】
	VotedFor int
	//获得选票
	voteNum int
	//最新选举时间
	latestElectionTime time.Time
	//心跳间隙
	heartBeatIntervalMS time.Duration
	//日志,序列从1开始【持久化】
	log         []RaftLog
	commitIndex int
	lastApplied int
	//leader才维护
	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg
}

type RaftLog struct {
	Cmd  interface{}
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.State == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	rf.persister.Save(rf.serializeState(), nil)
	DPrintf("[%d],role=%d term=%d\t persist %v", rf.me, rf.State, rf.CurrentTerm, rf.log)
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var votedFor int
	var log []RaftLog
	if d.Decode(&curTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("readPersist error")
		return
	} else {
		rf.VotedFor = votedFor
		rf.CurrentTerm = curTerm
		rf.log = log
		DPrintf("[%d],role=%d term=%d\t readPersist %v", rf.me, rf.State, rf.CurrentTerm, rf.log)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//当前时间
	Term int
	//发起投票的候选人ID
	CandidateId int
	//最后一个日志的位置和任期，用于作为投票的考虑因素
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	//投票人的任期
	Term int
	//true表示把票投给申请者
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//reply false if term < currentTerm
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.VotedFor = -1
		rf.persist()
	}
	if rf.State != Follower {
		return
	}
	rf.latestElectionTime = time.Now()
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		//If votedFor is null or candidateID, grant vote.
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
		DPrintf("[%d],role=%d term=%d\t vote to %d", rf.me, rf.State, rf.CurrentTerm, rf.VotedFor)
	}
	return
}

// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
// If the logs end with the same term, then whichever log is longer is more up-to-date.
// 1. 任期越大越新 2. 相同任期越长越新
func (rf *Raft) isUpToDate(lastLogIndex, lastLogTerm int) bool {
	//该节点的lastLogIndex=len(rf.log)-1
	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIndex].Term
	DPrintf("[%d],role=%d term=%d\t myLastLogIndex:%d,myLastLogTerm:%d,lastLogIndex:%d,lastLogTerm:%d", rf.me, rf.State, rf.CurrentTerm, myLastLogIndex, myLastLogTerm, lastLogIndex, lastLogTerm)
	if lastLogTerm > myLastLogTerm {
		return true
	}
	if lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex {
		return true
	}
	return false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	//prevLogIndex=nextIndex[i]-1
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []RaftLog
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//优化
	Xterm  int //term in the conflicting entry (if any)
	XIndex int //index of first entry with that term (if any)
	XLen   int //log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Reply false if term < currentTerm
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	//if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	reply.Success = false
	//	reply.Term = rf.CurrentTerm
	//	return
	//}
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.XIndex = -1
		reply.Xterm = -1
		reply.XLen = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		//找到任期内的起始坐标
		curTerm, i := rf.log[args.PrevLogIndex].Term, args.PrevLogIndex
		//log从1开始
		for i > 1 && curTerm == rf.log[i-1].Term {
			i--
		}
		reply.XLen = len(rf.log)
		reply.XIndex = i
		reply.Xterm = curTerm
		return
	}
	//rf.latestElectionTime = time.Now()
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.VotedFor = -1
	}
	rf.latestElectionTime = time.Now()
	reply.Success = true
	reply.Term = rf.CurrentTerm
	//追加内容
	DPrintf("[%d],role=%d term=%d\t before append entries %v,preLogIndex:%d,args entries: %v,from %d", rf.me, rf.State, rf.CurrentTerm, rf.log, args.PrevLogIndex, args.Entries, args.LeaderId)
	//if len(args.Entries) > 0 {
	//	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	//}
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		if idx < len(rf.log) && entry.Term != rf.log[idx].Term {
			rf.log = append(rf.log[:idx], args.Entries[i:]...)
		}
		if idx >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()
	//更新commit
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		//If commitIndex > lastApplied: increment lastApplied,
		//apply log[lastApplied] to state machine
		if rf.commitIndex > rf.lastApplied {
			rf.applyCommitted()
		}
	}
	DPrintf("[%d],role=%d term=%d\t receive append entries from %d", rf.me, rf.State, rf.CurrentTerm, args.LeaderId)
}
func (rf *Raft) _sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) applyCommitted() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		DPrintf("[%d],role=%d term=%d\t commitIndex=%d", rf.me, rf.State, rf.CurrentTerm, rf.commitIndex)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log[rf.lastApplied].Cmd,
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("[%d],role=%d term=%d\t start cmd[%+v]", rf.me, rf.State, rf.CurrentTerm, command)
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.State == Leader
	if isLeader {
		DPrintf("[%d],leader term=%d\t replicate cmd[%v]", rf.me, rf.CurrentTerm, command)
		term = rf.CurrentTerm
		index = len(rf.log)
		rf.log = append(rf.log, RaftLog{Term: term, Cmd: command})
		rf.persist()
		//then issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
		go rf.replicateEntry()
	}
	return index, term, isLeader
}

func (rf *Raft) replicateEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex := len(rf.log) - 1
	for i := range rf.peers {
		if i == rf.me {
			//更新leader自己的nextIndex和match
			rf.nextIndex[rf.me] = lastLogIndex + 1
			rf.matchIndex[rf.me] = lastLogIndex
			continue
		}
		//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		if lastLogIndex >= rf.nextIndex[i] {
			go rf.sendAppendEntries(false, i)
		}
	}
}

func (rf *Raft) lastLog() (ret RaftLog) {
	rf.mu.Lock()
	ret = rf.log[len(rf.log)-1]
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntries(isHeartBeat bool, peerId int) {
	rf.mu.Lock()
	if rf.State != Leader {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[peerId]
	if nextIndex <= 0 {
		nextIndex = 1
	}
	if nextIndex > len(rf.log) {
		nextIndex = len(rf.log)
	}
	prevLogIndex := nextIndex - 1
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]RaftLog, len(rf.log)-nextIndex),
	}
	copy(args.Entries, rf.log[nextIndex:])
	reply := AppendEntriesReply{}
	if isHeartBeat {
		DPrintf("[%d],role=%d term=%d\t send heart beat to %d\t prevlogIndex:%d,prevlogTerm:%d,entries:%v", rf.me, rf.State, rf.CurrentTerm, peerId, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	} else {
		DPrintf("[%d],role=%d term=%d\t send append entries to %d\t prevlogIndex:%d,prevlogTerm:%d,commitIndex:%d,entries:%v", rf.me, rf.State, rf.CurrentTerm, peerId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	}
	rf.mu.Unlock()

	ok := rf._sendAppendEntries(peerId, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm < reply.Term {
		DPrintf("[%d],role=%d term=%d\t replyTerm=%d become follower", rf.me, rf.State, rf.CurrentTerm, reply.Term)
		rf.State = Follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		return
	}
	if reply.Success {
		leaderLastLogIndex := args.PrevLogIndex + len(args.Entries)
		//update nextIndex and matchIndex for follower
		rf.nextIndex[peerId] = leaderLastLogIndex + 1
		rf.matchIndex[peerId] = leaderLastLogIndex
		go rf.checkCommitIndexMajority()
	} else if rf.nextIndex[peerId] > 1 {
		//rf.nextIndex[peerId]--
		//优化回退策略
		//Case 1: leader doesn't have XTerm:
		//nextIndex = XIndex
		//Case 2: leader has XTerm:
		//nextIndex = leader's last entry for XTerm
		//Case 3: follower's log is too short:
		//nextIndex = XLen
		if reply.XIndex == -1 && reply.Xterm == -1 {
			rf.nextIndex[peerId] = reply.XLen
		} else {
			//判断是否存在Xterm
			if exist, idx := findXTerm(rf.log, reply.Xterm); exist {
				rf.nextIndex[peerId] = idx
			} else {
				//不存在表示这个任期内所有操作都将被覆盖
				rf.nextIndex[peerId] = reply.XIndex
			}
		}
		go rf.sendAppendEntries(false, peerId)
	}
	return
}

// 找到大于等于Xterm的最后一个坐标
func findXTerm(log []RaftLog, Xterm int) (bool, int) {
	l, r, target := 1, len(log)-1, Xterm+1
	for l < r {
		mid := (l + r) / 2
		if log[mid].Term >= target {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if log[l].Term == Xterm {
		return true, l
	} else if l == 1 {
		return false, l
	} else {
		return false, l - 1
	}
}

func (rf *Raft) checkCommitIndexMajority() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != Leader {
		return
	}
	DPrintf("[%d],role=%d term=%d\t match[%v]", rf.me, rf.State, rf.CurrentTerm, rf.matchIndex)
	for tryCommitIndex := rf.commitIndex + 1; tryCommitIndex < len(rf.log); tryCommitIndex++ {
		//解决Figure8问题
		if rf.log[tryCommitIndex].Term != rf.CurrentTerm {
			continue
		}
		cnt := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= tryCommitIndex {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			for nextCommitIndex := rf.commitIndex + 1; nextCommitIndex <= tryCommitIndex; nextCommitIndex++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[nextCommitIndex].Cmd,
					CommandIndex: nextCommitIndex,
				}
			}
			DPrintf("[%d],role=%d term=%d\t commitIndex=%d tryCommitIndex=%d", rf.me, rf.State, rf.CurrentTerm, rf.commitIndex, tryCommitIndex)
			rf.commitIndex = tryCommitIndex
		} else {
			break
		}
	}
	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		timeBeforeSleep := time.Now()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.State == Leader {

		} else {
			//如果最新的选举时间比睡眠前的时间都要新（即睡眠这个阶段没有任何时间刷新），则需要发起选举
			if rf.latestElectionTime.Before(timeBeforeSleep) {
				go rf.StartElection()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.voteNum = 1
	rf.CurrentTerm += 1
	rf.State = Candidate
	rf.VotedFor = rf.me
	rf.latestElectionTime = time.Now()
	rf.persist()
	DPrintf("[%d],role=%d term=%d\t start election", rf.me, rf.State, rf.CurrentTerm)
	//给所有其他节点发起请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf._sendRequest(rf.CurrentTerm, i)
	}
}

func (rf *Raft) _sendRequest(newTerm int, peerId int) {
	rf.mu.Lock()
	reply := RequestVoteReply{}
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	rf.mu.Unlock()
	ok := rf.sendRequestVote(peerId, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm < reply.Term {
		rf.State = Follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		return
	}
	//角色变化
	if rf.State != Candidate {
		return
	}
	//任期变化
	if rf.CurrentTerm != newTerm || rf.CurrentTerm > reply.Term {
		return
	}
	if reply.VoteGranted {
		rf.voteNum++
	}
	if rf.voteNum > len(rf.peers)/2 {
		rf.State = Leader
		DPrintf("[%d],role=%d term=%d\t become leader\t log len %d", rf.me, rf.State, rf.CurrentTerm, len(rf.log))
		rf.VotedFor = -1
		rf.persist()
		rf.reInitNextAndMatchIndex(len(rf.log) - 1)
		go rf.sendHeartBeat()
	}
	return
}

func (rf *Raft) reInitNextAndMatchIndex(lastLogEntryIndex int) {
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogEntryIndex + 1
	}
	rf.matchIndex[rf.me] = lastLogEntryIndex
}

func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.State != Leader {
			rf.mu.Unlock()
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntries(true, i)
		}
		rf.mu.Unlock()
		time.Sleep(rf.heartBeatIntervalMS)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = -1
	rf.State = Follower
	rf.CurrentTerm = 0
	rf.latestElectionTime = time.Now()
	rf.heartBeatIntervalMS = time.Duration(50) * time.Millisecond
	//0位占用空log
	rf.log = []RaftLog{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
