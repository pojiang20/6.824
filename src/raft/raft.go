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
	//当前任期
	CurrentTerm int
	//把票投给了谁，要么是投给了谁的candidateID要么是空
	VotedFor int
	//获得选票
	voteNum int
	//最新选举时间
	latestElectionTime time.Time
	//心跳间隙
	heartBeatIntervalMS time.Duration
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
	//reply false if term < currentTerm
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.VotedFor = -1
	}
	rf.latestElectionTime = time.Now()
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		//If votedFor is null or candidateID, grant vote.
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		DPrintf("[%d],role=%d term=%d\t vote to %d", rf.me, rf.State, rf.CurrentTerm, rf.VotedFor)
	}
	rf.mu.Unlock()
	return
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
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.VotedFor = -1
	}
	rf.latestElectionTime = time.Now()
	reply.Success = true
	reply.Term = rf.CurrentTerm
	DPrintf("[%d],role=%d term=%d\t receive append entries from %d", rf.me, rf.State, rf.CurrentTerm, args.LeaderId)
	rf.mu.Unlock()
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	args := RequestVoteArgs{
		Term:        rf.CurrentTerm,
		CandidateId: rf.me,
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
		DPrintf("[%d],role=%d term=%d\t become leader", rf.me, rf.State, rf.CurrentTerm)
		rf.State = Leader
		rf.VotedFor = -1
		go rf.sendHeartBeat()
	}
	return
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
			go rf._sendHeartBeat(i)
		}
		rf.mu.Unlock()
		time.Sleep(rf.heartBeatIntervalMS)
	}
}

func (rf *Raft) _sendHeartBeat(peerId int) {
	DPrintf("[%d],role=%d term=%d\t send heart beat to %d", rf.me, rf.State, rf.CurrentTerm, peerId)
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.CurrentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peerId, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.CurrentTerm < reply.Term {
		rf.State = Follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
	}
	rf.mu.Unlock()
	return
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
	rf.heartBeatIntervalMS = time.Duration(20) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
