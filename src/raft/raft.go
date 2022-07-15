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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

const (
	Leader     State = 1
	Follower   State = 2
	Candidater State = 3
)

const Null int = -1
const HeartsBeatsTimeOut int = 125
const ElectionTimeOut int = 1000

func StableHeartbeatTimeOut() time.Duration {
	return time.Duration(HeartsBeatsTimeOut) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeOut+rand.Intn(ElectionTimeOut)) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	state          State
	currentTerm    int
	votedFor       int
	lastTicker     *time.Timer
	heartbeatTimer *time.Timer
	log            []Entry
	replicatorCond []*sync.Cond
	commitIndex    int
	lastApplied    int
	matchIndex     []int
	nextIndex      []int
	applyCond      *sync.Cond
	applyCh        chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf("server %v persist success", rf.me)
}

//
// restore previously persisted state.
//
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
	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		panic(err)
	} else {
		rf.currentTerm = currentTerm
	}
	var votedFor int
	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	} else {
		rf.votedFor = votedFor
	}
	var log []Entry
	if err := d.Decode(&log); err != nil {
		panic(err)
	} else {
		rf.log = log
	}
	// DPrintf("server %v readPersist success", rf.me)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// Follower
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.log = make([]Entry, 1)
	} else {
		rf.log = rf.log[lastIncludedIndex-rf.getFirstLog().Index:]
		rf.log[0].Command = nil
	}

	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.persist()
	// DPrintf("(2D) server %v snapshot success index = %v", rf.me, lastIncludedIndex)
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// Leader
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index-rf.getFirstLog().Index <= 0 {
		return
	}

	rf.log = rf.log[index-rf.getFirstLog().Index:]
	rf.log[0].Command = nil
	// DPrintf("(2D) server %v snapshot success index = %v", rf.me, index)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, snapshot)
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	args := InstallSnapshotArgs{}
	args.Data = rf.persister.snapshot
	args.LastIncludedIndex = rf.getFirstLog().Index
	args.LastIncludedTerm = rf.getFirstLog().Term
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.Done = true
	args.Offset = 0
	return &args
}

func (rf *Raft) HandleInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		} else {
			rf.matchIndex[peer] = max(args.LastIncludedIndex, rf.commitIndex)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.state = Follower
	rf.lastTicker.Reset(RandomizedElectionTimeout())

	if args.LastIncludedIndex <= rf.commitIndex {
		// DPrintf("server %v find don't install Snapshot with lastIndex : %v and commitIndex : %v", rf.me, args.LastIncludedIndex, rf.commitIndex)
		return
	}
	// DPrintf("server %v  install Snapshot", rf.me)
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
			Snapshot:      args.Data,
		}
	}()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 	1. Reply false if term < currentTerm (§5.1)
	// 	2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != Null &&
			rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = Null
	}
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	rf.lastTicker.Reset(RandomizedElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	return
}

func (rf *Raft) isLogUpToDate(LastLogIndex, LastLogTerm int) bool {
	lastLog := rf.getLastLog()
	return LastLogTerm > lastLog.Term || (LastLogTerm == lastLog.Term && LastLogIndex >= lastLog.Index)
}

//
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
// look at the comments in 6.824/labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	// get log entry
	entry := rf.addEntry(command)
	// DPrintf("add entry {%v}", entry)
	// broadcast to other server
	rf.BroadCast(false)
	return entry.Index, entry.Term, isLeader
}

func (rf *Raft) addEntry(command interface{}) Entry {
	entry := Entry{}
	entry.Command = command
	entry.Term = rf.currentTerm
	entry.Index = rf.getLastLog().Index + 1
	rf.log = append(rf.log, entry)
	DPrintf("server %v's log %v", rf.me, rf.log)
	rf.persist()
	return entry
}

func (rf *Raft) getLastLog() Entry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getFirstLog() Entry {
	return rf.log[0]
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// AppendEntries RPC struct

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  //currentTerm
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Receiver implementation 1
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 发现自己不是最新的
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = Follower
	rf.lastTicker.Reset(RandomizedElectionTimeout())
	// instilled snapshot
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term = 0
		reply.Success = false
		return
	}
	// Receiver implementation 2
	if !rf.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		lastIndex := rf.getLastLog().Index
		if lastIndex < args.PrevLogIndex {
			reply.ConflictIndex = lastIndex + 1
			reply.ConflictTerm = -1
		} else {
			//If an existing entry conflicts with a new one (same index but different terms)
			//delete the existing entry and all that follow it
			firstIndex := rf.getFirstLog().Index
			reply.ConflictTerm = rf.log[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.log[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		// DPrintf("sever %v loading...", rf.me)
		return
	}
	for index, entry := range args.Entries {
		if !rf.matchLog(entry.Index, entry.Term) {
			// DPrintf("entries %v\n", args.Entries[index:])
			// DPrintf("server %v before add entry,Entry : %v", rf.me, rf.log)
			rf.log = append(rf.log[:entry.Index-rf.getFirstLog().Index], args.Entries[index:]...)
			DPrintf("server %v after add entry,Entry : %v\n", rf.me, rf.log)
			break
		}
	}

	if args.LeaderCommit >= rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		// DPrintf("server %v LeaderCommit vs server %v commitIndex : [%v : %v]", args.LeaderId, rf.me, args.LeaderCommit, rf.commitIndex)
		rf.applyCond.Signal()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) matchLog(prevLogIndex, prevLogTerm int) bool {
	firstLog := rf.getFirstLog()
	return prevLogIndex-firstLog.Index < len(rf.log) && rf.log[prevLogIndex-firstLog.Index].Term == prevLogTerm
}

func (rf *Raft) replicateOneRound(peer int) {
	// TODO:send appendEntries
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	// TODO:select snapshot or append
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// do something about install snapshot(2D)
		args := rf.genInstallSnapshotArgs()
		rf.mu.Unlock()
		reply := new(InstallSnapshotReply)
		// DPrintf("send installSnapshot to sever %v", peer)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			rf.HandleInstallSnapshot(peer, args, reply)
			rf.mu.Unlock()
		}
	} else {
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.Unlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntry(peer, args, reply) {
			rf.mu.Lock()
			// process reply
			rf.HandleAppendEntriesReply(peer, args, reply)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.log[prevLogIndex-rf.getFirstLog().Index].Term
	entries := make([]Entry, 0)
	entries = append(entries, rf.log[prevLogIndex-rf.getFirstLog().Index+1:]...)
	args.Entries = entries
	return &args
}

func (rf *Raft) UpdateCommitIndex() {
	vote := 1
	e := len(rf.peers) / 2
	for peer := range rf.matchIndex {
		if peer == rf.me {
			continue
		}
		if rf.matchIndex[peer] == rf.getLastLog().Index {
			vote++
		}
		if vote > e {
			rf.commitIndex = rf.getLastLog().Index
			rf.applyCond.Signal()
			return
		}
	}
}

func (rf *Raft) HandleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Success {
			// DPrintf("server %v append entries success\n", peer)
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.UpdateCommitIndex()
		} else {
			// DPrintf("server %v append entries fail\n", peer)
			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = args.Term
				rf.votedFor = Null
				rf.persist()
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstLog := rf.getFirstLog()
					for i := args.PrevLogIndex; i >= firstLog.Index; i-- {
						if rf.log[i-firstLog.Index].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
}

func (rf *Raft) BroadCast(isHeartsBeats bool) {
	if isHeartsBeats {
		rf.HeartBeats()
	} else {
		rf.BroadCastAppendEntries()
	}
}

func (rf *Raft) HeartBeats() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			// DPrintf("HeartBeats to %v", peer)
			go rf.replicateOneRound(peer)
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	// DPrintf("server %v's server %v begin replicator\n", rf.me, peer)
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// DPrintf("server %v need Replicating\n", peer)
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) BroadCastAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			// DPrintf("BroadCastAppendEntries to server %v", peer)
			rf.replicatorCond[peer].Signal()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				// DPrintf("server %v send a heartBeat", rf.me)
				rf.BroadCast(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeOut())
			}
			rf.mu.Unlock()
		case <-rf.lastTicker.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.lastTicker.Reset(RandomizedElectionTimeout())
			} else {
				rf.state = Candidater
				rf.currentTerm++
				// DPrintf("server %v start election\n", rf.me)
				rf.StartElection()
				rf.lastTicker.Reset(RandomizedElectionTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("server %v's lastApplied : %v commitIndex : %v", rf.me, rf.lastApplied, rf.commitIndex)
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		// DPrintf("server %v's lastApplied : %v commitIndex : %v", rf.me, rf.lastApplied, rf.commitIndex)
		firstIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		begin := 1
		if lastApplied-firstIndex+1 > 0 {
			begin = lastApplied - firstIndex + 1
		}
		end := commitIndex - firstIndex + 1
		entries := make([]Entry, end-begin)
		copy(entries, rf.log[begin:end])
		// DPrintf("applier entries: %v", entries)
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartElection() {
	args := rf.genRequestVoteRequest()
	// DPrintf("{Node %v} starts election with RequestVoteRequest %v\n", rf.me, args)
	voted := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v\n", rf.me, reply, peer, args, rf.currentTerm)
				if rf.currentTerm == args.Term && rf.state == Candidater {
					if reply.VoteGranted {
						voted++
						if voted > len(rf.peers)/2 {
							// DPrintf("{Node %v} receives majority votes in term %v\n", rf.me, rf.currentTerm)
							rf.state = Leader
							rf.BroadCast(true)
							rf.heartbeatTimer.Reset(StableHeartbeatTimeOut())
						}
					} else if reply.Term > rf.currentTerm {
						// DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = Null
						rf.persist()
					}
				}
			}
		}(peer)
	}

}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.getLastLog().Index
	args.LastLogTerm = rf.getLastLog().Term
	return &args
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.dead = 0
	rf.votedFor = Null
	rf.lastTicker = time.NewTimer(RandomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeOut())
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Entry, 1)
	rf.replicatorCond = make([]*sync.Cond, len(rf.peers))
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)
	LastLog := rf.getLastLog()
	for peer := range rf.peers {
		if rf.me == peer {
			continue
		}
		rf.matchIndex[peer] = 0
		rf.nextIndex[peer] = LastLog.Index + 1
		rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(peer)
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
