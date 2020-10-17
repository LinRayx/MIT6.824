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
	"bytes"
	"log"
	"math/rand"
	"src/labgob"
	"src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

const (
	FOLLOWER = iota
	LEADER
	CANDIDATE
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid 	bool
	Command      	interface{}
	Snapshot		[]byte
	CommandIndex 	int
}

type LogEntry struct {
	Cmd  			interface{}
	Term 			int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

// snapshot
type SnapShot struct {
	Kv          map[string]string
	MaxSeriesID map[int64]int
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
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
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionReset chan int
	// perisistent
	currentTerm int
	voteFor     int
	log         []LogEntry

	// vote
	state 		int8

	// log
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int

	// snapshot
	lastIncludeIndex  int
	lastIncludeTerm   int
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
	//DPrintf("rf.me: [%d] Start\n", rf.me)
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader == false {
		return index, term, isLeader
	}
	rf.mu.Lock()
	rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
	rf.persist()
	//index = len(rf.log)-1
	index = rf.getLastLogIndex()
	DPrintf("rf.me: [%d] Start index: %d term: %d isLeader:%v command: %v log: %v\n", rf.me, index, term, isLeader, command, rf.log)
	rf.mu.Unlock()
	go rf.doConsensus()
	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) autoSendApplyMsgs() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			//log.Printf("rf.me:[%d] autoSendApplyMsgs lastApplied: %d commitIndex: %d index: %d log: %v", rf.me, rf.lastApplied, rf.commitIndex, rf.getLogIndex(rf.lastApplied), rf.log)
			cmd := rf.log[rf.getLogIndex(rf.lastApplied)].Cmd

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg

		}
		rf.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//DPrintf("rf.me: [%d] persist currentTerm: %d voteFor: %d log: %v ", rf.me, rf.currentTerm, rf.voteFor, rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.mu.Lock()
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.voteFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.lastIncludeIndex) != nil ||
		d.Decode(&rf.lastIncludeTerm) != nil {
		log.Fatalf("rf.me: [%d] readPersist decode error", rf.me)
	}
	DPrintf("rf.me: [%d] currentTerm: %d rf.voteFor: %d lastIncludeIndex: %d lastIncludeTerm: %d rf.log: %v", rf.me, rf.currentTerm, rf.voteFor, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.log)

	rf.mu.Unlock()
}

func (rf *Raft) transToLeader() {
	DPrintf("rf.me: [%d] transToLeader", rf.me)
	for i, _ := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLogLen()
	}
	rf.state = LEADER
	rf.persist()
}

func (rf *Raft) sendHeartBeats() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)
		rf.doConsensus()
	}
}

func (rf *Raft) doHeartBeat() {

}

func (rf *Raft) doAppendEntries(index int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	if index == rf.me {
		rf.nextIndex[index] = rf.getLogLen()
		rf.matchIndex[index] = rf.nextIndex[index] - 1
		rf.mu.Unlock()
		return
	}

	DPrintf("rf.me: [%d] sendHeartBeats index: %d _nextIndex: %d rf.lastIncludeIndex: %d", rf.me, index, rf.nextIndex[index], rf.lastIncludeIndex)
	if rf.nextIndex[index] > rf.lastIncludeIndex {
		// send AE
		// 切片是引用赋值
		_nextIndex := rf.getLogIndex(rf.nextIndex[index])
		tmpLog := rf.log[_nextIndex:]
		appendEntries := make([]LogEntry, len(tmpLog))

		copy(appendEntries, tmpLog)
		prevIndex := rf.nextIndex[index] - 1
		var preLogTerm int
		if prevIndex == rf.lastIncludeIndex {
			preLogTerm = rf.lastIncludeTerm
		} else {
			preLogTerm = rf.getLogTerm(prevIndex)
		}
		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			Entries:      appendEntries,
			PreLogIndex:  prevIndex,
			PreLogTerm:   preLogTerm,
		}
		rep := AppendEntriesReply{}
		//DPrintf("rf.me: [%d] I am Leader sendAE to %d PreLogIndex: %d PreLogTerm: %d", rf.me, index, req.PreLogIndex, req.PreLogTerm)
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(index, &req, &rep)
		rf.mu.Lock() // 2 lock
		if ok && rf.currentTerm == req.Term {
			if rep.Success == true {
				//if req.PreLogIndex + len(req.Entries) < rf.matchIndex[index] {
				//	log.Fatal("req.PreLogIndex + len(req.Entries) < rf.matchIndex[index]")
				//}
				rf.matchIndex[index] = req.PreLogIndex + len(req.Entries)
				rf.nextIndex[index] = rf.matchIndex[index] + 1
				DPrintf("rf.me: [%d] currentTerm: %d tongbu success rf.matchIndex[%d]: %d rf.nextIndex[%d]: %d rf.lastIncludeIndex: %d" , rf.me, rf.currentTerm, index, rf.matchIndex[index], index, rf.nextIndex[index], rf.lastIncludeIndex)
				rf.mu.Unlock() // 2 unlock
				return
			} else {
				// 其他server的Term大于当前Term
				if rep.Term > rf.currentTerm { // this leader node is outdated
					rf.currentTerm = rep.Term
					rf.state = FOLLOWER
					rf.persist()
					DPrintf("rf.me: [%d] currentTerm: %d AE other server: %d > currentTerm", rf.me, rf.currentTerm, index)
					go func() { rf.electionReset <- 1 }()
					rf.mu.Unlock() // 2 unlock
					return
				} else {
					rf.nextIndex[index] = rep.ConflictIndex
					DPrintf("rf.me: [%d] currentTerm: %d nextIndex[%d]: %d back", rf.me, rf.currentTerm, index, rf.nextIndex[index])
				}
			}
		}
		rf.mu.Unlock() // ok == false 2 unlock
	} else {
		// send InstallSnapshot RPC
		args := InstallSnapshotArgs{
			Term:             rf.currentTerm,
			LeaderId:         rf.me,
			LastIncludeIndex: rf.lastIncludeIndex,
			LastIncludeTerm:  rf.lastIncludeTerm,
			Offset:           0,
			Data:             rf.persister.ReadSnapshot(),
			Done:             false,
		}
		reply := InstallSnapshotReply{}
		//DPrintf("rf.me: [%d] I am Leader sendInstallSnapshotRPC to %d LastIncludeIndex: %d LastIncludeTerm: %d", rf.me, index, rf.lastIncludeIndex, rf.lastIncludeTerm)
		rf.mu.Unlock()
		ok := rf.sendInstallSnapshotRPC(index, &args, &reply)
		rf.mu.Lock() // 3 lock
		if ok && rf.currentTerm == args.Term {
			if reply.Term > rf.currentTerm { // this leader node is outdated
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.persist()
				DPrintf("rf.me: [%d] currentTerm: %d AE other server: %d > currentTerm", rf.me, rf.currentTerm, index)
				go func() { rf.electionReset <- 1 }()
				rf.mu.Unlock() // for
				return
			} else {
				rf.nextIndex[index] = rf.getLogLen()
				rf.matchIndex[index] = rf.lastIncludeIndex
				DPrintf("rf.me: [%d] sendInstallSnapshotRPC reply rf.nextIndex[%d]: %d rf.matchIndex[%d]: %d", rf.me, index, rf.nextIndex[index], index, rf.matchIndex[index])
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock() // ok == false 3 unlock
	}
}

func (rf *Raft) doConsensus() {
	rf.mu.Lock()
	if rf.state == LEADER {
		for i, _ := range rf.peers {
			go rf.doAppendEntries(i)
		}
		logLen := rf.getLogLen()
		rf.commitIndex = rf.lastApplied
		//log.Printf("rf.me: [%d] rf.commitIndex: %d logLen: %d rf.lastApplied: %d rf.lastIncludeIndex: %d", rf.me, rf.commitIndex, logLen, rf.lastApplied, rf.lastIncludeIndex)
		for N := rf.commitIndex + 1; N < logLen; N++ {
			logTerm := rf.log[rf.getLogIndex(N)].Term
			if logTerm < rf.currentTerm {
				continue
			} else if logTerm > rf.currentTerm {
				break
			}
			followerHas := 0
			for index := range rf.peers {
				if rf.matchIndex[index] >= N {
					followerHas++
				}
			}
			// If majority has the log entry of index N
			if followerHas > len(rf.peers)/2 && rf.commitIndex < N {
				rf.commitIndex = N
			}
			DPrintf("rf.me: [%d] leader matchIndex: %v rf.commitIndex: %d rf.lastApplied: %d", rf.me, rf.matchIndex, rf.commitIndex, rf.lastApplied)
		}
	}
	rf.mu.Unlock()

}
// 判断log是否完整
func (rf *Raft) isCandidateOutdata(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("rf.me: [%d] rf.log: %v args.LastLogIndex: [%d] term: %d\n", rf.me, rf.log, args.LastLogIndex, args.LastLogTerm)
	term := rf.getLogTerm(rf.getLastLogIndex())
	if args.LastLogTerm != term {
		return args.LastLogTerm < term
	}
	return args.LastLogIndex < rf.getLastLogIndex()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Candidate: %d rf.me: %d RequestVote",args.CandidateId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		//all server rule 1 If RPC request or response contains term T > currentTerm:
		DPrintf("Candidate: %d rf.me: %d RequestVote args.Term > rf.currentTerm",args.CandidateId, rf.me)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}

	reply.VoteGranted = false
	if args.Term < rf.currentTerm || (rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		// Reply false if term < currentTerm (§5.1)  If votedFor is not null and not candidateId,
		DPrintf("Candidate: %d rf.me: %d rejected args.Term: %d < rf.currentTerm: %d || rf.voteFor: %d", args.CandidateId, rf.me, args.Term, rf.currentTerm, rf.voteFor)
	} else if rf.isCandidateOutdata(args, reply) == true {
		// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		// Reply false if candidate’s log is at least as up-to-date as receiver’s log
		DPrintf("Candidate: %d rf.me: %d rejected isCandidateOutdata", args.CandidateId, rf.me)
	} else {
		DPrintf("Candidate: %d voteFor rf.me: %d", args.CandidateId, rf.me)
		//grant vote
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = FOLLOWER
		rf.persist()
		go func() {
			rf.electionReset <- 1
		}()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// DPrintf("rf.me: [%d] sendRequestVote to server [%d]\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("rf.me: [%d] sendAppendEntries to server [%d]\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}

func (rf *Raft) logConsistencyCheck(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if args.PreLogIndex < rf.lastIncludeIndex {
		reply.ConflictIndex = -1
		return false
	} else if args.PreLogIndex == rf.lastIncludeIndex {
		if args.PreLogTerm != rf.lastIncludeTerm {
			reply.ConflictIndex = -1
			return false
		}
	} else {
		// preLogIndex == lastIncludeIndex 特殊判断
		lastLogIndex := rf.getLastLogIndex()
		//DPrintf("PreLogIndex: %d %d", args.PreLogIndex, lastLogIndex)
		if lastLogIndex < args.PreLogIndex || rf.getLogTerm(args.PreLogIndex) != args.PreLogTerm {
			// 不一致的话需要删除当前开始之后的log
			if lastLogIndex >= args.PreLogIndex {
				preLogIndexTerm := rf.getLogTerm(args.PreLogIndex)
				conflictTerm := preLogIndexTerm
				//i := args.PreLogIndex
				i := rf.getLogIndex(args.PreLogIndex)

				for ; i > 0; i-- {
					if rf.log[i].Term != conflictTerm {
						break
					}
				}
				reply.ConflictIndex = rf.getOriginIndex(i + 1)
			} else {
				reply.ConflictIndex = rf.getLogLen()
			}
			return false
		}
	}
	return true
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("rf.me: [%d] rf.currentTerm :%d AppendEntriesHandler args: %v rf.log: %v rf.committed: %d\n", rf.me, rf.currentTerm, args, rf.log, rf.commitIndex)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.PreLogIndex < rf.commitIndex {
		reply.Success = false
		return
	}
	if args.Term >= rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
		go func() {
			rf.electionReset <- 1
		}()
		if rf.logConsistencyCheck(args, reply) == true {
			if len(args.Entries) > 0 {
				preLogIndex := rf.getLogIndex(args.PreLogIndex)
				rf.log = append(rf.log[:preLogIndex+1], args.Entries...)
				//rf.log = append(rf.log[:args.PreLogIndex+1], args.Entries...)
				rf.persist()
			}
			if rf.commitIndex < args.LeaderCommit {
				lastLogIndex := rf.getLastLogIndex()
				if args.LeaderCommit < lastLogIndex {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = lastLogIndex
				}
			}
			reply.Success = true
			DPrintf("rf.me: [%d] logReplicateAE success len(rf.log): %d rf.log: %v reply: %v rf.commitIndex: %d lastApplied: %d rf.lastIncludeIndex: %d", rf.me, len(rf.log), rf.log, reply, rf.commitIndex, rf.lastApplied, rf.lastIncludeIndex)
		} else {
			DPrintf("rf.me: [%d] logReplicateAE failed len(rf.log): %d args.preLogIndex: %d reply: %v lastIncludeIndex: %d", rf.me, len(rf.log), args.PreLogIndex, reply, rf.lastIncludeIndex)
			reply.Success = false
		}
	} else {
		reply.Success = false
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) kickoffElection() {
	//ms := 200+
	//DPrintf("rf.me: [%d] election interval: %d", rf.me, 200+ms)
	for rf.killed() == false {
		select {
		case <-rf.electionReset:
			DPrintf("rf.me: [%d] electionReset", rf.me)
		case <-time.After(time.Duration(150+rand.Int63()%300) * time.Millisecond):
			rf.election()

		}
	}
}
func (rf *Raft) election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("rf.me: [%d] election rf.state: %d", rf.me, rf.state)
	if rf.state == LEADER {
		return
	}
	if rf.state == FOLLOWER {
		rf.state = CANDIDATE
		rf.persist()
	}
	var counterLock sync.Mutex
	if rf.state == CANDIDATE {
		//rf.print("start to request votes for term %d", rf.currentTerm+1)
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.persist()

		counter := 1

		//logLen := rf.getLogLen()
		lastTerm := 0
		lastIndex := rf.getLastLogIndex()
		if lastIndex < rf.lastIncludeIndex {
			log.Fatalf("rf.me: [%d] election lastIndex:[%d] < rf.lastIncludeIndex: [%d]", rf.me, lastIndex, rf.lastIncludeIndex)
		}
		if lastIndex == rf.lastIncludeIndex {
			lastTerm = rf.lastIncludeTerm
		} else {
			lastTerm = rf.getLogTerm(rf.getLastLogIndex())
		}

		//DPrintf("rf.me: [%d] begin election currentTerm: %d logLen: %d lastIndex: %d lastTerm: %d", rf.me, rf.currentTerm, logLen, lastIndex, lastTerm)
		rvArgs := RequestVoteArgs{rf.currentTerm, rf.me, lastIndex, lastTerm}
		rvReplies := make([]RequestVoteReply, len(rf.peers))

		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int) {
				if rf.killed() == true {
					return
				}
				DPrintf("rf.me: [%d] sendRequestVote to: %d", rf.me, index)
				ok := rf.sendRequestVote(index, &rvArgs, &rvReplies[index])
				rf.mu.Lock()
				// 其他server的Term比当前大
				if ok && rvArgs.Term == rf.currentTerm {
					if rvReplies[index].Term > rf.currentTerm {
						rf.currentTerm = rvReplies[index].Term
						rf.state = FOLLOWER
						rf.voteFor = -1
						rf.persist()
						go func() {
							rf.electionReset <- 1
						}()
						DPrintf("rf.me: [%d] currentTerm: %d other server Term > currentTerm", rf.me, rf.currentTerm)
						//rf.persist(rf.currentTerm, rf.voteFor, rf.log)
					} else if ok && (rvArgs.Term == rf.currentTerm) && rvReplies[index].VoteGranted {
						counterLock.Lock()
						counter++
						DPrintf("rf.me: [%d] currentTerm: %d get a vote from :%d", rf.me, rf.currentTerm, index)
						if counter >= len(rf.peers)/2+1 && rf.state != LEADER {
							rf.transToLeader()
						}
						counterLock.Unlock()
					} else {
						DPrintf("rf.me: [%d] currentTerm: %d rejected by %d reply: %v", rf.me, rf.currentTerm, index, rvReplies[index])
					}
				}
				rf.mu.Unlock()
			}(index)
		}
	}
}


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.electionReset = make(chan int)

	rf.log = append(rf.log, LogEntry{0, 0}) // begin in index 1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.InitInstallSnapshotToApp()
	rf.InitThread()
	DPrintf("rf.me: [%d] start rf.commitIndex: %d rf.lastApplied: %d", rf.me, rf.commitIndex, rf.lastApplied)
	return rf
}

func(rf *Raft) InitThread() {
	go rf.kickoffElection()
	go rf.autoSendApplyMsgs()
	go rf.sendHeartBeats()
}

// snapshot
// ------------ 新数据转换到原始数据 --------------------
func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludeIndex + len(rf.log) - 1
}

func (rf *Raft) getOriginIndex(index int) int {
	return rf.lastIncludeIndex + index
}
func (rf *Raft) getLogLen() int {
	return rf.lastIncludeIndex + len(rf.log)
}

// ---------------------------------------------------

// ------------ 原始数据转换到新数据 --------------------
// 原始坐标在新坐标中的位置
func (rf *Raft) getLogIndex(index int) int {
	return index - rf.lastIncludeIndex
}

func (rf *Raft) getLogTerm(index int) int {
	return rf.log[rf.getLogIndex(index)].Term
}

// ---------------------------------------------------

func (rf *Raft) ShouldSnapshot(maxRaftState int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() > maxRaftState {
		//log.Printf("rf.me: [%d] rf.persister.RaftStateSize(): %d rf.lastIncludeIndex: %d", rf.me, rf.persister.RaftStateSize(), rf.lastIncludeIndex)
		return true
	}
	return false
}

func (rf *Raft) snapShotPersist(data []byte) {
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), data)
}

func (rf *Raft) GetAppliedLogLen() int {
	rf.mu.Lock()
	lastApplied := rf.lastIncludeIndex
	rf.mu.Unlock()
	return lastApplied
}

func (rf *Raft) LogTruncate(data []byte, lastIncludeIndex int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("rf.me: [%d] rf.state: %d LogTruncate lastIncludeIndex: %d lastApplied: %d logLen: %d log: %v", rf.me, rf.state, rf.lastIncludeIndex, rf.lastApplied, len(rf.log), rf.log)
	if rf.lastIncludeIndex > rf.lastApplied {
		log.Fatalf("rf.me: [%d] rf.lastIncludeIndex: %d > rf.lastApplied: %d", rf.me, rf.lastIncludeIndex, rf.lastApplied)
		return
	}

	if rf.lastIncludeIndex >= lastIncludeIndex {
		//log.Printf("rf.me: [%d] rf.lastIncludeIndex: %d >= lastIncludeIndex: %d", rf.me, rf.lastIncludeIndex, lastIncludeIndex)
		return
	}

	var newLog []LogEntry
	newLog = append(newLog, LogEntry{})
	// rf.getLogIndex(lastIncludeIndex)+1 < logLen -> 6:1
	newLog = append(newLog, rf.log[rf.getLogIndex(lastIncludeIndex)+1:]...)
	rf.lastIncludeIndex = lastIncludeIndex
	rf.lastIncludeTerm = rf.getLogTerm(lastIncludeIndex)
	rf.log = newLog
	log.Printf("rf.me: [%d] LogTruncate len(rf.log): %d",rf.me, len(rf.log))
	rf.persist()
	rf.snapShotPersist(data)
	//log.Printf("rf.me: [%d] store Snapshot lastIncludeIndex: %d rf.lastApplied: %d len(rf.log): %d", rf.me, rf.lastIncludeIndex, rf.lastApplied, len(rf.log))
	DPrintf("rf.me: [%d] rf.state: %d LogTruncate Finish lastIncludeIndex: %d lastApplied: %d logLen: %d log: %v", rf.me, rf.state, rf.lastIncludeIndex, rf.lastApplied, len(rf.log), rf.log)
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPCHandle", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshotRPCHandle(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term >= rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
		go func() {
			rf.electionReset <- 1
		}()
	}

	// 过期的snapshot
	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		DPrintf("rf.me: [%d] InstallSnapshotRPCHandle args.LastIncludeIndex: %d < rf.lastIncludeIndex: %d", rf.me, args.LastIncludeIndex, rf.lastIncludeIndex)
		return
	}

	// snapshot 大于目前的 日志
	//sel := 1
	var newLog []LogEntry
	newLog = append(newLog, LogEntry{})
	// args.LastIncludeIndex+1 >= rf.getLastLogIndex() 当初我为啥这么写???
	if args.LastIncludeIndex >= rf.getLastLogIndex() {
		// 1. 覆盖超过整个日志
		//sel = 1
		if rf.commitIndex < args.LastIncludeIndex {
			rf.commitIndex = args.LastIncludeIndex
		}
	} else if  rf.getLogTerm(args.LastIncludeIndex) != args.LastIncludeTerm {
		// 2. 快照处的日志有冲突，扔掉快照外的所有日志
		//sel = 2
		rf.commitIndex = args.LastIncludeIndex
	} else {
		// 2. 覆盖一部分日志
		//sel = 3
		newLog = append(newLog, rf.log[rf.getLogIndex(args.LastIncludeIndex)+1:]...)
		if rf.commitIndex < args.LastIncludeIndex {
			rf.commitIndex = args.LastIncludeIndex
		}
	}

	rf.log = newLog
	log.Printf("rf.me: [%d] InstallSnapshotRPCHandle len(rf.log): %d",rf.me, len(rf.log))
	// 要放在后面赋值，否则会导致rf.log[rf.getLogIndex(args.LastIncludeIndex)+1:]没变化
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm


	rf.persist()
	//log.Printf("rf.me[%d] InstallSnapshotRPCHandle lastIncludeIndex: %d lastApplied: %d commitIndex: %d", rf.me, rf.lastIncludeIndex, rf.lastApplied, rf.commitIndex)
	DPrintf("rf.me: [%d] InstallSnapshotRPCHandle finish lastIncludeIndex: %d, rf.log: %v", rf.me, rf.lastIncludeIndex, rf.log)

	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	rf.InstallSnapshotToApp()
	// 需要重新提交
	rf.lastApplied = args.LastIncludeIndex
}

func (rf *Raft) InstallSnapshotToApp() {

	applyMsg := ApplyMsg{
		CommandValid: false,
		Snapshot: rf.persister.ReadSnapshot(),
		CommandIndex: rf.lastIncludeIndex,
	}
	DPrintf("rf.me: [%d] InstallSnapshotToApp applyCh: %d", rf.me, len(rf.applyCh))
	rf.applyCh <- applyMsg
}

func (rf *Raft) InitInstallSnapshotToApp() {
	rf.mu.Lock()
	applyMsg := ApplyMsg{
		CommandValid: false,
		Snapshot: rf.persister.ReadSnapshot(),
		CommandIndex: rf.lastIncludeIndex,
	}

	rf.applyCh <- applyMsg
	rf.commitIndex = rf.lastIncludeIndex
	rf.lastApplied = rf.lastIncludeIndex
	//log.Printf("rf.me: [%d] InitInstallSnapshotToApp rf.lastIncludeIndex: %d rf.lastApplied: %d", rf.me, rf.lastIncludeIndex, rf.lastApplied)
	rf.mu.Unlock()
}
