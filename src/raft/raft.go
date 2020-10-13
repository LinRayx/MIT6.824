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
	"sync"
	"time"
)
import "sync/atomic"
import "src/labrpc"

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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Cmd 	interface{}
	Term 	int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted 	bool
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
	Term 		int
	Success		bool
	ConflictIndex	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me  		   	int                 // this peer's index into peers[]
	dead      		int32               // set by Kill()
	applyCh 		chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionReset	chan int
	// perisistent
	currentTerm 	int
	voteFor 		int
	log				[]LogEntry

	// vote
	state 			int8

	// log
	nextIndex		[]int
	matchIndex		[]int
	commitIndex		int
	lastApplied		int
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
	index = len(rf.log)-1
	DPrintf("rf.me: [%d] Start index: %d term: %d isLeader:%v command: %v\n", rf.me, index, term, isLeader, command)
	rf.mu.Unlock()
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
			//rf.sendApplyMsgs(rf.lastApplied+1, rf.commitIndex)
			rf.lastApplied++
			DPrintf("rf.me:[%d] autoSendApplyMsgs lastApplied: %d commitIndex: %d", rf.me, rf.lastApplied, rf.commitIndex)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg

		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
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
		d.Decode(&rf.log) != nil {
		log.Fatalf("rf.me: [%d] readPersist decode error", rf.me)
	}
	DPrintf("rf.me: [%d] currentTerm: %d rf.voteFor: %d rf.log: %v", rf.me, rf.currentTerm, rf.voteFor, rf.log)
	rf.mu.Unlock()
}

func (rf *Raft) transToLeader() {
	DPrintf("rf.me: [%d] transToLeader", rf.me)
	for i,_ := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.state = LEADER
	rf.persist()
}

func (rf *Raft) sendHeartBeats() {

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == LEADER {
			for i, _ := range rf.peers {
				go func(index int) {
					for rf.killed() == false {
						rf.mu.Lock()
						if rf.state != LEADER {
							rf.mu.Unlock()
							break
						}
						if index == rf.me {
							rf.nextIndex[index] = len(rf.log)
							rf.matchIndex[index] = len(rf.log) - 1
							rf.mu.Unlock()
							break
						}
						// 切片是引用赋值
						appendEntries := make([]LogEntry, len(rf.log[rf.nextIndex[index]:]))
						//for i, cmd := range rf.log[rf.nextIndex[index]:] {
						//	appendEntries[i] = cmd
						//}
						copy(appendEntries, rf.log[rf.nextIndex[index]:])
						prevIndex := rf.nextIndex[index]-1
						req := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							LeaderCommit: rf.commitIndex,
							Entries:      appendEntries,
							PreLogIndex:  prevIndex,
							PreLogTerm:   rf.log[rf.nextIndex[index]-1].Term,
						}
						rep := AppendEntriesReply{}
						rf.mu.Unlock()
						DPrintf("rf.me: [%d] I am Leader sendAE to %d PreLogIndex: %d PreLogTerm: %d", rf.me, index, req.PreLogIndex, req.PreLogTerm)
						ok := rf.sendAppendEntries(index, &req, &rep)
						rf.mu.Lock()
						if ok && rf.currentTerm == req.Term { // ensure the reply is not outdated
							if rep.Success == true {
								rf.matchIndex[index] = req.PreLogIndex + len(req.Entries)
								rf.nextIndex[index] = rf.matchIndex[index] + 1
								DPrintf("rf.me: [%d] currentTerm: %d tongbu success rf.matchIndex[%d]: %d rf.nextIndex[%d]: %d", rf.me, rf.currentTerm, index, rf.matchIndex[index], index, rf.nextIndex[index])
								rf.mu.Unlock() // for
								break
							} else {
								// 其他server的Term大于当前Term
								if rep.Term > rf.currentTerm { // this leader node is outdated
									rf.currentTerm = rep.Term
									rf.state = FOLLOWER
									rf.persist()
									DPrintf("rf.me: [%d] currentTerm: %d AE other server: %d > currentTerm", rf.me, rf.currentTerm, index)
									go func() { rf.electionReset <- 1 }()
									rf.mu.Unlock() // for
									break
								} else {
									rf.nextIndex[index] = rep.ConflictIndex
									DPrintf("rf.me: [%d] currentTerm: %d nextIndex[%d]: %d back", rf.me, rf.currentTerm, index, rf.nextIndex[index])
								}
							}
						}
						rf.mu.Unlock() // for
					}
				}(i)
			}

			for N := rf.commitIndex + 1; N < len(rf.log); N++ {
				// To eliminate problems like the one in Figure 8,
				//  Raft never commits log entries from previous terms by count- ing replicas.
				if rf.log[N].Term < rf.currentTerm {
					continue
				} else if rf.log[N].Term > rf.currentTerm {
					break
				}
				followerHas := 0
				for index := range rf.peers {
					if rf.matchIndex[index] >= N {
						followerHas++
					}
				}
				// If majority has the log entry of index N
				if followerHas > len(rf.peers)/2 {
					rf.commitIndex = N
					//go rf.autoSendApplyMsgs()
				}
				DPrintf("rf.me: [%d] leader matchIndex: %v rf.commitIndex: %d", rf.me, rf.matchIndex, rf.commitIndex)
			}
			rf.mu.Unlock()
			DPrintf("-1")
			time.Sleep(100 * time.Millisecond)

		} else {
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// 判断log是否完整
func (rf *Raft) isCandidateOutdata(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("rf.me: [%d] rf.log: %v args.LastLogIndex: [%d] term: %d\n", rf.me, rf.log, args.LastLogIndex, args.LastLogTerm)
	if args.LastLogTerm != rf.log[len(rf.log)-1].Term {
		return args.LastLogTerm < rf.log[len(rf.log)-1].Term
	}
	return args.LastLogIndex < len(rf.log)-1

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {//all server rule 1 If RPC request or response contains term T > currentTerm:
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
		//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		// Reply false if candidate’s log is at least as up-to-date as receiver’s log
		DPrintf("Candidate: %d rf.me: %d rejected isCandidateOutdata", args.CandidateId, rf.me)
	} else {
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
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

	if len(rf.log)-1 < args.PreLogIndex || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		// 不一致的话需要删除当前开始之后的log

		if len(rf.log)-1 >= args.PreLogIndex {
			conflictTerm := rf.log[args.PreLogIndex].Term
			i := args.PreLogIndex
			for ; i > 0; i-- {
				if rf.log[i].Term != conflictTerm {
					break
				}
			}
			reply.ConflictIndex = i+1
		} else {
			reply.ConflictIndex = len(rf.log)
			//DPrintf("reply.ConflictIndex :%d rf.log: %v", reply.ConflictIndex, rf.log)
		}

		return false
	}
	return true
}
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("rf.me: [%d] rf.currentTerm :%d AppendEntriesHandler args: %v rf.log: %v rf.committed: %d\n", rf.me, rf.currentTerm, args, rf.log, rf.commitIndex)
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
		go func() {
			rf.electionReset <- 1
		}()
		if rf.logConsistencyCheck(args, reply) == true {
			if len(args.Entries) > 0 {
				rf.log = append(rf.log[:args.PreLogIndex+1], args.Entries...)
				rf.persist()
			}
			if rf.commitIndex < args.LeaderCommit {
				if args.LeaderCommit < len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
				}else{
					rf.commitIndex = len(rf.log)-1
				}
			}
			reply.Success = true
			DPrintf("rf.me: [%d] logReplicateAE success len(rf.log): %d args: %v reply: %v rf.commitIndex: %d lastApplied: %d", rf.me, len(rf.log), args, reply, rf.commitIndex, rf.lastApplied)
		} else {
			DPrintf("rf.me: [%d] logReplicateAE failed len(rf.log): %d args: %v reply: %v", rf.me, len(rf.log), args, reply)
			reply.Success = false
		}
	} else {
		reply.Success = false
	}
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

func (rf *Raft) kickoffElection() {
	//ms := 200+
	//DPrintf("rf.me: [%d] election interval: %d", rf.me, 200+ms)
	for rf.killed() == false {
		select {
			case <-rf.electionReset:
				DPrintf("rf.me: [%d] electionReset", rf.me)
			case <-time.After(time.Duration(300+rand.Int63()%300) * time.Millisecond):
				DPrintf("rf.me: [%d] try to election", rf.me)
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
		DPrintf("rf.me: [%d] begin election currentTerm: %d", rf.me, rf.currentTerm)
		counter := 1

		logLen := len(rf.log)
		lastTerm := 0
		lastIndex := logLen-1
		if logLen > 0 {
			lastTerm = rf.log[logLen-1].Term
		}
		rvArgs := RequestVoteArgs{rf.currentTerm, rf.me, lastIndex, lastTerm}
		rvReplies := make([]RequestVoteReply, len(rf.peers))

		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int) {
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
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.electionReset = make(chan int)
	rf.commitIndex = 0
	rf.log = append(rf.log, LogEntry{0, 0}) // begin in index 1
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.kickoffElection()
	go rf.autoSendApplyMsgs()
	go rf.sendHeartBeats()
	return rf
}
