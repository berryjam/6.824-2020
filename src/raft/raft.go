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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "g.csail.mit.edu/6.824/src/labrpc"

// import "bytes"
// import "../labgob"

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

type LogEntry struct {
	data []byte
	term int
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                   sync.Mutex          // Lock to protect shared access to this peer's state
	peers                []*labrpc.ClientEnd // RPC end points of all peers
	persister            *Persister          // Object to hold this peer's persisted state
	me                   int                 // this peer's index into peers[]
	dead                 int32               // set by Kill()
	lastRpcRevMillSecond int64

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	CurStatus Status
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.CurStatus == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

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
}

type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.lastRpcRevMillSecond = time.Now().UnixNano() / 1e6
	// todo
	//reply.term = rf.currentTerm
	//
	//if args.term > rf.currentTerm {
	//	rf.currentTerm = args.term
	//	rf.CurStatus = Follower
	//}
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.lastRpcRevMillSecond = time.Now().UnixNano() / 1e6
	reply.term = rf.currentTerm
	reply.voteGranted = false // default not granted
	if args.term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	/**
		The RequestVote RPC implements this restriction: the RPC
	includes information about the candidate’s log, and the
	voter denies its vote if its own log is more up-to-date than
	that of the candidate.
		Raft determines which of two logs is more up-to-date
	by comparing the index and term of the last entries in the
	logs. If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer is
	more up-to-date.
	*/
	isCandidateLogUpToDate := false
	if len(rf.log) == 0 {
		if args.lastLogIndex >= 0 {
			isCandidateLogUpToDate = true
		}
	} else {
		if args.lastLogTerm > rf.log[len(rf.log)-1].term {
			isCandidateLogUpToDate = true
		} else if args.lastLogTerm == rf.log[len(rf.log)-1].term {
			if args.lastLogIndex >= len(rf.log) {
				isCandidateLogUpToDate = true
			}
		}
	}
	if (rf.votedFor == -1 || rf.votedFor == args.candidateId) && isCandidateLogUpToDate {
		reply.voteGranted = true
	}
	if reply.voteGranted {
		rf.votedFor = args.candidateId
	}
	if args.term > rf.currentTerm {
		rf.currentTerm = args.term
		rf.CurStatus = Follower
	}

	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

	// Your code here (2B).

	return index, term, isLeader
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.CurStatus = Follower
	rf.lastRpcRevMillSecond = -1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(r *Raft) {
		for {
			r.mu.Lock()
			if r.CurStatus == Follower {
				salt := rand.Intn(11)
				timeoutUnit := time.Duration(500+salt) * time.Millisecond
				if r.lastRpcRevMillSecond == -1 {
					r.mu.Unlock()
					time.Sleep(timeoutUnit)
				} else {
					now := time.Now()
					if r.lastRpcRevMillSecond+int64(timeoutUnit) <= now.UnixNano()/1e6 { // timeout
						/**
						If a
						follower receives no communication over a period of time
						called the election timeout, then it assumes there is no viable leader and begins an election to choose a new leader.
						*/
						r.currentTerm++
						r.CurStatus = Candidate
						r.mu.Unlock()
					} else {
						r.mu.Unlock()
						fmt.Printf("sleep %+v millisecond\n", r.lastRpcRevMillSecond+int64(timeoutUnit)-now.UnixNano()/1e6)
						time.Sleep(time.Duration(r.lastRpcRevMillSecond+int64(timeoutUnit)-now.UnixNano()/1e6) * time.Millisecond)
					}
				}
			}
		}
	}(rf)

	go func(r *Raft) {
		for {
			r.mu.Lock()
			if r.CurStatus == Candidate {
				r.mu.Unlock()
				r.StartElection()
				salt := rand.Intn(11)
				timeoutUnit := time.Duration(500+salt) * time.Millisecond
				time.Sleep(timeoutUnit)
			} else {
				r.mu.Unlock()
			}
		}
	}(rf)

	go func(r *Raft) {
		for {
			r.mu.Lock()
			if r.CurStatus == Leader {
				r.mu.Unlock()
				r.HeartBeat()
			} else {
				r.mu.Unlock()
			}
		}
	}(rf)

	return rf
}

func (r *Raft) HeartBeat() {
	for i := 0; i < len(r.peers); i++ {
		if i != r.me {
			go func() {
				r.mu.Lock()
				appendEntriesArgs := AppendEntriesArgs{
					term:         r.currentTerm,
					leaderId:     r.me,
					prevLogIndex: len(r.log) - 1,
					prevLogTerm:  r.log[len(r.log)-1].term,
					entries:      []LogEntry{},
					leaderCommit: r.commitIndex,
				}
				r.mu.Unlock()
				appendEntriesReply := AppendEntriesReply{}
				r.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)
			}()
		}
	}
}

func (r *Raft) StartElection() {
	r.mu.Lock()
	voteReq := RequestVoteArgs{
		term:         r.currentTerm,
		candidateId:  r.me,
		lastLogIndex: len(r.log) - 1,
		lastLogTerm:  r.log[len(r.log)-1].term,
	}
	r.mu.Unlock()
	var n int32 = 0
	var wg sync.WaitGroup
	for i := 0; i < len(r.peers); i++ {
		if i != r.me {
			wg.Add(1)
			go func() {
				voteReply := RequestVoteReply{
				}
				voteSucc := r.sendRequestVote(i, &voteReq, &voteReply)
				if voteSucc {
					if voteReply.voteGranted {
						atomic.AddInt32(&n, 1)
					}
					r.mu.Lock()
					if voteReply.term > r.currentTerm {
						r.currentTerm = voteReply.term
						r.CurStatus = Follower
						r.lastRpcRevMillSecond = -1
					}
					r.mu.Unlock()
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
	if atomic.LoadInt32(&n) >= int32(len(r.peers)/2+1) {
		r.mu.Lock()
		r.CurStatus = Leader
		r.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
