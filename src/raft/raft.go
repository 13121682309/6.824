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

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "sort"
import "sync/atomic"
import "fmt"
import "bytes"
import "labgob"

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

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

const HeartbeatInterval = 50
const MinElectionTimeout = 300
const MaxElectionTimeout = 400
const VOTENULL = -1

//
// struct definition for log entry
//
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("(i:%v,t:%v,c:%v)", entry.Index, entry.Term, entry.Command)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state             RaftState
	heartbeatInterval time.Duration

	// Persistent state on all servers
	// Updated on stable storage before responding to RPCs
	currentTerm int        // latest term server has seen, initialized to 0
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited, initialized to 0
	lastApplied int // index of highest log entry applied to state machine, initialized to 0

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	applyCh       chan ApplyMsg
	appendEntryCh chan bool
	grantVoteCh   chan bool
	leaderCh      chan bool
	exitCh        chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	return term, isLeader
}

func (rf *Raft) getPrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm(server int) int {
	prevLogIndex := rf.getPrevLogIndex(server)
	if prevLogIndex == 0 {
		return -1
	} else {
		return rf.log[prevLogIndex].Term
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex == 0 {
		return -1
	} else {
		return rf.log[lastLogIndex].Term
	}
}

func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func getRandomElectionTimeout() time.Duration {
	randomTimeout := MinElectionTimeout + rand.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(randomTimeout) * time.Millisecond
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
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		// error...
	}
}

// RequestVote RPC
// Invoked by candidates to gather votes

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candiate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 1. Reply false if term < currentTerm
	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote

	// all servers
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	voteGranted := false
	if args.Term == rf.currentTerm && (rf.votedFor == VOTENULL || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		voteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		dropAndSet(rf.grantVoteCh)
		DPrintf("%v vote %v, my term:%d, vote term:%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
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

// AppendEntries RPC
// Invoked by leader to replicate log entries, also used as heartbeat

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store, empty for heartbeat
	LeaderCommit int        // leader's commitIndex
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	success := false
	conflictIndex := 0
	conflictTerm := 0

	// 1. Reply false if term < currentTerm
	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	// all servers
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		rf.state = Follower
		dropAndSet(rf.appendEntryCh)

		if args.PrevLogIndex > rf.getLastLogIndex() {
			conflictIndex = len(rf.log)
			conflictTerm = 0
		} else {
			prevLogTerm := rf.log[args.PrevLogIndex].Term
			if args.PrevLogTerm != prevLogTerm {
				conflictTerm = rf.log[args.PrevLogIndex].Term
				for i := 1; i < len(rf.log); i++ {
					if rf.log[i].Term == conflictTerm {
						conflictIndex = i
						break
					}
				}
			}

			if args.PrevLogIndex == 0 || (args.PrevLogIndex <= rf.getLastLogIndex() && args.PrevLogTerm == prevLogTerm) {
				success = true
				index := args.PrevLogIndex
				for i := 0; i < len(args.Entries); i++ {
					index++
					if index > rf.getLastLogIndex() {
						rf.log = append(rf.log, args.Entries[i:]...)
						DPrintf("Server(%v=>%v), log len:%v", args.LeaderId, rf.me, len(rf.log))
						break
					}

					if rf.log[index].Term != args.Entries[i].Term {
						DPrintf("Term not equal, Server(%v=>%v), prevIndex=%v, index=%v", args.LeaderId, rf.me, args.PrevLogIndex, index)
						for len(rf.log) > index {
							rf.log = rf.log[0 : len(rf.log)-1]
						}
						rf.log = append(rf.log, args.Entries[i])
					}
				}

				DPrintf("Server(%v=>%v), term:%v, args:%v, handle AppendEntries success", args.LeaderId, rf.me, rf.currentTerm, args)

				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
					rf.applyLogs()
				}
			}
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = success
	reply.ConflictIndex = conflictIndex
	reply.ConflictTerm = conflictTerm
}

//
// send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	index := -1
	isLeader := (rf.state == Leader)

	if isLeader {
		index = rf.getLastLogIndex() + 1
		entry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("Kill Server(%v)", rf.me)
	dropAndSet(rf.exitCh)
}

func (rf *Raft) convertToCandidate() {
	defer rf.persist()
	DPrintf("Convert server(%v) state(%v=>candidate) term(%v)", rf.me, rf.state.String(), rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) convertToFollower(term int) {
	defer rf.persist()
	DPrintf("Convert server(%v) state(%v=>follower) term(%v=>%v)", rf.me, rf.state.String(), rf.currentTerm, term)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = VOTENULL
}

func (rf *Raft) convertToLeader() {
	defer rf.persist()

	if rf.state != Candidate {
		return
	}
	DPrintf("Convert server(%v) state(%v=>leader) term %v", rf.me, rf.state.String(), rf.currentTerm)
	rf.state = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var numVoted int32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			DPrintf("sendRequestVote(%v=>%v) args:%v", rf.me, server, args)
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}

				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&numVoted, 1)
				}

				if atomic.LoadInt32(&numVoted) > int32(len(rf.peers)/2) {
					DPrintf("Server(%d) win vote", rf.me)
					rf.convertToLeader()
					dropAndSet(rf.leaderCh)
				}
			}
		}(i, args)
	}
}

func (rf *Raft) applyLogs() {
	for rf.commitIndex > rf.lastApplied {
		DPrintf("Server(%v) applyLogs, commitIndex:%v, lastApplied:%v, command:%v", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied].Command)
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) advanceCommitIndex() {
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	matchIndex[rf.me] = len(rf.log) - 1
	sort.Ints(matchIndex)

	targetMatchIndex := matchIndex[len(rf.peers)/2]
	DPrintf("matchIndex:%v, targetMatchIndex:%v", matchIndex, targetMatchIndex)

	if rf.state == Leader && targetMatchIndex > rf.commitIndex && rf.log[targetMatchIndex].Term == rf.currentTerm {
		DPrintf("Server(%v) advanceCommitIndex(%v=>%v)", rf.me, rf.commitIndex, targetMatchIndex)
		rf.commitIndex = targetMatchIndex
		rf.applyLogs()
	}
}

func (rf *Raft) startAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				nextIndex := rf.nextIndex[server]
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.log[nextIndex:]...)

				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIndex(server),
					PrevLogTerm:  rf.getPrevLogTerm(server),
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(server, args, reply)
				DPrintf("SendAppendEntries (%v=>%v), arg:%v", rf.me, server, args)

				if !ok {
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				if rf.state != Leader || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					DPrintf("SendAppendEntries Success(%v=>%v), nextIndex:%v, matchIndex:%v", rf.me, server, rf.nextIndex, rf.matchIndex)
					rf.advanceCommitIndex()
					rf.mu.Unlock()
					return
				} else {
					newIndex := reply.ConflictIndex
					for i := 1; i < len(rf.log); i++ {
						entry := rf.log[i]
						if entry.Term == reply.ConflictTerm {
							newIndex = i + 1
						}
					}
					rf.nextIndex[server] = Max(1, newIndex)
					rf.mu.Unlock()
				}
			}
		}(i)
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

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower
	rf.heartbeatInterval = time.Duration(HeartbeatInterval) * time.Millisecond

	rf.currentTerm = 0
	rf.votedFor = VOTENULL
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{})

	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state on leaders
	// rf.nextIndex  []int // for each server, index of the next log entry to send to that server
	// rf.matchIndex []int // for each server, index of highest log entry known to be replicated on server

	rf.applyCh = applyCh
	rf.appendEntryCh = make(chan bool, 1)
	rf.grantVoteCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)
	rf.exitCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Make Server(%v)", rf.me)

	go func() {
	Loop:
		for {
			select {
			case <-rf.exitCh:
				DPrintf("Exit Server(%v)", rf.me)
				break Loop
			default:
			}

			electionTimeout := getRandomElectionTimeout()
			rf.mu.Lock()
			state := rf.state
			DPrintf("Server(%d) state:%v, electionTimeout:%v", rf.me, state, electionTimeout)
			rf.mu.Unlock()

			switch state {
			case Follower:
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case Candidate:
				go rf.leaderElection()
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-rf.leaderCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendEntries()
				time.Sleep(rf.heartbeatInterval)
			}

		}
	}()

	return rf
}
