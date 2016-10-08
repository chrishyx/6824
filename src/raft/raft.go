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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Raft is a Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	currentState uint32
	votedFor     int
	log          []*ApplyMsg
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int

	lastHeartBeat int64
	heartBeatChan chan *AppendEntriesRequest
	elecDoneChan  chan int
	voteChan      chan *RequestVoteArgs
	voteResChan   chan *RequestVoteReply
}

const (
	follower uint32 = iota
	candidate
	leader
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.currentState == leader
	DPrintf("Get State for %v, myterm is %d, I am Leader: %v\n", rf.me, term,
		isleader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// RequestVoteArgs is an
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidatedID int
	LastLogIndex int64
	LastLogTerm  int
}

// RequestVoteReply is an example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	Vote bool
}

// RequestVote is an example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.voteChan <- &args
	re := <-rf.voteResChan
	reply.Term = re.Term
	reply.Vote = re.Vote
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs,
	reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start is the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	return index, term, isLeader
}

// Kill the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make the service or tester wants to create a Raft server. the ports
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
	rf.currentState = follower
	rf.heartBeatChan = make(chan *AppendEntriesRequest, 10)
	rf.elecDoneChan = make(chan int, 1)
	rf.voteChan = make(chan *RequestVoteArgs, 1)
	rf.voteResChan = make(chan *RequestVoteReply, 1)

	// Your initialization code here.
	go func() {
		rand.Seed(time.Now().UnixNano())
		duration := (rand.Intn(150) + 150) * 1000000
		DPrintf("%v duration is %d", rf.me, duration)
		for {
			select {
			case <-time.After(time.Duration(duration)):
				switch rf.currentState {
				case leader:
				case candidate:
					rf.reElect()
				case follower:
					if rf.timeout(duration) {
						rf.currentState = candidate
						rf.elect()
					}
				}
			case args := <-rf.heartBeatChan:
				rf.currentTerm = args.Term
				rf.votedFor = args.LeaderID
				rf.lastHeartBeat = time.Now().UnixNano()
				switch rf.currentState {
				case leader:
					rf.currentState = follower
				case candidate:
					rf.currentState = follower
					rf.elecDoneChan <- 1
				}
			case args := <-rf.voteChan:
				myterm := rf.currentTerm
				var reply RequestVoteReply
				if args.Term > rf.currentTerm {
					rf.currentTerm = args.Term
					rf.votedFor = args.CandidatedID
					rf.lastHeartBeat = time.Now().UnixNano()
					switch rf.currentState {
					case leader:
						rf.currentState = follower
					case candidate:
						rf.currentState = follower
						rf.elecDoneChan <- 1
					}
					reply = RequestVoteReply{args.Term, true}
					rf.voteResChan <- &reply
				} else {
					reply = RequestVoteReply{rf.currentTerm, false}
					rf.voteResChan <- &reply
				}
				DPrintf("%v received vote request for %v, my term is %d, his term is %d, reply is %v\n",
					rf.me, args.CandidatedID, myterm, args.Term, reply)
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) timeout(duration int) bool {
	if time.Now().UnixNano()-rf.lastHeartBeat > int64(duration) {
		DPrintf("%v time out\n", rf.me)
		return true
	}
	return false
}

func (rf *Raft) reElect() {
	//TODO cleanup
	rf.elecDoneChan <- 1
	rf.elect()
}

func (rf *Raft) elect() {
	DPrintf("Vote for me %v\n", rf.me)
	rf.currentTerm++
	resultCh := make(chan *RequestVoteReply, len(rf.peers))
	for index, peer := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(peer *labrpc.ClientEnd) {
			request := RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
			var reply RequestVoteReply
			ok := peer.Call("Raft.RequestVote", request, &reply)
			if ok {
				resultCh <- &reply
			}
		}(peer)
	}
	go func() {
		grant := 1
	loop:
		for i := 1; i < len(rf.peers); i++ {
			select {
			case reply := <-resultCh:
				if reply.Vote {
					grant++
				}
				if grant > len(rf.peers)/2 && rf.currentState == candidate {
					rf.currentState = leader
					DPrintf("%v becomes the leader\n", rf.me)
					go func() {
						for rf.currentState == leader {
							rf.sendHeartBeat()
							time.Sleep(150 * time.Microsecond)
						}
					}()
					break loop
				}
			case <-rf.elecDoneChan:
				break loop
			}
		}
		DPrintf("%v election end", rf.me)
	}()
}

func (rf *Raft) sendHeartBeat() {
	for index, peer := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(peer *labrpc.ClientEnd) {
			args := AppendEntriesRequest{rf.currentTerm, rf.me, 0, 0}
			var reply AppendEntriesReply
			ok := peer.Call("Raft.AppendEntries", args, &reply)
			if ok != true {
				//DPrintf("Rpc err when sending heartbeat to %v", peer)
			}
		}(peer)
	}
}

//AppendEntriesRequest request args of AppendEntries
type AppendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int64
	PrevLogTerm  int
	//entries      []interface{}
}

//AppendEntriesReply reply of AppendEntries
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//AppendEntries nvoked by leader to replicate log entries, also used as
//heartbeat
func (rf *Raft) AppendEntries(args AppendEntriesRequest,
	reply *AppendEntriesReply) {
	// DPrintf("%v received heartbeat from %v\n", rf.me, args.LeaderID)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//TODO
	rf.heartBeatChan <- &args
	reply.Term = args.Term
	reply.Success = true
	return
}
