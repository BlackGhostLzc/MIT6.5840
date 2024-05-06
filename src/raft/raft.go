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

// A Go object implementing a single Raft peer.
const (
	Follower  int = 0
	Candidate     = 1
	Leader        = 2
)

type LogEntry struct {
	Command interface{} // each entry contains command for state machine,
	Term    int         // and term when entry was received by leader(fisrt index is 1)
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state    int
	leaderId int

	// 我们需要两个计时器
	electionTimeoutChan chan bool
	heartbeatPeriodChan chan bool

	CurrentTerm int
	VoteFor     int
	Log         []LogEntry

	commitIndex int
	lastApplied int

	latestIssueTime int64 // 最新的leader发送心跳的时间(leader发送心跳)
	latestHeardTime int64 // 最新的收到leader的AppendEntries RPC(包括heartbeat)

	nextIndex  []int
	matchIndex []int

	electionTimeout int // 选举超时时间
	heartbeatPeriod int // 心跳超时时间
}

func state2name(state int) string {
	var name string
	if state == Follower {
		name = "Follower"
	} else if state == Candidate {
		name = "Candidate"
	} else if state == Leader {
		name = "Leader"
	}
	return name
}

func (rf *Raft) switchTo(newState int) {
	rf.state = newState
}

// 为了避免同时多个 raft 实例选举超时，这个时间需要随机
func (rf *Raft) resetElectionTimer() {
	// 选举超时要稍大于心跳超时
	rf.electionTimeout = rf.heartbeatPeriod + rand.Intn(150)
	rf.latestHeardTime = time.Now().UnixNano()
}

// 选举超时
func (rf *Raft) electionTimeoutTick() {
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.latestHeardTime // 纳秒时间单位
			if int(elapseTime/int64(time.Millisecond)) >= rf.electionTimeout {
				rf.electionTimeoutChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// 只有Leader才会发送心跳
func (rf *Raft) heartbeatPeriodTick() {
	for {
		if _, isLeader := rf.GetState(); isLeader == false {

		} else {
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.latestIssueTime
			if int(elapseTime/int64(time.Millisecond)) >= rf.heartbeatPeriod {
				rf.heartbeatPeriodChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) eventLoop() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutChan:
			go rf.startElection()

		case <-rf.heartbeatPeriodChan:
			go rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 设置状态
	rf.switchTo(Candidate)

	// 1. currentTerm 加 1
	rf.CurrentTerm += 1
	rf.VoteFor = rf.me
	nVotes := 1
	rf.resetElectionTimer()

	rf.mu.Unlock()

	// 启动一个协程完成这个投票的整个任务
	go func(nVotes *int, rf *Raft) {
		var wg sync.WaitGroup

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}

			wg.Add(1)
			rf.mu.Lock()
			// 构造 RequestVoteArgs
			lastLogIndex := len(rf.Log) - 1
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  rf.Log[lastLogIndex].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}

			go func(serverId int, rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
				defer wg.Done()
				ok := rf.sendRequestVote(serverId, args, reply)

				if ok == false {
					return
				}

				// 首先判断这个 raft 实例还处不处于 Candidate 的状态，如果不处于后面的逻辑不需要执行
				rf.mu.Lock()
				if rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if reply.VoteGranted == true {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					*nVotes = *nVotes + 1
					// // 这里必须加上判断 rf.state == Candidate,(防止多次进入该函数)
					if *nVotes >= len(rf.peers)/2+1 && rf.state == Candidate {
						rf.switchTo(Leader) // 都加锁进行保护了
						rf.leaderId = rf.me

						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.Log)
						}

						// 发送一次心跳，(在broadcastHeartbeat中还需要进行上锁)
						go rf.broadcastHeartbeat()
					}

				} else {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 被拒绝了的原因可能有：
					// 1. 投票者的term 比选举者的要大
					if reply.Term > rf.CurrentTerm {
						rf.switchTo(Follower)
						rf.VoteFor = -1
						rf.CurrentTerm = reply.Term
						return
					}

				}

			}(i, rf, &args, &reply)

		}

		wg.Wait()
	}(&nVotes, rf)

}

// 一种特殊的 AppendEntryRPC
func (rf *Raft) broadcastHeartbeat() {
	if _, isLeader := rf.GetState(); isLeader == false {
		return
	}

	rf.mu.Lock()
	rf.latestIssueTime = time.Now().UnixNano()

	// 向所有的节点发送一次心跳
	go rf.broadcastAppendEntries(rf.CurrentTerm)

	rf.mu.Unlock()
}

func (rf *Raft) broadcastAppendEntries(leaderTerm int) {
	if _, isLeader := rf.GetState(); isLeader == false {
		return
	}

	var wg sync.WaitGroup

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)

		go func(i int, rf *Raft) {
			defer wg.Done()

			if _, isLeader := rf.GetState(); isLeader == false {
				return
			}

			args := AppendEntryArgs{Term: leaderTerm, LeaderId: rf.me}
			reply := AppendEntryReply{}

			ok := rf.sendAppendEntries(i, &args, &reply)

			// 无法建立通信
			if ok == false {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.CurrentTerm {
				rf.switchTo(Follower)
				rf.CurrentTerm = reply.Term
				rf.VoteFor = -1
			}
			rf.mu.Unlock()

		}(i, rf)

	}
	wg.Wait()
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
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
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntryArgs struct {
	Term         int        // leader's term
	LeaderId     int        // follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 投票者判断是否要投给这个候选者
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm <= args.Term {

		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
			// 如果不是follower，则重置voteFor为-1，以便可以重新投票
			rf.VoteFor = -1
			// 切换到follower状态
			rf.switchTo(Follower)
		}

		// 这里一定有 rf.CurrentTerm == args.Term
		if rf.VoteFor == -1 {
			// 首先要进行一致性检查(lab2A先不管)
			// TODO
			rf.VoteFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			return
		}
	}

	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	// TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm <= args.Term {
		if rf.CurrentTerm < args.Term {
			rf.switchTo(Follower)
			rf.VoteFor = -1
			rf.CurrentTerm = args.Term
			rf.leaderId = args.LeaderId
		}

		reply.Term = rf.CurrentTerm
		reply.Success = true
	} else {
		reply.Term = rf.CurrentTerm
		reply.Success = false
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
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
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

// 这里的 ApplyMsg是什么？

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.leaderId = -1

	rf.heartbeatPeriod = 120
	rf.latestIssueTime = time.Now().UnixNano()
	rf.resetElectionTimer()

	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatPeriodChan = make(chan bool)

	rf.CurrentTerm = 0
	rf.VoteFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.Log = make([]LogEntry, 0)
	rf.Log = append(rf.Log, LogEntry{Term: 0})

	size := len(rf.peers)
	rf.nextIndex = make([]int, size)
	// matchIndex元素的默认初始值即为0
	rf.matchIndex = make([]int, size)

	// 创建 3 个协程处理
	go rf.electionTimeoutTick()
	go rf.heartbeatPeriodTick()
	go rf.eventLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}
