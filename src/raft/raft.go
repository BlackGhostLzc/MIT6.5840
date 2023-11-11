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

type Status int

const (
	Follower   Status = iota
	Candidator
	Leader
)

type LogEntry struct {
	Term	int        // 当前任期
}


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
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int              // 当前的任期
	votedFor      int
	logs          []LogEntry       
	commitIndex   int              // index of highest log entry known to be commited
	lastApplied   int              // idnex of highest log entry applied to state machine
	nextIndex     []int
	matchIndex    []int

	status 	      Status           // Follower , Candidator, Leader
	overtime      time.Duration    // 超时时间 Leader不需要使用; 
	                               // Follower用作没有收到心跳的超时时间; Candidator用做选举超时时间->下一阶段选举 term++
	
	Electiontimer          *time.Ticker     
	Heartbeatimer          *time.Ticker		

	heartbeatTime          time.Duration    // Leader每隔heartbeat时间发送一次心跳给各个follower, candidator ,这个时间固定的
	electionoverTime       time.Duration

	electionTimeoutChan	chan bool	// 写入electionTimeoutChan意味着可以发起一次选举
	heartbeatPeriodChan	chan bool	// 写入heartbeatPeriodChan意味leader需要向其他peers发送一次心跳
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	isleader = false
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == Leader {
		isleader = true
	}
	term = rf.currentTerm

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
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted     bool
}

type AppendEntriesArgs struct {
	Term 			int			// leader's term
	LeaderId		int			// so follower can redirect clients
	PrevLogIndex	int			// index of Log entry immediately preceding new ones
	PrevLogTerm		int			// term of PrevLogIndex entry
	Entries			[]LogEntry	// Log entries to store(empty for heartbeat; may send
	// more than one for efficiency)
	LeaderCommit	int			// leader's commitIndex
}

type AppendEntriesReply struct {
	Term 			    int			// CurrentTerm, for leader to update itself
	Success			    bool		// true if follower contained entry matching
	// prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else if rf.currentTerm == args.Term {
		if rf.votedFor == -1 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}else {
			// 已经投了票了
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}else { 
		// 自己的任期比申请投票者的任期号要小
		// 把自己切换为 Follower
		rf.currentTerm = args.Term
		rf.status = Follower

		// 重置超时时间

		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}

	rf.mu.Unlock()
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

// 在这里改一下返回值类型 bool->void 
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	
	// 判断 voteNum 是否达到半数
	if !ok {
		return 
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1.被杀死了
	// 2.这说明可能是candidator收到了来自更高任期leader的心跳，从而会更新自己的任期，旧任期的选举都直接退出
	// 3.如果返回的任期号大于候选者自己的任期号，应该什么也不要做。要收到领导人心跳才会把自己退化为 Follower
	if (rf.killed() || rf.currentTerm > args.Term || reply.Term != rf.currentTerm) {
		return   
	}
	
	if reply.Term == rf.currentTerm && reply.VoteGranted == true {
		*voteNum += 1
	}

	if *voteNum >= len(rf.peers)/2 + 1 {
		// 成功当选
		rf.status = Leader
		go rf.BroadcastHeartBeat()
		rf.Heartbeatimer.Reset(rf.heartbeatTime)
	}

	// rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	if !ok {
		return
	}
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		// 我要把自己的状态切换为 Follower,更新任期
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.electionoverTime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.Electiontimer.Reset(rf.electionoverTime)
	} else if args.Term == rf.currentTerm {
		rf.electionoverTime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.Electiontimer.Reset(rf.electionoverTime)
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

func (rf *Raft) StartElection() {
	rf.mu.Lock()    // 不会死锁，因为这是另一个协程
	// 需要把当前任期加 1
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	voteNum := 1
	// 开启多个协程拉取投票
	for i:=0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term         : rf.currentTerm,
			CandidateId  : rf.me,
			LastLogIndex : len(rf.logs) - 1,
			LastLogTerm  : 0,
		}
		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply, &voteNum)
	}
	rf.mu.Unlock()
}

func (rf *Raft) BroadcastHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 发送心跳 
	// 发送AppendEntriesArgs但不带日志条目
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		
		args := AppendEntriesArgs{
			Term      :  	rf.currentTerm ,	// leader's term
			LeaderId  :  	rf.me,  			// so follower can redirect clients
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}


func (rf *Raft) ticker() {
	rf.Heartbeatimer = time.NewTicker(rf.heartbeatTime)
	rf.Electiontimer = time.NewTicker(rf.electionoverTime)    

	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.Electiontimer.C:
			// 开始选举,但如果是 leader 直接跳过
			rf.mu.Lock()
			if rf.status != Leader {
				go rf.StartElection()
				rf.electionoverTime = time.Duration(150+rand.Intn(200)) * time.Millisecond
				rf.Electiontimer.Reset(rf.electionoverTime)
			}
			rf.mu.Unlock()

		case <-rf.Heartbeatimer.C:
			// 向所有人发送心跳
			rf.mu.Lock()
			if rf.status == Leader {
				go rf.BroadcastHeartBeat()
				rf.Heartbeatimer.Reset(rf.heartbeatTime)
			}
			rf.mu.Unlock()

		}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化
	/*
	status 	      Status           // Follower , Candidator, Leader
	overtime      time.Duration    // 超时时间
	timer         *time.Ticker     
	*/
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower

	rf.heartbeatTime = 100 * time.Millisecond
	rf.electionoverTime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生150-350ms

	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatPeriodChan = make(chan bool)
	 
	//rf.Heartbeatimer = time.NewTicker(rf.heartbeatTime)
	//rf.Electiontimer = time.NewTicker(rf.electionoverTime)    

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
