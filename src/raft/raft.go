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
	"fmt"
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

	applyCh chan ApplyMsg // 这个 channel 是用来沟通 raft 实例和 config 的，把 raft 已提交的日志提供给 config

	// 条件变量无非就是协调 electionTimeoutTick() , heartbeatPeriodTick() , applyEntries() 这三个协程的关系
	// 这个是用来唤醒发送心跳信息的协程的
	leaderHeartbeatCond          *sync.Cond
	applyEntriesCond             *sync.Cond
	nonleaderElectionTimeoutCond *sync.Cond
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
	oldState := rf.state
	rf.state = newState
	if oldState == Leader && newState == Follower {
		rf.nonleaderElectionTimeoutCond.Broadcast()
	} else if oldState == Candidate && newState == Leader {
		rf.leaderHeartbeatCond.Broadcast()
	}
}

// 为了避免同时多个 raft 实例选举超时，这个时间需要随机
func (rf *Raft) resetElectionTimer() {
	// 选举超时要稍大于心跳超时
	rf.electionTimeout = 5*rf.heartbeatPeriod + rand.Intn(200)
	rf.latestHeardTime = time.Now().UnixNano()
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

// 选举超时
func (rf *Raft) electionTimeoutTick() {
	for {
		if _, isLeader := rf.GetState(); isLeader == false {
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.latestHeardTime // 纳秒时间单位
			if int(elapseTime/int64(time.Millisecond)) >= rf.electionTimeout {
				rf.electionTimeoutChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		} else {
			// 如果是领导者则没有必要运行这个协程
			rf.mu.Lock()
			rf.nonleaderElectionTimeoutCond.Wait()
			rf.mu.Unlock()
		}
	}
}

// 只有Leader才会发送心跳
func (rf *Raft) heartbeatPeriodTick() {
	for {
		if _, isLeader := rf.GetState(); isLeader == false {
			// 这是个协程在运行，如果该协程的 raft 实例不是一个 leader, 那么这个协程却一直在运行,需要条件变量阻塞
			rf.mu.Lock()
			rf.leaderHeartbeatCond.Wait() // 进入 Wait 函数后释放这把锁
			rf.mu.Unlock()
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
			rf.mu.Lock()
			DPrintf("Server %d start a election at term %d\n", rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			go rf.startElection()

		case <-rf.heartbeatPeriodChan:
			rf.mu.Lock()
			DPrintf("Leader %d send heartbeat, term %d\n", rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			go rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) applyEntries() {
	for {
		// 不断读取已提交的日志条目，把命令写进 channel
		rf.mu.Lock()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex

		// 这里是安全的，并不需要进行加锁
		for i := lastApplied + 1; i <= commitIndex; i++ {
			rf.lastApplied = i

			// fmt.Printf("server %d commit log %s index is %d\n", rf.me, rf.Log[i].Command, i)

			// 记住了，这里是 append rf.Log[i].Command !!!
			appMsg := ApplyMsg{CommandValid: true, Command: rf.Log[i].Command, CommandIndex: i}
			// fmt.Printf("server %d commit log %s index is %d\n logs is %v\n", rf.me, rf.Log[i].Command, i, rf.Log)

			rf.applyCh <- appMsg
		}

		rf.applyEntriesCond.Wait()
		rf.mu.Unlock()
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

	rf.persist()

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

				rf.mu.Lock()
				if rf.CurrentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if reply.VoteGranted == true {
					rf.mu.Lock()
					*nVotes = *nVotes + 1
					// // 这里必须加上判断 rf.state == Candidate,(防止多次进入该函数)
					if *nVotes >= len(rf.peers)/2+1 && rf.state == Candidate {
						// fmt.Printf("%d 获得的票数为 %d, 任期号为 %d\n", rf.me, *nVotes, rf.CurrentTerm)
						rf.switchTo(Leader) // 都加锁进行保护了
						rf.leaderId = rf.me

						for j := 0; j < len(rf.peers); j++ {
							rf.nextIndex[j] = len(rf.Log)
							rf.matchIndex[j] = 0
						}

						// DPrintf("%d win the election, term is %d\n", rf.me, rf.CurrentTerm)
						// fmt.Printf("%d win the election, term is %d\n", rf.me, rf.CurrentTerm)

						// 发送一次心跳，(在broadcastHeartbeat中还需要进行上锁)
						go rf.broadcastHeartbeat()
					}
					rf.mu.Unlock()

				} else {
					rf.mu.Lock()
					// 被拒绝了的原因可能有：
					// 1. 投票者的term 比选举者的要大
					if reply.Term > rf.CurrentTerm {
						rf.VoteFor = -1
						rf.CurrentTerm = reply.Term
						rf.switchTo(Follower)

						rf.persist()

					}
					rf.mu.Unlock()
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
	index := len(rf.Log) - 1
	nAgree := 1
	// 向所有的节点发送一次心跳, 这里的 index 为 Leader 最末尾的 index
	go rf.broadcastAppendEntries(index, rf.CurrentTerm, nAgree, rf.commitIndex)
	rf.mu.Unlock()
}

func (rf *Raft) broadcastAppendEntries(index int, term int, nAgree int, commitIndex int) {
	if _, isLeader := rf.GetState(); isLeader == false {
		return
	}
	var wg sync.WaitGroup

	isAgree := false

	// 新的任期不应该再发送之前任期应该发送的 AppendEntry
	rf.mu.Lock()
	if rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)

		go func(i int, rf *Raft) {
			defer wg.Done()

		retry:
			if _, isLeader := rf.GetState(); isLeader == false {
				return
			}

			// 不应该处理上个任期的 AppendEntriesRPC
			// 在提交的限制中，Leader不能够提交不是当前任期内的日志,只能通过提交当前任期的日志顺带也把之前未提交的日志给提交了
			rf.mu.Lock()
			if rf.CurrentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			rf.mu.Lock()
			nextIndex := rf.nextIndex[i]
			prevLogIndex := nextIndex - 1
			entries := make([]LogEntry, 0)

			if index >= nextIndex {
				// 第一次空日志, 新leader选举出来的时候 nextIndex = len(rf.Log)
				entries = make([]LogEntry, len(rf.Log[nextIndex:index+1]))
				copy(entries, rf.Log[nextIndex:index+1])
			}

			// 因为我们在刚开始创建Raft实例的时候，就会人为添加一条term=0的空日志，所以prevLogIndex最小为 0
			if prevLogIndex < 0 {
				DPrintf("Error: prevLogIndex < 0 , term %d\n", term)
			}
			prevLogTerm := rf.Log[prevLogIndex].Term

			args := AppendEntryArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: commitIndex}
			reply := AppendEntryReply{}
			reply.Success = false

			// fmt.Printf("sendAppendEntries: LeaderId %d to %d, PrevLogIndex %d, len of entries %d\n", rf.me, i, prevLogIndex, len(entries))

			// 为什么这两行代码调换一下位置就不行 ???? 答案是不行 !!!
			rf.mu.Unlock()
			// 这应该是一个长调用(时间开销大),如果调换了位置获取锁的时间就会很长
			ok := rf.sendAppendEntries(i, &args, &reply)

			// 无法建立通信
			if ok == false {
				DPrintf("[Term is %d] Leader %d send AppendEntryRPC to server %d failed\n", term, rf.me, i)
				return
			} else {

				rf.mu.Lock()
				if rf.CurrentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				// 发送 appendentry 成功被 server 接收
				if reply.Success == true {
					rf.mu.Lock()

					if rf.CurrentTerm != args.Term {
						rf.mu.Unlock()
						return
					}

					// 还需要更新 nextIndex[i]
					// 如果 nextIndex[i] == index + 1: 则证明 server i 并不缺少日志
					if rf.nextIndex[i] < index+1 {
						rf.nextIndex[i] = index + 1
						rf.matchIndex[i] = index
					}

					nAgree++

					// 如果(没有传输任何命令)，我们不能写入 ApplyCh
					// 这里必须要加上判断 rf.state == Leader !!!
					if isAgree == false && nAgree >= len(rf.peers)/2+1 && rf.state == Leader {
						// 更新 commitIndex, 并向其他节点发送心跳接受新的 commitIndex
						isAgree = true

						// fmt.Printf("得票：%d, 总机器数:%d\n", nAgree, len(rf.peers))

						// 这个条件说明这条 AppendEntry 中有内容
						if rf.commitIndex < index && rf.Log[index].Term == rf.CurrentTerm {
							// 想一下这段逻辑的一些问题。假设一个 leader 连续两次 Start(cmd)
							// 假设第二个 Start 的 go broadcastAppendEntries() 先达成一致，第一个还没有达成一致
							// 这样的话会更新 rf.commitIndex 为第二次调用 Start 时的 index，
							// 进而唤醒了 applyEntries 协程，出现了错误！！！
							// 不会的！会进行一致性检查
							rf.commitIndex = index

							// 不应该在这里放入 rf.applyMsg 中，这段逻辑应该在相应的协程中才会执行
							// applyMsg := ApplyMsg{CommandValid: true, Command: rf.Log[index], CommandIndex: index}
							// rf.applyCh <- applyMsg
							rf.applyEntriesCond.Broadcast()

							// 有了更新的 commitIndex, 需要告诉所有的 server
							go rf.broadcastHeartbeat()
						}

					}

					rf.mu.Unlock()
					return

				} else {
					// 有可能 server 的 term 更大，此时该 Leader 需要切换为 Follower
					rf.mu.Lock()
					if reply.Term > term {
						rf.switchTo(Follower)
						rf.CurrentTerm = reply.Term
						rf.VoteFor = -1
						// 谁告诉你这里要设置 leaderId 啦！！等到发送心跳才会知道
						// rf.leaderId = i

						rf.persist()

						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					// 一致性检查失败，需要重试
					DPrintf("Leader %d to server %d 一致性检查失败，需要重试:\n", rf.me, i)

					rf.mu.Lock()
					// 下次当选时能够避免从 len(rf.Log)处开始进行一致性检查
					// rf.nextIndex[i] = nextIndex - 1
					// 利用 ConflictFirstIndex 和 ConflictTerm 进行加速
					// nextIndex := rf.getNextIndex(reply, nextIndex)
					// follower ConflictFirstIndex 往后的日志都是错误的
					nextIndex := reply.ConflictFirstIndex

					rf.nextIndex[i] = nextIndex

					rf.mu.Unlock()

					goto retry
				}
			}
		}(i, rf)
	}
	wg.Wait()
}

func (rf *Raft) getNextIndex(reply AppendEntryReply, nextIndex int) int {
	// 这里递减nextIndex使用了论文中提到的优化策略：
	// If desired, the protocol can be optimized to reduce the number of rejected AppendEntries
	// RPCs. For example,  when rejecting an AppendEntries request, the follower can include the
	// term of the conflicting entry and the first index it stores for that term. With this
	// information, the leader can decrement nextIndex to bypass all of the conflicting entries
	// in that term; one AppendEntries RPC will be required for each term with conflicting entries,
	// rather than one RPC per entry.

	// reply's conflictTerm=0，表示None。说明peer:i的log长度小于nextIndex。
	if reply.ConflictTerm == -1 {
		// If it does not find an entry with that term, it should set nextIndex = conflictIndex
		nextIndex = reply.ConflictFirstIndex
	} else {
		// peer:i的prevLogIndex处的任期与leader不等
		// leader搜索它的log确认是否存在等于该任期的entry
		conflictIndex := reply.ConflictFirstIndex
		conflictTerm := rf.Log[conflictIndex].Term
		// 只有conflictTerm大于或等于reply's conflictTerm，才有可能或一定找得到任期相等的entry
		if conflictTerm >= reply.ConflictTerm {
			// 从reply.ConflictFirstIndex处开始向搜索，寻找任期相等的entry
			for i := conflictIndex; i > 0; i-- {
				if rf.Log[i].Term == reply.ConflictTerm {
					break
				}
				conflictIndex -= 1
			}
			// conflictIndex不为0，leader的log中存在同任期的entry
			if conflictIndex != 0 {
				// 向后搜索，使得conflictIndex为最后一个任期等于reply.ConflictTerm的entry
				for i := conflictIndex + 1; i < nextIndex; i++ {
					if rf.Log[i].Term != reply.ConflictTerm {
						break
					}
					conflictIndex += 1
				}
				nextIndex = conflictIndex + 1
			} else { // conflictIndex等于0，说明不存在同任期的entry
				nextIndex = reply.ConflictFirstIndex
			}
		} else {
			// conflictTerm < reply.ConflictTerm，并且必须往前搜索，所以一定找不到任期相等的entry
			// conflictTerm < reply.ConflictTerm,说明 follower 这段日志全部无效
			nextIndex = reply.ConflictFirstIndex
		}

	}
	return nextIndex
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// 论文中有提到 哪些字段需要进行持久化

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// 调用之前必须 已经持有 rf.mu 这把锁
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

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

	// 这里传入的是 raftstate[] bytes
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) == nil && d.Decode(&voteFor) == nil && d.Decode(&logs) == nil {
		rf.CurrentTerm = currentTerm
		rf.VoteFor = voteFor
		rf.Log = logs
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

	// 加速 backtracking 的过程
	ConflictTerm       int
	ConflictFirstIndex int
}

// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if rf.CurrentTerm <= args.Term {
		// 这种情况直接投票给 Candidate
		if rf.CurrentTerm < args.Term {
			// 需要把 rf 切换为 Follower
			rf.switchTo(Follower)
			rf.CurrentTerm = args.Term
			rf.VoteFor = -1

			// 这里不能投票给Candidate !!! 还没有判断日志呢！！！！
			// reply.VoteGranted = true
			// 修改后顺利通过 Test (2B): rejoin of partitioned leader
			rf.persist()
		}

		reply.Term = rf.CurrentTerm

		if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
			// 关于日志复制还需要进行选举限制
			// 收到投票请求的服务器 v 将比较谁的日志更完整
			// 拒绝投票的条件：
			// 1.  Server.term > Candidate.term
			// 2. (Server.term == Candidate.term) && (Server.lastindex > Candidate.lastindex)
			// fmt.Printf("Candidate info: lastLogTerm:%d, lastLogIndex:%d\n",
			//	args.LastLogTerm, args.LastLogIndex)
			// fmt.Printf("Server info: lastLogTerm:%d, lastLogIndex:%d\n\n",
			// rf.Log[len(rf.Log)-1].Term, len(rf.Log)-1)

			if rf.Log[len(rf.Log)-1].Term > args.LastLogTerm {
				// 拒绝投票
				reply.VoteGranted = false
				return
			} else if rf.Log[len(rf.Log)-1].Term == args.LastLogTerm {
				// 拒绝投票
				if len(rf.Log)-1 > args.LastLogIndex {
					reply.VoteGranted = false
					return
				}
			}

			rf.resetElectionTimer()
			rf.VoteFor = args.CandidateId
			// fmt.Printf("%d 在任期 %d 投票给了 %d\n\n\n\n\n", rf.me, rf.CurrentTerm, args.CandidateId)
			rf.switchTo(Follower)
			reply.VoteGranted = true

			rf.persist()

			return
		}

	}

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
}

// 别忘了重置 electiontimeout 计时器
/*
发送心跳 和 发送日志 这两种形式的 AppendEntriesArgs 怎么组织

*/

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm <= args.Term {
		if rf.CurrentTerm < args.Term {
			rf.switchTo(Follower)

			rf.VoteFor = -1
			rf.CurrentTerm = args.Term
			rf.leaderId = args.LeaderId

			rf.persist()
		}

		// 有没有可能现在 rf 也是 Candidate 状态
		rf.switchTo(Follower)
		// 这里必须加上 LeaderId 的设置
		rf.leaderId = args.LeaderId
		reply.Term = rf.CurrentTerm

		rf.resetElectionTimer()

		// 这种情况下需要接受 Leader 发过来的 entries, 首先进行一致性检查
		if len(rf.Log)-1 < args.PrevLogIndex {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.ConflictFirstIndex = len(rf.Log)

			return
		}
		// 有这条日志记录
		if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false

			// 我们要利用 conflictIndex 和 conflictTerm 来加速这一回退的过程
			reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
			reply.ConflictFirstIndex = args.PrevLogIndex

			for i := reply.ConflictFirstIndex - 1; i >= 0; i-- {
				if rf.Log[i].Term != reply.ConflictTerm {
					break
				} else {
					reply.ConflictFirstIndex -= 1
				}
			}

			return
		}

		rf.switchTo(Follower)

		rf.VoteFor = args.LeaderId

		rf.persist()

		// 一致性检查成功，改写日志,
		// 也不一定哦 ！！！
		// 这样直接改日志可能让日志长度变短了(连续两个AppendEntries, 后一个更新的 AppendEntries 先被处理，会被覆盖掉)
		// 这样仍然更新了 rf.CommitIndex,会出现 index out of range 的错误

		// 这里的判断非常重要，不能截断正确的日志
		// 想一下， leader 发送过来的日志肯定都是安全的，可以 append 的
		// 1. 如果 entries 中的日志和 rf.Log中的完全一致匹配，那么就不需要进行覆盖写，覆盖写反而会出错
		tmpId := args.PrevLogIndex + 1
		entriesId := 0
		needAppend := false
		for ; entriesId < len(args.Entries); entriesId++ {
			if tmpId == len(rf.Log) {
				needAppend = true
				break
			}
			if rf.Log[tmpId].Term != args.Entries[entriesId].Term {
				needAppend = true
				break
			}
			tmpId++
		}

		if len(args.Entries) > 0 && needAppend {
			entries := make([]LogEntry, len(args.Entries))
			copy(entries, args.Entries)
			// 不包括 PrevLogIndex + 1 处的元素
			// rf.Log = append(rf.Log[:args.PrevLogIndex+1], entries...) // [0, nextIndex) + entries
			rf.Log = rf.Log[:args.PrevLogIndex+1]
			rf.Log = append(rf.Log, entries...)
			// 打印一下当前的 logs
			// fmt.Printf("server %d, after consistency check %v\n", rf.me, rf.Log)
			rf.persist()
		}

		// 如何更新 commitIndex 呢
		// 1.首先，commitIndex 不能大于 len(rf.Log)
		// prevLogIndex [entries]

		indexOfLastOfNewEntry := args.PrevLogIndex + len(args.Entries)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if rf.commitIndex > indexOfLastOfNewEntry {
				rf.commitIndex = indexOfLastOfNewEntry
			}

			// 更新了commitIndex之后给applyCond条件变量发信号，以应用新提交的entries到状态机
			rf.applyEntriesCond.Broadcast()
		}

		//fmt.Printf("server %d 现在的日志是 %v, 长度为 %d, 应该提交的commitIndex为 %d\n", rf.me, rf.Log, len(rf.Log), rf.commitIndex)

		rf.resetElectionTimer()
		reply.Term = rf.CurrentTerm
		reply.Success = true
		rf.leaderId = args.LeaderId

		// rf.persist()

		return

	} else {
		// 告诉你我才是老大(Leader)
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

/*
lab document : Start() should return immediately, without waiting for the log appends to complete.
*/

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// 调用 broadcastAppendEntries()函数
	if term, isLeader = rf.GetState(); isLeader == true {
		rf.mu.Lock()

		logEntry := LogEntry{Command: command, Term: rf.CurrentTerm}
		rf.Log = append(rf.Log, logEntry)
		index = len(rf.Log) - 1 // 这是需要进行共识Log的下标
		agreeNum := 1           // 获得同意的数目
		rf.latestIssueTime = time.Now().UnixNano()

		rf.persist()

		DPrintf("Leader %d attempt vote for a new entry[%s] index[%d]\n", rf.me, command, index)

		go rf.broadcastAppendEntries(index, rf.CurrentTerm, agreeNum, rf.commitIndex)

		rf.mu.Unlock()
	}
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

func (rf *Raft) debug() {
	for {
		rf.mu.Lock()
		fmt.Printf("id:%d \t term:%d \t state:%s \n", rf.me, rf.CurrentTerm, state2name(rf.state))
		rf.mu.Unlock()
		time.Sleep(1000 * time.Millisecond)
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

	rf.applyCh = applyCh

	rf.leaderHeartbeatCond = sync.NewCond(&rf.mu)
	rf.nonleaderElectionTimeoutCond = sync.NewCond(&rf.mu)
	rf.applyEntriesCond = sync.NewCond(&rf.mu)

	// 创建 3 个协程处理
	go rf.electionTimeoutTick()
	go rf.heartbeatPeriodTick()
	go rf.applyEntries()

	// go rf.debug()

	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	//rf.mu.Lock()
	//rf.persist()
	//rf.mu.Unlock()

	go rf.eventLoop()

	// initialize from state persisted before a crash

	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}
