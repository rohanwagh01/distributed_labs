package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//  "bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//  "6.5840/labgob"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////                                                                                          /////////
/////////                                       R  A  F  T                                         /////////
/////////                                                                                          /////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ELECTIONTIMEOUTMIN int = 1000 //msec
const ELECTIONTIMEOUTMAX int = 3000 //msec

type Status int

const (
	FOLLOWER  Status = 0
	CANDIDATE Status = 1
	LEADER    Status = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	channel   chan raftapi.ApplyMsg

	status      Status     // one of either LEADER, FOLLOWER, CANDIDATE
	CurrentTerm int        // the term that this instance thinks is its in
	VotedFor    int        // peer that secureed vote for the term listed in currentTerm
	Log         []LogEntry //

	commitIndex int       // index of the higher log entry known to be committed
	lastApplied int       // index of highest log entry applied to state machine
	nextIndex   []int     // for each server, index of the next log entry to send to that server
	matchIndex  []int     // for each server, index of highest log entry known to be replicated on server
	timeLastMSG time.Time // update every time a appendEntries is recieved or a vote is granted
}

type LogEntry struct {
	TermAdded  int
	LogContent interface{}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////        RPCs      ///////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XLogTerm   int
	XLogLength int
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////       RPC Handlers         //////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
MUST RELEASE LOCK TO USE
Responds to a request for a vote. First checks the RPC message to see if outdated. Votes for sender if log up to date, and vote is available.

	Inputs:
		- args: the RPC message from the peer
		- reply: the RPC message space to place response
	Mutability: updates rf.VotedFor, timeLastMDG, voteGranted, term, and status. Converts to follower if outdated.
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// abort if the response if outdated, convert to undecided follower if the term is larger
	if !rf.checkResponse(args) {
		reply.Term = rf.CurrentTerm
		return
	}

	// vote for the incoming request if possible, otherwise ignore  |  possible if vote is available and candidate has an up to date log or if already voted for the candidate
	lastLogTerm := rf.Log[len(rf.Log)-1].TermAdded
	candidateLogUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.Log)-1)

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && candidateLogUpToDate {

		rf.updateState(rf.CurrentTerm, args.CandidateID, rf.Log)
		rf.timeLastMSG = time.Now()

		reply.VoteGranted = true
		//fmt.Println("    ", rf.me, "has voted for", args.CandidateID, rf.Log, args.LastLogIndex, args.LastLogTerm)
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
}

/*
MUST RELEASE LOCK TO USE
Responds to an append entries request. Ignores if message out of date. Checks if the log matches at the index before entries and if not responds with the next spot to check.

	If it matches, update log with contents and updates client if commitIndex is larger.

	Inputs:
		- args: the RPC message from the peer
		- reply: the RPC message space to place response
	Mutability: updates rf.VotedFor, timeLastMDG, voteGranted, term, and status. Converts to follower if outdated.
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.timeLastMSG = time.Now()
	reply.Term = rf.CurrentTerm

	// abort if the response if outdated, convert to undecided follower if the term is larger
	if !rf.checkResponse(args) {
		return
	}

	rf.status = FOLLOWER
	switch {
	// check if short log
	case args.PrevLogIndex >= len(rf.Log):
		//fmt.Println("short log", rf.me)
		reply.XLogTerm = -1
		reply.XLogLength = len(rf.Log)
		reply.Success = false

	// logs match
	case rf.Log[args.PrevLogIndex].TermAdded == args.PrevLogTerm:

		// append and new entries not already in log from prevlogindex onwards
		rf.updateState(rf.CurrentTerm, rf.VotedFor, append(rf.Log[:args.PrevLogIndex+1], args.Entries...))
		if len(args.Entries) > 0 {
			//fmt.Println(rf.me, " added command to log: ", rf.Log)
		}

		// update commit if the leader commit is larger
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
		}

		rf.updateClient()

		reply.Success = true

	// mismatch at this spot
	default:
		//fmt.Println("incorrect log", rf.me)
		reply.XLogTerm = rf.Log[args.PrevLogIndex].TermAdded

		reply.Success = false
		reply.Term = rf.CurrentTerm

		// delete excess log
		rf.updateState(rf.CurrentTerm, rf.VotedFor, rf.Log[:args.PrevLogIndex])
	}
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////
// ////////////////////////////////////      Running Elections        //////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
MUST RELEASE LOCK TO USE
Start an election process. Updates to candidate and requests a vote from each peer. If majority votes for the server, upgrades to leader.

	Mutability: Reverts to follower if outdated, updates status, nextIndex, matchIndex, timeLastMSG, votedFor, status, currentTerm
*/
func (rf *Raft) startElection() {
	rf.mu.Lock()

	// increase the term and promote to candidate
	rf.status = CANDIDATE
	rf.updateState(rf.CurrentTerm+1, rf.me, rf.Log)
	electionTerm := rf.CurrentTerm

	// start election and timer
	rf.timeLastMSG = time.Now()

	// unblock for handleVote routines
	rf.mu.Unlock()

	majority := rf.applyFuncUntilMajority(rf.handleVote, rf.abortElection)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if majority && rf.CurrentTerm == electionTerm && rf.status == CANDIDATE {
		rf.status = LEADER
		//fmt.Println(rf.me, "is leader")
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.Log)
			rf.matchIndex[i] = 0
		}
		go rf.heartBeats(rf.CurrentTerm)
	}
}

// MUST HOLD LOCK TO USE
func (rf *Raft) abortElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.status != CANDIDATE || rf.killed()
}

/*
MUST RELEASE LOCK TO USE
Sends a requestVote RPC to a peer and waits for response. Sends down the vote down the channel if they voted for this server.

	Inputs:
		- server: the peer to contact
		- responseChannel: sends down true when it recieves a vote
		- wg: updates waitgroup once the function completes
	Mutability: Reverts to follower if out of term.
*/
func (rf *Raft) handleVote(server int, responseChannel chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	args.CandidateID = rf.me
	args.Term = rf.CurrentTerm
	args.LastLogIndex = len(rf.Log) - 1
	args.LastLogTerm = rf.Log[args.LastLogIndex].TermAdded

	// send requests until successful
	rf.mu.Unlock()
	ok := rf.sendTCP(server, "Raft.RequestVote", &args, &reply)
	rf.mu.Lock()

	responseChannel <- ok && reply.VoteGranted && rf.status == CANDIDATE

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////       Leader Appends       //////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
MUST RELEASE LOCK TO USE
Updates the log with the requested command and starts the agreement process for all followers. Returns without waiting

	Inputs:
		- command interface
	Mutability: Reverts to follower if out of date. Updates nextIndex, matchIndex, and commitIndex.
	Returns:
		- entryIndex: the index that this log is placed in
		- currentTerm: the term that this server is in
		- true/false: if the server still believes it is the leader
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) { //3B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != LEADER {
		return -1, -1, false
	}

	newEntry := LogEntry{TermAdded: rf.CurrentTerm, LogContent: command}
	//newLog := append(rf.Log, newEntry)
	rf.updateState(rf.CurrentTerm, rf.VotedFor, append(rf.Log, newEntry))
	//fmt.Println("   ", rf.me, "added command to log: ", rf.Log)

	entryIndex := len(rf.Log) - 1

	//fmt.Println(rf.me, "sending command", len(rf.Log))

	go rf.runAgreement(entryIndex, rf.CurrentTerm)

	return entryIndex, rf.CurrentTerm, true
}

/*
MUST RELEASE LOCK AND BE LEADER TO USE
Updates all followers about a change to the log. If majority responds that they updated, it commits the message and increases the commitIndex for this LEADER.

	Inputs:
		- entryIndex: The index of this entry for the log.
	Mutability: Reverts to follower if out of date. Updates nextIndex, matchIndex, and commitIndex.
*/
func (rf *Raft) runAgreement(entryIndex int, agreementTerm int) {

	majority := rf.applyFuncUntilMajority(rf.handleCommandUpdate, rf.abortCommandSend)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if majority && rf.CurrentTerm == agreementTerm && rf.status == LEADER && !rf.killed() {
		//fmt.Println("Acheived Majority")

		rf.commitIndex = max(entryIndex, rf.commitIndex)
		rf.updateClient()
	}
}

// MUST HOLD LOCK TO USE
func (rf *Raft) abortCommandSend() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.status != LEADER || rf.killed()
}

/*
MUST RELEASE LOCK AND BE LEADER TO USE
Sends out an Append Entries Message to server and waits for response. Upon response, resends if the follower needs more updates.

	Inputs:
		- server: peer to contact
		- responseChannel: sends true down the line if the peer accepted the update
		- wg: wait group to update once the function completes
	Mutability: Reverts to follower if out of date. Updates nextIndex and matchIndex
*/
func (rf *Raft) handleCommandUpdate(server int, responseChannel chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.Log[rf.nextIndex[server]-1].TermAdded,
		Entries:      rf.Log[rf.nextIndex[server]:], //XXX seems like entries can grow larger than the size of the log
		LeaderCommit: rf.commitIndex,
	}

	reply := AppendEntriesReply{}

	// send requests until successful, run blocking, need response
	for {
		rf.mu.Unlock()
		ok := rf.sendTCP(server, "Raft.AppendEntries", &args, &reply)
		rf.mu.Lock()

		if !ok || rf.status != LEADER || args.Term != rf.CurrentTerm || rf.killed() {
			return
		} else if reply.Success {
			rf.nextIndex[server] = len(rf.Log)
			rf.matchIndex[server] = len(rf.Log) - 1
			responseChannel <- reply.Success
			return
		}

		if reply.Term == -1 && reply.XLogLength > 0 { // responder log is short
			rf.nextIndex[server] = reply.XLogLength // cannot step before log XXX check here if the next index could be growing larger
		} else {
			// find the first entry of highest term < XTerm
			nextIndex := max(rf.nextIndex[server]-1, 1)
			for i := rf.nextIndex[server] - 1; i > 0 && rf.Log[i].TermAdded >= reply.XLogTerm; i-- {
				nextIndex = i
			}
			rf.nextIndex[server] = nextIndex
		}
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.Log[rf.nextIndex[server]-1].TermAdded
		args.Entries = rf.Log[rf.nextIndex[server]:]
	}
}

/*
MUST RELEASE LOCK TO USE
Sends out heartbeats every 100 msec while LEADER

	Input:
		- heartBeatTerm: the term to send out heartbeats for, allows heartbeats to stop if the same server is quickly reelected
	Mutability: Reverts Raft member to Follower if the incoming message is a higher term
*/
func (rf *Raft) heartBeats(heartBeatTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() && rf.status == LEADER && rf.CurrentTerm == heartBeatTerm {
		//fmt.Println(rf.me, "SENDING HEARTBEAT for term:", rf.CurrentTerm)
		go rf.applyFuncUntilMajority(rf.handleCommandUpdate, rf.abortCommandSend)

		// sleep without blocking
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
	}

	//fmt.Println(rf.me, "stopping heartbeats as demoted")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////         Persister          //////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(data) < 1 {
		zeroEntry := LogEntry{TermAdded: 0}
		rf.updateState(0, -1, append(rf.Log, zeroEntry))
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	resT := d.Decode(&term)
	resV := d.Decode(&votedFor)
	resL := d.Decode(&log)
	if resT != nil || resV != nil || resL != nil {
		//fmt.Println("Error in Load: Buffer Mismatch")
	} else {
		rf.updateState(term, votedFor, log)
		//fmt.Println(rf.me, " Loading Persist with ", rf.Log)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////        Raft Updates        /////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
MUST HOLD LOCK TO USE
Updates the client of all log contents from after last applied to the commit index. If lastApplied is the same a commit index does not update with any content.

	Mutability: updates lastApplied if logs committed
*/
func (rf *Raft) updateClient() {
	//apply the changes to our client
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[i].LogContent,
			CommandIndex: i, // logs are 1 indexed
		}
		rf.channel <- msg
		//fmt.Println("       ", rf.me, "sending commands to client: ", i)
		rf.lastApplied = i
	}
}

func (rf *Raft) updateState(term int, votedFor int, log []LogEntry) {
	rf.CurrentTerm = term
	rf.VotedFor = votedFor
	rf.Log = log
	rf.persist()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.status == LEADER
}

// The tester calls Kill() when it is done with a Raft instance (e.g.,
// when it simulates a crash and restarts the peer or when the test is
// done).  Kill allows your implementation (1) to close the applyCh so
// that the application on top of Raft can clean up, and (2) to return
// out of long-running goroutines.
//
// Long-running goroutines use memory and may chew up CPU time,
// perhaps causing later tests to fail and generating confusing debug
// output. any goroutine with a long-running loop should call killed()
// to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
MUST RELEASE LOCK TO USE
Starts a timeout ticker that wakes up some random time between the ELECTIONTIMEOUT parameters. If no contact from a leader or election step in the timeout, then starts an election.

	Mutability: all updates from startElection
*/
func (rf *Raft) ticker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		//wait for a random amount of time
		ms := int64(ELECTIONTIMEOUTMIN) + (rand.Int63() % int64(ELECTIONTIMEOUTMAX-ELECTIONTIMEOUTMIN))
		sleepStart := time.Now()

		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()

		if !rf.timeLastMSG.After(sleepStart) && rf.status != LEADER {
			//fmt.Println(rf.me, "has started an election", rf.CurrentTerm+1, " as a", rf.status)
			go rf.startElection() //begin a new election
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////       Network Comm         //////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AllowedParams interface {
	GetTerm() int
}

func (r *RequestVoteArgs) GetTerm() int    { return r.Term }
func (r *RequestVoteReply) GetTerm() int   { return r.Term }
func (a *AppendEntriesArgs) GetTerm() int  { return a.Term }
func (a *AppendEntriesReply) GetTerm() int { return a.Term }

/*
MUST HOLD LOCK TO USE
Checks an incoming message from RPC and returns false if the msg should be ignored.

	Inputs:
		- rpcMsg: Message to check
	Mutability: Reverts Raft member to Follower if the incoming message is a higher term
	Returns:
		- True if the message should still be viewed by Raft
		- False if the message should not be viewed by Raft
*/
func (rf *Raft) checkResponse(rpcMsg AllowedParams) bool {
	//ignore if the reply is from of a previous term
	switch {
	case rpcMsg.GetTerm() < rf.CurrentTerm:
		return false
	case rpcMsg.GetTerm() > rf.CurrentTerm:
		// we are behind in term: update term
		rf.updateState(rpcMsg.GetTerm(), -1, rf.Log)
		rf.status = FOLLOWER
	}
	return true
}

// both functions are BLOCKING

/*
MUST RELEASE LOCK TO USE
Send out an RPC message to a peer. Waits for the response message (ensures response). Returns false if the msg should be ignored.

	Inputs:
		- server: ID of the server to send the message
		- endpoint: the response handler to send the message to
		- args: pointer to the args RPC struct that holds the msg
		- reply: pointer to the reply RPC struct that should hold the response
	Mutability: Reverts Raft member to Follower if the incoming message is a higher term
	Returns:
		- True if the message should still be viewed by Raft
		- False if the message should not be viewed by Raft
*/
func (rf *Raft) sendTCP(server int, endpoint string, args AllowedParams, reply AllowedParams) bool {

	ok := false
	for !ok && !rf.killed() { // repeat indefinitely until response
		ok = rf.peers[server].Call(endpoint, args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.checkResponse(reply)
}

/*
MUST RELEASE LOCK TO USE
Applies a given function for each server peer except for the sender server. Waits for either abort, all responses, or a majority of the responses to return true through channel.

	Inputs:
		- responseHandler: function that takes in a server ID, bool channel, and WaitGroup Sync
								sends a true or false through the channel and updates WaitGroup
		- abort: function that returns true if the function should
	Mutability: May be mutable depending on responseHandler/abort
	Returns:
		- true if majority of the functions responded with true
		- false if the majority was not acheived or if the function aborted early
*/
func (rf *Raft) applyFuncUntilMajority(responseHandler func(int, chan bool, *sync.WaitGroup), abort func() bool) bool {
	responseCh := make(chan bool, len(rf.peers))
	voteCount := 1
	var resChWG sync.WaitGroup
	resChWG.Add(len(rf.peers) - 1)

	rf.mu.Lock()
	// request a vote from each peer
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go responseHandler(i, responseCh, &resChWG)
		}
	}

	// unblock for function routines
	rf.mu.Unlock()

	go func() {
		resChWG.Wait()
		close(responseCh)
	}()

	// collect votes until abort condition is satisfied
	for !abort() {
		if <-responseCh {
			voteCount += 1
		}
		if voteCount > len(rf.peers)/2 {
			return true
		}
	}
	return false

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////         Snapshot           //////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) { //3D
	// Your code here (3D).

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
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.channel = applyCh

	rf.status = FOLLOWER

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeLastMSG = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
