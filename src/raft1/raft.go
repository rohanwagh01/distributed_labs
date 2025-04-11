package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.

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
	Snap        SnapShot   //

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

type SnapShot struct {
	Snap      []byte //
	LastTerm  int    //
	LastIndex int    //
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////       Struct Handlers         ///////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (rf *Raft) logLength() int {
	return len(rf.Log) + rf.Snap.LastIndex
}

func (rf *Raft) get(i int) *LogEntry {
	index := i - rf.Snap.LastIndex
	if index < 0 || index >= len(rf.Log) {
		////fmt.Println(rf.me, "improper log access with index: ", i, "snapshot size: ", rf.Snap.LastIndex, " and log length: ", len(rf.Log))
		////fmt.Println(rf.Log)
		////fmt.Println(rf.Snap.LastIndex)
		return nil
	}
	return &rf.Log[index]
}

///////////////////////s/////////////////////////////////////////////////////////////////////////////////////
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
	//lastLogTerm := rf.Log[len(rf.Log)-1].TermAdded
	//candidateLogUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.Log)-1)
	lastLogTerm := rf.get(rf.logLength() - 1).TermAdded
	candidateLogUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.logLength()-1)

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && candidateLogUpToDate {

		rf.updateState(rf.CurrentTerm, args.CandidateID, rf.Log)
		rf.timeLastMSG = time.Now()

		reply.VoteGranted = true
		////fmt.Println("    ", rf.me, "has voted for", args.CandidateID, rf.Log, args.LastLogIndex, args.LastLogTerm)
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
	//////fmt.Println(rf.me, "got a request", args)

	switch {
	// check if short log
	case args.PrevLogIndex >= rf.logLength():
		////fmt.Println("short log", rf.me)
		reply.XLogTerm = -1
		reply.XLogLength = rf.logLength()
		reply.Success = false

	//check if trying to reach back into snapshot, point it to the end of snapshot to see if a install snapshot rpc is needed
	case args.PrevLogIndex < rf.Snap.LastIndex:
		reply.XLogTerm = -1
		reply.XLogLength = rf.Snap.LastIndex + 1
		reply.Success = false

	// logs match
	//case rf.Log[args.PrevLogIndex].TermAdded == args.PrevLogTerm:
	case rf.get(args.PrevLogIndex).TermAdded == args.PrevLogTerm:

		// append and new entries not already in log from prevlogindex onwards
		rf.updateState(rf.CurrentTerm, rf.VotedFor, append(rf.Log[:args.PrevLogIndex+1-rf.Snap.LastIndex], args.Entries...))

		// update commit if the leader commit is larger
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.logLength()-1)
		}

		reply.Success = true

		//go rf.updateClient()

	// mismatch at this spot

	//if at the end of the snapshot and mismatch, need a install snapshot RPC
	case args.PrevLogIndex == rf.Snap.LastIndex:
		reply.XLogTerm = -2
		reply.XLogLength = -2
		reply.Success = false

	default:
		////fmt.Println("incorrect log", rf.me)
		//reply.XLogTerm = rf.Log[args.PrevLogIndex].TermAdded

		if rf.Snap.LastTerm >= rf.get(args.PrevLogIndex).TermAdded { //same or greater term, so jump to the end of the snapshot and is mismatch, then request a install rpc
			reply.XLogTerm = -1
			reply.XLogLength = rf.Snap.LastIndex + 1
			reply.Success = false
		} else {
			reply.XLogTerm = rf.get(args.PrevLogIndex).TermAdded
			reply.Success = false
			reply.Term = rf.CurrentTerm
		}

		// delete excess log
		rf.updateState(rf.CurrentTerm, rf.VotedFor, rf.Log[:args.PrevLogIndex-rf.Snap.LastIndex])
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.me, "recieved snapshot from", args.LeaderID, "up to ", args.LastIncludedIndex)

	// abort if the response if outdated, convert to undecided follower if the term is larger
	if !rf.checkResponse(args) {
		return
	}
	if args.LastIncludedIndex < rf.Snap.LastIndex { //ignore if trying to 'go back'
		return
	}
	////fmt.Println(rf.me, "installing snapshot from: ", args.LeaderID, "at: ", args.LastIncludedIndex)

	rf.updateSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)

	rf.lastApplied = 0
	rf.commitIndex = args.LastIncludedIndex
	//update client
	reply.Term = rf.CurrentTerm

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

	votes := 1
	rf.mu.Unlock()
	// request a vote from each peer
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.handleVote(i, &votes, electionTerm)
		}
	}
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
func (rf *Raft) handleVote(server int, votes *int, electionTerm int) {
	rf.mu.Lock()

	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	args.CandidateID = rf.me
	args.Term = rf.CurrentTerm
	args.LastLogIndex = rf.logLength() - 1
	args.LastLogTerm = rf.get(args.LastLogIndex).TermAdded

	// send requests until successful
	rf.mu.Unlock()
	ok := rf.sendTCP(server, "Raft.RequestVote", &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && reply.VoteGranted && rf.status == CANDIDATE && rf.CurrentTerm == electionTerm {
		*votes += 1

		//majority
		if *votes > len(rf.peers)/2 {
			rf.status = LEADER
			////fmt.Println(rf.me, "is leader")
			for i := range rf.peers {
				rf.nextIndex[i] = rf.logLength()
				rf.matchIndex[i] = 0
			}
			go rf.heartBeats(rf.CurrentTerm)
		}
	}
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
	////fmt.Println("   ", rf.me, "added command to log: ", rf.Log)

	entryIndex := rf.logLength() - 1

	////fmt.Println(rf.me, "sending command", len(rf.Log))

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

		if entryIndex < rf.logLength() {
			rf.commitIndex = max(entryIndex, rf.commitIndex)
		}
		////fmt.Println("Acheived Majority: ", rf.lastApplied, rf.commitIndex)
		//go rf.updateClient()
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

	if rf.nextIndex[server] > rf.logLength() {
		////fmt.Println("next index got too large")
		rf.nextIndex[server] = rf.logLength()
	} else if rf.nextIndex[server] <= rf.Snap.LastIndex { //we don't have this available
		////fmt.Println(rf.me, "sending to ", server, "a request to install snapshot with a nextInd:", rf.nextIndex[server], " and a snapshot last index of ", rf.Snap.LastIndex)
		ok := rf.sendInstallSnapshot(server)
		responseChannel <- ok
		return
	}

	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.get(rf.nextIndex[server] - 1).TermAdded,
		Entries:      rf.Log[rf.nextIndex[server]-rf.Snap.LastIndex:], //XXX seems like entries can grow larger than the size of the log
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
			rf.nextIndex[server] = rf.logLength()
			rf.matchIndex[server] = rf.logLength() - 1
			responseChannel <- reply.Success
			return
		}

		if reply.Term == -1 && reply.XLogLength > 0 { // responder log is short
			rf.nextIndex[server] = reply.XLogLength // cannot step before log XXX check here if the next index could be growing larger
		} else if reply.Term == -2 { //install snapshot requested
			ok := rf.sendInstallSnapshot(server)
			responseChannel <- ok
			return
		} else {
			// find the first entry of highest term < XTerm
			nextIndex := max(rf.nextIndex[server]-1, 1)
			for i := rf.nextIndex[server] - 1; i > rf.Snap.LastIndex && rf.get(i).TermAdded >= reply.XLogTerm; i-- {
				nextIndex = i
			}
			rf.nextIndex[server] = nextIndex
		}
		if rf.nextIndex[server] <= rf.Snap.LastIndex { //we don't have this available
			ok := rf.sendInstallSnapshot(server)
			responseChannel <- ok
			return
		}
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.get(rf.nextIndex[server] - 1).TermAdded
		args.Entries = rf.Log[rf.nextIndex[server]-rf.Snap.LastIndex:]
	}
}

func (rf *Raft) sendInstallSnapshot(server int) bool {
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.Snap.LastIndex,
		LastIncludedTerm:  rf.Snap.LastTerm,
		Data:              rf.Snap.Snap,
	}

	reply := InstallSnapshotReply{}

	rf.mu.Unlock()
	ok := rf.sendTCP(server, "Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()
	//if it is still the term
	////fmt.Println(rf.me, "received that", server, "installed the snapshot with last index:", args.LastIncludedIndex)
	if ok && rf.status == LEADER && args.Term == rf.CurrentTerm && !rf.killed() {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	return ok
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
		////fmt.Println(rf.me, "SENDING HEARTBEAT for term:", rf.CurrentTerm)
		go rf.applyFuncUntilMajority(rf.handleCommandUpdate, rf.abortCommandSend)

		// sleep without blocking
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
	}

	////fmt.Println(rf.me, "stopping heartbeats as demoted")
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
	e.Encode(rf.Snap.LastIndex)
	e.Encode(rf.Snap.LastTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.Snap.Snap)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
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
	var lastIndex int
	var lastTerm int
	resT := d.Decode(&term)
	resV := d.Decode(&votedFor)
	resL := d.Decode(&log)
	reslI := d.Decode(&lastIndex)
	reslT := d.Decode(&lastTerm)
	if resT != nil || resV != nil || resL != nil || reslI != nil || reslT != nil {
		////fmt.Println("Error in Load: Buffer Mismatch")
	} else {
		rf.updateState(term, votedFor, log)
		rf.Snap.LastIndex = lastIndex
		rf.Snap.LastTerm = lastTerm
		//fmt.Println("reading snapshot")
		rf.Snap.Snap = snapshot
		//fmt.Println("success")
		//update last applied and commit to reflect snapshot (which is committed), they now both point to the 'zero' in the log still
		rf.commitIndex = lastIndex
		rf.lastApplied = 0
		////fmt.Println(rf.me, " Loading Persist with ", rf.Log)
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

// XXX make this a routine and have some way to send a command to send down the channel to send it out
func (rf *Raft) updateClient() {

	for !rf.killed() {
		rf.mu.Lock()
		//check for snapshot that needs to be sent
		if rf.lastApplied < rf.Snap.LastIndex {
			msg := raftapi.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.Snap.Snap,
				SnapshotTerm:  rf.Snap.LastTerm,
				SnapshotIndex: rf.Snap.LastIndex,
			}
			rf.lastApplied = rf.Snap.LastIndex
			rf.mu.Unlock()
			rf.channel <- msg
		} else if rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.logLength()-1 {
			rf.lastApplied += 1
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.get(rf.lastApplied).LogContent,
				CommandIndex: rf.lastApplied, // logs are 1 indexed
			}
			rf.mu.Unlock()
			rf.channel <- msg
			//fmt.Println(rf.me, "sending", rf.lastApplied, "to client")
		} else {
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
			////fmt.Println(rf.me, "has started an election", rf.CurrentTerm+1, " as a", rf.status)
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

func (r *RequestVoteArgs) GetTerm() int      { return r.Term }
func (r *RequestVoteReply) GetTerm() int     { return r.Term }
func (a *AppendEntriesArgs) GetTerm() int    { return a.Term }
func (a *AppendEntriesReply) GetTerm() int   { return a.Term }
func (a *InstallSnapshotArgs) GetTerm() int  { return a.Term }
func (a *InstallSnapshotReply) GetTerm() int { return a.Term }

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

// remake to just be something that is handled in each responder, use a pointer to an int protected by the raft lock and have each handler do the end conditions.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	////fmt.Println(rf.me, "starting snapshot at: ", index, "with log: ", rf.Log, "and previous snapshot: ", rf.Snap.LastIndex)

	term := rf.get(index).TermAdded
	rf.updateSnapshot(index, term, snapshot)

	rf.lastApplied = max(rf.lastApplied, index)
	rf.commitIndex = max(rf.commitIndex, index)
	////fmt.Println(rf.me, "ended snapshot at: ", index, "with last index: ", rf.Snap.LastIndex, "and log: ", rf.Log)
}

func (rf *Raft) updateSnapshot(lastIndex int, lastTerm int, snapshot []byte) { //3D
	//compact log: if index in log, then split. otherwise reset
	if lastIndex < rf.logLength() && lastIndex >= rf.Snap.LastIndex && lastTerm == rf.get(lastIndex).TermAdded {
		//include index as the new 'zero' spot and trim log
		rf.updateState(rf.CurrentTerm, rf.VotedFor, rf.Log[lastIndex-rf.Snap.LastIndex:])
	} else { //snapshot larger than log
		zeroEntry := LogEntry{TermAdded: lastTerm}
		var log []LogEntry
		rf.updateState(rf.CurrentTerm, rf.VotedFor, append(log, zeroEntry))
	}
	rf.Snap.LastIndex = lastIndex
	rf.Snap.LastTerm = lastTerm
	rf.Snap.Snap = snapshot
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
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.updateClient()

	return rf
}
