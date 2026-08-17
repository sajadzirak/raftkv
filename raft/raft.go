package raft

import (
	"math/rand"
	"raftkv/network"
	"raftkv/types"
	"sync"
	"time"
)

type LogEntry struct {
	Term    types.Term
	command any
}

type Raft struct {
	Id                types.Id
	currentTerm       types.Term
	votedFor          *types.Id
	log               []LogEntry
	commitIdx         types.LogIdx
	lastApplied       types.LogIdx
	state             types.State
	leaderId          *types.Id
	mutexLock         sync.Mutex
	lastElectionReset time.Time
	network           *network.Network
	peerIds           []types.Id
	voteCount         uint16
	nextIdx           []types.LogIdx // only if it is a leader
	matchIdx          []types.LogIdx // only if it is a leader
	// electionTimer *time.Timer
}

func (r *Raft) AppendEntries(args *types.AppendEntriesArgs, reply *types.AppendEntriesReply) {
	r.mutexLock.Lock()
	defer r.mutexLock.Unlock()
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return
	}
	// once legitimate
	r.leaderId = &args.LeaderId
	if args.Term >= r.currentTerm {
		r.currentTerm = args.Term
		r.state = types.Follower
	}
	r.lastElectionReset = time.Now()
	reply.Term = r.currentTerm
	reply.Success = true // just for now
}

func (r *Raft) RequestVote(args *types.RequestVoteArgs, reply *types.RequestVoteReply) {
	r.mutexLock.Lock()
	defer r.mutexLock.Unlock()
	if r.currentTerm > args.Term {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.state = types.Follower
		r.votedFor = nil
	}
	reply.Term = r.currentTerm
	candidateUpToDate := args.LastLogTerm > r.getLastLogTerm() ||
		(args.LastLogTerm == r.getLastLogTerm() && args.LastLogIdx >= r.getLastLogIdx())
	if (r.votedFor == nil || (*r.votedFor) == args.CandidateId) && candidateUpToDate {
		reply.VoteGranted = true
		candidateId := args.CandidateId
		r.votedFor = &candidateId
		r.lastElectionReset = time.Now()
	} else {
		reply.VoteGranted = false
	}
}

func (r *Raft) init(id types.Id, peerIds []types.Id, network *network.Network) {
	r.Id = id
	r.peerIds = peerIds
	r.network = network
	r.currentTerm = 0
	r.votedFor = nil
	r.log = make([]LogEntry, 0)
	r.commitIdx = 0
	r.lastApplied = 0
	r.state = types.Follower
	r.leaderId = nil
	r.mutexLock = sync.Mutex{}
	r.voteCount = 0
	r.nextIdx = make([]types.LogIdx, 0)
	r.matchIdx = make([]types.LogIdx, 0)
}

func NewRaft(id types.Id, peerIds []types.Id, network *network.Network) *Raft {
	raft := Raft{}
	raft.init(id, peerIds, network)
	return &raft
}

func (r *Raft) Run() {
	go r.electionRoutine()
}

func (r *Raft) electionRoutine() {
	for {
		sleepStartedAt := time.Now()
		time.Sleep((150 + time.Duration(rand.Intn(150))) * time.Millisecond)
		r.mutexLock.Lock()
		if sleepStartedAt.Before(r.lastElectionReset) {
			r.mutexLock.Unlock()
			continue
		} else {
			if r.state == types.Leader {
				r.mutexLock.Unlock()
				continue
			}
			r.currentTerm += 1
			r.votedFor = nil
			r.state = types.Candidate
			r.votedFor = &r.Id
			r.voteCount = 1
			currentTerm := r.currentTerm
			lastLogIdx := r.getLastLogIdx()
			lastLogTerm := r.getLastLogTerm()
			r.mutexLock.Unlock()
			for _, id := range r.peerIds {
				go r.sendRequestVote(id, currentTerm, lastLogIdx, lastLogTerm)
			}
		}
	}
}

func (r *Raft) sendRequestVote(destination types.Id, voteTerm types.Term,
	lastLogIdx types.LogIdx, lastLogTerm types.Term) {
	args := &types.RequestVoteArgs{Term: voteTerm, CandidateId: r.Id,
		LastLogIdx: lastLogIdx, LastLogTerm: lastLogTerm}
	channel := r.network.SendRequestVote(r.Id, destination, args)
	select {
	case reply := <-channel:
		r.mutexLock.Lock()
		defer r.mutexLock.Unlock()
		if reply.Term > r.currentTerm {
			r.state = types.Follower
			r.currentTerm = reply.Term
			r.votedFor = nil
		} else if reply.VoteGranted {
			if voteTerm == r.currentTerm {
				r.voteCount += 1
			}
			if r.hasMajority() && r.state == types.Candidate {
				r.becomeLeader()
			}
		}
	case <-time.After(50 * time.Millisecond):
		return
	}
}

func (r *Raft) sendAppendEntries(destination types.Id, term types.Term,
	leaderId types.Id, prevLogIdx types.LogIdx, prevLogTerm types.Term,
	entries []any, leaderCommit types.LogIdx) {
	args := &types.AppendEntriesArgs{Term: term, LeaderId: leaderId,
		PrevLogIdx: prevLogIdx, PrevLogTerm: prevLogTerm, Entries: entries,
		LeaderCommit: leaderCommit}
	r.network.SendAppendEntries(r.Id, destination, args)
	// we don't care for reply right now because it is only a heartbeat
}

func (r *Raft) heartBeatRoutine() {
	for {
		r.mutexLock.Lock()
		if r.state == types.Leader {
			for _, id := range r.peerIds {
				go r.sendAppendEntries(id, r.currentTerm, r.Id, -1, -1, make([]any, 0), -1) // just for now
			}
			r.mutexLock.Unlock()
		} else {
			r.mutexLock.Unlock()
			break
		}
		// time.Sleep((50 + time.Duration(rand.Intn(50))) * time.Millisecond)
		time.Sleep(50 * time.Millisecond)
	}
}

func (r *Raft) becomeLeader() {
	lastLogIdx := r.getLastLogIdx()
	r.state = types.Leader
	r.nextIdx = make([]types.LogIdx, len(r.peerIds))
	for i := range r.nextIdx {
		r.nextIdx[i] = lastLogIdx + 1
	}
	r.matchIdx = make([]types.LogIdx, len(r.peerIds))
	for i := range r.matchIdx {
		r.matchIdx[i] = 0
	}
	go r.heartBeatRoutine()
}

func (r *Raft) hasMajority() bool {
	if r.voteCount > uint16(len(r.peerIds)+1)/2 {
		return true
	}
	return false
}

func (r *Raft) getLastLogIdx() types.LogIdx {
	var lastLogIdx types.LogIdx
	if len(r.log) == 0 {
		lastLogIdx = -1
	} else {
		lastLogIdx = types.LogIdx(len(r.log)) - 1
	}
	return lastLogIdx
}

func (r *Raft) getLastLogTerm() types.Term {
	lastLogIdx := r.getLastLogIdx()
	if lastLogIdx == -1 {
		return -1
	} else {
		return r.log[lastLogIdx].Term
	}
}

func (r *Raft) GetState() (types.State, types.Term) {
	r.mutexLock.Lock()
	defer r.mutexLock.Unlock()
	return r.state, r.currentTerm
}
