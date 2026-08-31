package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"raftkv/network"
	"raftkv/types"
	"sort"
	"sync"
	"time"
)

type Raft struct {
	Id                   types.Id
	currentTerm          types.Term
	votedFor             *types.Id
	log                  []types.LogEntry
	commitIdx            types.LogIdx
	lastApplied          types.LogIdx
	state                types.State
	leaderId             *types.Id
	mutexLock            sync.Mutex
	lastElectionReset    time.Time
	network              *network.Network
	PeerIds              []types.Id
	voteCount            uint16
	nextIdx              map[types.Id]types.LogIdx // only if it is a leader
	matchIdx             map[types.Id]types.LogIdx // only if it is a leader
	appendEntriesChannel chan struct{}
}

type persistentState struct {
	CurrentTerm types.Term
	VotedFor    *types.Id
	Log         []types.LogEntry
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
		r.persist()
		r.state = types.Follower
	}
	r.lastElectionReset = time.Now()
	if args.PrevLogIdx >= types.LogIdx(len(r.log)) {
		reply.Success = false
	} else if args.PrevLogIdx >= 0 && r.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		reply.Success = false
	} else {
		i := args.PrevLogIdx + 1
		for ; i < types.LogIdx(len(r.log)) &&
			i-args.PrevLogIdx-1 < types.LogIdx(len(args.Entries)); i++ {
			if r.log[i].Term != args.Entries[i-args.PrevLogIdx-1].Term {
				r.log = r.log[:i]
				break
			}
		}
		for j := i - args.PrevLogIdx - 1; j < types.LogIdx(len(args.Entries)); j++ {
			r.log = append(r.log, args.Entries[j])
		}
		r.persist()
		if args.LeaderCommit > r.commitIdx {
			r.commitIdx = min(args.LeaderCommit, r.getLastLogIdx())
			if entries := r.getAppliableEntries(); len(entries) > 0 {
				fmt.Printf("[node %d] applying entries: %+v\n", r.Id, entries)
			}
		}
		reply.Success = true
	}
	reply.Term = r.currentTerm
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
	r.persist()
}

func (r *Raft) init(id types.Id, peerIds []types.Id, network *network.Network) {
	r.Id = id
	r.PeerIds = peerIds
	r.network = network
	persistedState := r.loadPersisted()
	if persistedState != nil {
		r.currentTerm = persistedState.CurrentTerm
		r.votedFor = persistedState.VotedFor
		r.log = persistedState.Log
	} else {
		r.currentTerm = 0
		r.votedFor = nil
		r.log = make([]types.LogEntry, 0)
	}
	r.commitIdx = -1
	r.lastApplied = -1
	r.state = types.Follower
	r.leaderId = nil
	r.mutexLock = sync.Mutex{}
	r.voteCount = 0
	r.nextIdx = make(map[types.Id]types.LogIdx)
	r.matchIdx = make(map[types.Id]types.LogIdx)
	r.appendEntriesChannel = make(chan struct{}, 10)
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
			r.persist()
			r.voteCount = 1
			currentTerm := r.currentTerm
			lastLogIdx := r.getLastLogIdx()
			lastLogTerm := r.getLastLogTerm()
			r.mutexLock.Unlock()
			for _, id := range r.PeerIds {
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
			r.persist()
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

func (r *Raft) sendAppendEntries(destination types.Id) {
	args := r.buildAppendEntriesArgs(destination)
	channel := r.network.SendAppendEntries(r.Id, destination, &args)
	select {
	case reply := <-channel:
		r.mutexLock.Lock()
		defer r.mutexLock.Unlock()
		if reply.Term > r.currentTerm {
			r.state = types.Follower
			r.currentTerm = reply.Term
			r.votedFor = nil
			r.persist()
		} else if !reply.Success {
			if r.nextIdx[destination] > 0 {
				r.nextIdx[destination] -= 1
			}
		} else {
			if len(args.Entries) > 0 {
				r.nextIdx[destination] = max(args.PrevLogIdx+types.LogIdx(len(args.Entries))+1, r.nextIdx[destination])
				r.matchIdx[destination] = max(args.PrevLogIdx+types.LogIdx(len(args.Entries)), r.matchIdx[destination])
				if shouldAdvance, newIdx := r.tryAdvanceCommitIndex(); shouldAdvance {
					r.commitIdx = newIdx
				}
			}
		}
	case <-time.After(200 * time.Millisecond):
		return
	}
}

func (r *Raft) Start(command types.Command) (types.LogIdx, types.Term, bool) {
	r.mutexLock.Lock()
	if r.state != types.Leader {
		r.mutexLock.Unlock()
		return -1, -1, false
	} else {
		currentTerm := r.currentTerm
		entry := types.LogEntry{Term: currentTerm, Command: command}
		r.log = append(r.log, entry)
		r.persist()
		select { // use select statement and default for non-blocking send to avoid deadlock
		case r.appendEntriesChannel <- struct{}{}:
		default:
		}
		lastLogIdx := r.getLastLogIdx()
		r.mutexLock.Unlock()
		return lastLogIdx, currentTerm, true
	}
}

func (r *Raft) buildAppendEntriesArgs(peer types.Id) types.AppendEntriesArgs {
	args := types.AppendEntriesArgs{}
	args.LeaderId = r.Id
	r.mutexLock.Lock()
	defer r.mutexLock.Unlock()
	args.Term = r.currentTerm
	args.LeaderCommit = r.commitIdx
	nextIdx := r.nextIdx[peer]
	args.PrevLogIdx = nextIdx - 1
	if args.PrevLogIdx >= 0 {
		args.PrevLogTerm = r.log[args.PrevLogIdx].Term
	} else {
		args.PrevLogTerm = -1
	}
	entries := make([]types.LogEntry, len(r.log[nextIdx:]))
	copy(entries, r.log[nextIdx:])
	args.Entries = entries
	return args
}

func (r *Raft) heartBeatRoutine() {
	for {
		select {
		case <-time.After(50 * time.Millisecond):
		case <-r.appendEntriesChannel:
		}
		r.mutexLock.Lock()
		if r.state != types.Leader {
			r.mutexLock.Unlock()
			return
		}
		for _, id := range r.PeerIds {
			go r.sendAppendEntries(id)
		}
		if entries := r.getAppliableEntries(); len(entries) > 0 {
			fmt.Printf("[node %d] applying entries: %+v\n", r.Id, entries)
		}
		r.mutexLock.Unlock()
	}
}

func (r *Raft) becomeLeader() {
	lastLogIdx := r.getLastLogIdx()
	r.state = types.Leader
	for _, peerId := range r.PeerIds {
		r.nextIdx[peerId] = lastLogIdx + 1
	}
	for _, peerId := range r.PeerIds {
		r.matchIdx[peerId] = 0
	}
	go r.heartBeatRoutine()
}

func (r *Raft) persist() error {
	logCopy := make([]types.LogEntry, len(r.log))
	copy(logCopy, r.log)
	persistentState := persistentState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor, Log: logCopy}

	data, err := json.Marshal(persistentState)
	if err != nil {
		return err
	}
	if err := os.MkdirAll("states", os.ModePerm); err != nil {
		return err
	}
	err = os.WriteFile(fmt.Sprintf("states/raft-state-%d.json", r.Id), data, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (r *Raft) loadPersisted() *persistentState {
	_, err := os.Stat("states")
	if err != nil {
		return nil
	}

	data, err := os.ReadFile(fmt.Sprintf("states/raft-state-%d.json", r.Id))
	if err != nil {
		return nil
	}

	// Unmarshal JSON to struct
	var state persistentState
	err = json.Unmarshal(data, &state)
	if err != nil {
		log.Fatal(err)
	}

	return &state
}

func (r *Raft) hasMajority() bool {
	if r.voteCount > uint16(len(r.PeerIds)+1)/2 {
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

func (r *Raft) GetLog() []types.LogEntry {
	r.mutexLock.Lock()
	defer r.mutexLock.Unlock()
	return r.log
}

func (r *Raft) tryAdvanceCommitIndex() (bool, types.LogIdx) {
	matchIndexes := make([]types.LogIdx, 0, len(r.PeerIds)+1)
	for _, idx := range r.matchIdx {
		matchIndexes = append(matchIndexes, idx)
	}
	matchIndexes = append(matchIndexes, types.LogIdx(len(r.log)-1))
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] > matchIndexes[j] // Descending order
	})
	count := 0
	commitIdx := matchIndexes[len(matchIndexes)-1]
	for _, idx := range matchIndexes {
		commitIdx = idx
		count += 1
		if count > (len(r.PeerIds)+1)/2 {
			if commitIdx > r.commitIdx &&
				r.log[commitIdx].Term == r.currentTerm {
				return true, commitIdx
			} else {
				return false, -1
			}
		}
	}
	return false, -1
}

func (r *Raft) getAppliableEntries() []types.LogEntry {
	if r.commitIdx <= r.lastApplied {
		return nil
	}
	entries := r.log[r.lastApplied+1 : r.commitIdx+1]
	r.lastApplied = r.commitIdx
	return entries
}
