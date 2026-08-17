package main

import (
	"fmt"
	"raftkv/network"
	"raftkv/raft"
	"raftkv/types"
	"strconv"
	"time"
)

func main() {
	nodesNum := 7
	network := new(network.Network)
	rafts := make([]*raft.Raft, nodesNum)
	network.Peers = make(map[types.Id]types.RPCHandler)
	network.Isolated = make(map[types.Id]bool)

	for i := 0; i < nodesNum; i++ {
		var peerIds []types.Id
		for j := 0; j < nodesNum; j++ {
			if j != i {
				peerIds = append(peerIds, types.Id(j))
			}
		}
		rafts[i] = raft.NewRaft(types.Id(i), peerIds, network)
		network.Peers[types.Id(i)] = rafts[i]
		network.Isolated[types.Id(i)] = false
	}

	for _, r := range rafts {
		go r.Run()
	}

	leaderId, leaderTerm, found, safe := waitForLeader(rafts, 2*time.Second)
	if found && safe {
		fmt.Println("Leader id: " + strconv.Itoa(int(leaderId)) +
			", leader Term: " + strconv.Itoa(int(leaderTerm)))
	} else if !found {
		fmt.Println("[Failed] no leader found")
	} else if found && !safe {
		fmt.Println("[Failed] split brain")
	}
	isolate(network, leaderId)
	time.Sleep(2 * time.Second)
	isolatedId := leaderId
	fmt.Println("--- After isolating the leader ---")
	leaderId, leaderTerm, found, safe = waitForLeader(rafts, 2*time.Second)
	if found && safe {
		fmt.Println("Leader id: " + strconv.Itoa(int(leaderId)) +
			", leader Term: " + strconv.Itoa(int(leaderTerm)))
	} else if !found {
		fmt.Println("[Failed] no leader found")
	} else if found && !safe {
		fmt.Println("[Failed] split brain")
	}
	restore(network, isolatedId)
	time.Sleep(2 * time.Second)
	fmt.Println("--- After restoring the isolated leader ---")
	leaderId, leaderTerm, found, safe = waitForLeader(rafts, 2*time.Second)
	if found && safe {
		fmt.Println("Leader id: " + strconv.Itoa(int(leaderId)) +
			", leader Term: " + strconv.Itoa(int(leaderTerm)))
	} else if !found {
		fmt.Println("[Failed] no leader found")
	} else if found && !safe {
		fmt.Println("[Failed] split brain")
	}
	restoredState, term := rafts[isolatedId].GetState()
	fmt.Println("restored node state: " + types.StateToString(restoredState) +
		", term: " + strconv.Itoa(int(term)))
}

// id, term, found, safe
func findLeader(rafts []*raft.Raft) (types.Id, types.Term, bool, bool) {
	var found, safe bool
	leaderCount := 0
	leaderTerms := make([]types.Term, 0)
	leaderIds := make([]types.Id, 0)
	for _, r := range rafts {
		state, term := r.GetState()
		if state == types.Leader {
			leaderCount += 1
			leaderTerms = append(leaderTerms, term)
			leaderIds = append(leaderIds, r.Id)
		}
	}

	switch leaderCount {
	case 0:
		found = false
		safe = true
		return 0, 0, found, safe
	case 1:
		found = true
		safe = true
		return leaderIds[0], leaderTerms[0], found, safe
	default:
		counter := make(map[types.Term]int)
		for _, term := range leaderTerms {
			counter[term] += 1
			if counter[term] >= 2 {
				return 0, 0, true, false
			}
		}
		maxId := leaderIds[0]
		maxTerm := leaderTerms[0]
		for i, term := range leaderTerms {
			if term > maxTerm {
				maxId = leaderIds[i]
				maxTerm = leaderTerms[i]
			}
		}
		return maxId, maxTerm, true, true // maybe network partition
	}
}

func waitForLeader(rafts []*raft.Raft, timeout time.Duration) (types.Id, types.Term, bool, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		id, term, found, safe := findLeader(rafts)
		if !safe {
			return id, term, found, safe
		}
		if found {
			return id, term, found, safe
		}
		time.Sleep(75 * time.Millisecond)
	}
	return 0, 0, false, true
}

func isolate(network *network.Network, id types.Id) {
	network.Isolated[id] = true
}

func restore(network *network.Network, id types.Id) {
	network.Isolated[id] = false
}
