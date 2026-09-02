package raft

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"raftkv/kv"
	"raftkv/network"
	"raftkv/raft"
	"raftkv/types"
)

type cluster struct {
	network *network.Network
	rafts   []*raft.Raft
	stores  []*kv.KVStore
	n       int
}

func newCluster(n int) *cluster {
	network := new(network.Network)
	network.Peers = make(map[types.Id]types.RPCHandler)
	network.Isolated = make(map[types.Id]bool)

	rafts := make([]*raft.Raft, n)
	stores := make([]*kv.KVStore, n)

	for i := 0; i < n; i++ {
		var peerIds []types.Id
		for j := 0; j < n; j++ {
			if j != i {
				peerIds = append(peerIds, types.Id(j))
			}
		}
		stores[i] = kv.NewKVStore()
		rafts[i] = raft.NewRaft(types.Id(i), peerIds, network, stores[i])
		network.RegisterPeer(types.Id(i), rafts[i])
		network.SetIsolated(types.Id(i), false)
	}

	for _, r := range rafts {
		go r.Run()
	}

	return &cluster{network: network, rafts: rafts, stores: stores, n: n}
}

func findLeader(network *network.Network, rafts []*raft.Raft) (types.Id, types.Term, bool, bool) {
	var found, safe bool
	leaderCount := 0
	leaderTerms := make([]types.Term, 0)
	leaderIds := make([]types.Id, 0)

	for _, r := range rafts {
		if network.IsIsolated(r.Id) { // skip nodes we know are cut off
			continue
		}
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
		return maxId, maxTerm, true, true
	}
}

func waitForLeader(network *network.Network, rafts []*raft.Raft, timeout time.Duration) (types.Id, types.Term, bool, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		id, term, found, safe := findLeader(network, rafts)
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

func logsEqual(a, b []types.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || a[i].Command != b[i].Command {
			return false
		}
	}
	return true
}

// Test 1: Leader Election

func TestLeaderElection(t *testing.T) {
	cluster := newCluster(5)

	leaderId, term1, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
	if !safe {
		t.Fatalf("split-brain detected: two leaders in term %d", term1)
	}
	if !found {
		t.Fatalf("no leader elected within timeout")
	}
	t.Logf("initial leader: id=%d term=%d", leaderId, term1)

	// Isolate the leader
	cluster.network.SetIsolated(leaderId, true)

	newLeaderId, term2, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
	if !safe {
		t.Fatalf("split-brain detected after isolating leader (term %d)", term2)
	}
	if !found {
		t.Fatalf("no new leader elected after isolating old leader")
	}
	if term2 <= term1 {
		t.Fatalf("expected strictly higher term after re-election: old=%d new=%d", term1, term2)
	}
	if newLeaderId == leaderId {
		t.Fatalf("isolated leader should not still be recognized as leader")
	}
	t.Logf("new leader after isolation: id=%d term=%d", newLeaderId, term2)

	// Restore the old leader
	cluster.network.SetIsolated(leaderId, false)
	time.Sleep(1 * time.Second)

	state, term := cluster.rafts[leaderId].GetState()
	if state != types.Follower {
		t.Fatalf("old leader did not step down after rejoining: state=%v term=%d", state, term)
	}
	if term < term2 {
		t.Fatalf("old leader did not catch up to current term: has=%d want>=%d", term, term2)
	}
}

// TestLeaderElectionVariousSizes checks quorum math holds at cluster-size edges.
func TestLeaderElectionVariousSizes(t *testing.T) {
	for _, n := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			cluster := newCluster(n)
			id, term, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
			if !safe {
				t.Fatalf("split-brain in %d-node cluster (term %d)", n, term)
			}
			if !found {
				t.Fatalf("no leader elected in %d-node cluster", n)
			}
			t.Logf("n=%d leader id=%d term=%d", n, id, term)
		})
	}
}

// Test 2: Log Replication

func TestLogReplication(t *testing.T) {
	cluster := newCluster(7)

	leaderId, _, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
	if !found || !safe {
		t.Fatalf("failed to establish initial leader (found=%v safe=%v)", found, safe)
	}
	leader := cluster.rafts[leaderId]

	isolatedFollower := (leaderId + 1) % types.Id(cluster.n)
	cluster.network.SetIsolated(isolatedFollower, true)

	const numCmds = 4
	for i := 0; i < numCmds; i++ {
		_, _, isLeader := leader.Start(types.Command{Op: "PUT", Key: fmt.Sprintf("key-%d", i),
			Value: fmt.Sprintf("value-%d", i)})
		if !isLeader {
			t.Fatalf("Start() called on non-leader node %d", leaderId)
		}
	}

	time.Sleep(1 * time.Second)

	leaderLog := leader.GetLog()
	if len(leaderLog) != numCmds {
		t.Fatalf("leader log length = %d, want %d", len(leaderLog), numCmds)
	}

	isolatedLog := cluster.rafts[isolatedFollower].GetLog()
	if len(isolatedLog) != 0 {
		t.Fatalf("isolated follower should not have received entries while isolated, got %d entries", len(isolatedLog))
	}

	cluster.network.SetIsolated(isolatedFollower, false)
	time.Sleep(2 * time.Second)

	recoveredLog := cluster.rafts[isolatedFollower].GetLog()
	if !logsEqual(leaderLog, recoveredLog) {
		t.Fatalf("restored follower log diverged from leader.\nleader:  %+v\nfollower: %+v", leaderLog, recoveredLog)
	}
}

func TestStartRejectsNonLeader(t *testing.T) {
	cluster := newCluster(5)
	leaderId, _, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
	if !found || !safe {
		t.Fatalf("failed to establish initial leader")
	}

	for i, r := range cluster.rafts {
		if types.Id(i) == leaderId {
			continue
		}
		_, _, isLeader := r.Start(types.Command{Op: "PUT", Key: "This",
			Value: "shouldn't be accepted"})
		if isLeader {
			t.Fatalf("node %d incorrectly accepted Start() while not leader", i)
		}
	}
}

// Test 3: Crash Recovery

func TestCrashRecovery(t *testing.T) {
	cluster := newCluster(5)

	leaderId, _, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
	if !found || !safe {
		t.Fatalf("failed to establish initial leader")
	}
	leader := cluster.rafts[leaderId]

	const numCmds = 3
	for i := 0; i < numCmds; i++ {
		_, _, isLeader := leader.Start(types.Command{Op: "PUT", Key: fmt.Sprintf("key-%d", i),
			Value: fmt.Sprintf("value-%d", i)})
		if !isLeader {
			t.Fatalf("Start() failed on leader")
		}
	}
	time.Sleep(1 * time.Second)

	crashedId := (leaderId + 1) % types.Id(cluster.n)
	peerIds := cluster.rafts[crashedId].PeerIds

	// Simulate crash by isolating old instance and replacing with a fresh one
	cluster.network.SetIsolated(crashedId, true)
	freshStore := kv.NewKVStore()
	cluster.rafts[crashedId] = raft.NewRaft(crashedId, peerIds, cluster.network, freshStore)
	cluster.stores[crashedId] = freshStore
	cluster.network.RegisterPeer(crashedId, cluster.rafts[crashedId])
	time.Sleep(500 * time.Millisecond)

	leaderLog := leader.GetLog()
	recoveredLog := cluster.rafts[crashedId].GetLog()
	if !logsEqual(leaderLog, recoveredLog) {
		t.Fatalf("recovered node's persisted log does not match leader.\nleader:    %+v\nrecovered: %+v", leaderLog, recoveredLog)
	}

	// Confirm it actually rejoined
	cluster.network.SetIsolated(crashedId, false)
	cluster.rafts[crashedId].Run()

	_, _, isLeader := leader.Start(types.Command{Op: "PUT", Key: fmt.Sprintf("key-%d", numCmds),
		Value: fmt.Sprintf("value-%d", numCmds)})
	if !isLeader {
		t.Fatalf("Start() failed on leader after recovery step")
	}
	time.Sleep(1 * time.Second)

	leaderLog = leader.GetLog()
	recoveredLog = cluster.rafts[crashedId].GetLog()
	if !logsEqual(leaderLog, recoveredLog) {
		t.Fatalf("recovered node failed to catch up on post-recovery entry.\nleader:    %+v\nrecovered: %+v", leaderLog, recoveredLog)
	}
}

// Test 4: KV Store Convergence

func TestKVConvergence(t *testing.T) {
	cluster := newCluster(7)

	leaderId, _, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
	if !found || !safe {
		t.Fatalf("failed to establish initial leader")
	}
	leader := cluster.rafts[leaderId]
	leaderStore := cluster.stores[leaderId]

	for i := 0; i < 3; i++ {
		if _, err := leaderStore.Put(leader, fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	isolatedId := (leaderId + 1) % types.Id(cluster.n)
	cluster.network.SetIsolated(isolatedId, true)

	if _, err := leaderStore.Put(leader, "key-3", "val-3"); err != nil {
		t.Fatalf("Put failed during isolation window: %v", err)
	}
	if _, err := leaderStore.Delete(leader, "key-0"); err != nil {
		t.Fatalf("Delete failed during isolation window: %v", err)
	}
	time.Sleep(1 * time.Second)

	cluster.network.SetIsolated(isolatedId, false)
	time.Sleep(1 * time.Second)

	want := leaderStore.Snapshot()
	for i := 0; i < cluster.n; i++ {
		id := types.Id(i)
		got := cluster.stores[id].Snapshot()
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("node %d KV store diverged from leader.\nleader: %+v\nnode:   %+v", id, want, got)
		}
	}
}

func TestGetRejectsOnNonLeader(t *testing.T) {
	cluster := newCluster(5)
	leaderId, _, found, safe := waitForLeader(cluster.network, cluster.rafts, 2*time.Second)
	if !found || !safe {
		t.Fatalf("failed to establish initial leader")
	}
	leader := cluster.rafts[leaderId]
	leaderStore := cluster.stores[leaderId]

	if _, err := leaderStore.Put(leader, "foo", "bar"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	for i, r := range cluster.rafts {
		if types.Id(i) == leaderId {
			continue
		}
		_, err := cluster.stores[i].Get(r, "foo")
		if err == nil {
			t.Fatalf("Get on non-leader node %d unexpectedly succeeded", i)
		}
	}
}
