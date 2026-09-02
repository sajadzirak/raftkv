package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"raftkv/kv"
	"raftkv/network"
	"raftkv/raft"
	"raftkv/types"
)

// a small demo
func main() {
	// cleaning up previous states
	if err := os.RemoveAll("states"); err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}

	const nodesNum = 5

	net := new(network.Network)
	net.Peers = make(map[types.Id]types.RPCHandler)
	net.Isolated = make(map[types.Id]bool)

	rafts := make([]*raft.Raft, nodesNum)
	stores := make([]*kv.KVStore, nodesNum)

	for i := 0; i < nodesNum; i++ {
		var peerIds []types.Id
		for j := 0; j < nodesNum; j++ {
			if j != i {
				peerIds = append(peerIds, types.Id(j))
			}
		}
		stores[i] = kv.NewKVStore()
		rafts[i] = raft.NewRaft(types.Id(i), peerIds, net, stores[i])
		net.RegisterPeer(types.Id(i), rafts[i])
	}

	for _, r := range rafts {
		go r.Run()
	}

	fmt.Println("Waiting for a leader to be elected...")
	time.Sleep(1 * time.Second)

	var leaderId types.Id = nodesNum
	for i, r := range rafts {
		if state, term := r.GetState(); state == types.Leader {
			leaderId = types.Id(i)
			fmt.Printf("Leader elected: node %d (term %d)\n", i, term)
		}
	}
	if leaderId == nodesNum {
		fmt.Println("No leader elected within timeout — try rerunning.")
		return
	}

	leader := rafts[leaderId]
	leaderStore := stores[leaderId]

	fmt.Println("\nSubmitting a few key-value writes through the leader...")
	writes := map[string]string{"foo": "bar", "hello": "world", "answer": "42"}
	for k, v := range writes {
		if _, err := leaderStore.Put(leader, k, v); err != nil {
			fmt.Printf("  Put(%q, %q) failed: %v\n", k, v, err)
			continue
		}
		fmt.Printf("  Put(%q, %q) committed\n", k, v)
	}

	time.Sleep(500 * time.Millisecond)

	fmt.Println("\nReading back through the leader:")
	for k := range writes {
		v, err := leaderStore.Get(leader, k)
		if err != nil {
			fmt.Printf("  Get(%q) failed: %v\n", k, err)
			continue
		}
		fmt.Printf("  Get(%q) = %q\n", k, v)
	}

	fmt.Println("\nSimulating a network partition of the leader...")
	net.SetIsolated(leaderId, true)
	time.Sleep(1 * time.Second)

	var newLeaderId types.Id = nodesNum
	for i, r := range rafts {
		if types.Id(i) == leaderId {
			continue
		}
		if state, term := r.GetState(); state == types.Leader {
			newLeaderId = types.Id(i)
			fmt.Printf("New leader elected after partition: node %d (term %d)\n", i, term)
		}
	}
	if newLeaderId == nodesNum {
		fmt.Println("No new leader elected — cluster may still be electing, try rerunning.")
	}

	fmt.Println("\nRestoring the partitioned node...")
	net.SetIsolated(leaderId, false)
	time.Sleep(1 * time.Second)

	if state, _ := rafts[leaderId].GetState(); state == types.Follower {
		fmt.Println("Old leader rejoined as Follower, as expected.")
	}

	fmt.Println("\nDone!")
}
