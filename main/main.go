package main

import (
	"fmt"
	"raftkv/network"
	"raftkv/raft"
	"raftkv/types"
	"sync"
	"time"
)

func main() {
	network := new(network.Network)
	raft1 := new(raft.Raft)
	raft2 := new(raft.Raft)

	network.Peers = make(map[types.Id]types.RPCHandler)
	network.Peers[0] = raft1
	network.Peers[1] = raft2

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		channel := network.SendRequestVote(1, &types.RequestVoteArgs{})
		select {
		case reply := <-channel:
			fmt.Println(reply.VoteGranted)
		case <-time.After(200 * time.Millisecond):
			println("Timeout after 200 milliseconds")
		}
		wg.Done()
	}()
	wg.Wait()

}
