package network

import (
	"math/rand"
	"raftkv/types"
	"sync"
	"time"
)

type Network struct {
	Peers     map[types.Id]types.RPCHandler
	Isolated  map[types.Id]bool
	NumPeers  uint16
	DropRate  uint8
	mutexLock sync.RWMutex
}

func (n *Network) SendAppendEntries(source types.Id, destination types.Id, args *types.AppendEntriesArgs) chan types.AppendEntriesReply {
	channel := make(chan types.AppendEntriesReply)
	n.mutexLock.Lock()
	isolatedSource := n.Isolated[source]
	isolatedDestination := n.Isolated[destination]
	n.mutexLock.Unlock()
	if isolatedSource || isolatedDestination {
		return channel
	}
	go func() {
		if shouldDrop(n.DropRate) {
			return
		}
		delay()
		n.mutexLock.Lock()
		peer := n.Peers[destination]
		n.mutexLock.Unlock()
		if peer == nil {
			return
		}
		var reply types.AppendEntriesReply
		peer.AppendEntries(args, &reply)
		channel <- reply
	}()
	return channel
}

func (n *Network) SendRequestVote(source types.Id, destination types.Id, args *types.RequestVoteArgs) chan types.RequestVoteReply {
	channel := make(chan types.RequestVoteReply)
	n.mutexLock.Lock()
	isolatedSource := n.Isolated[source]
	isolatedDestination := n.Isolated[destination]
	n.mutexLock.Unlock()
	if isolatedSource || isolatedDestination {
		return channel
	}
	go func() {
		if shouldDrop(n.DropRate) {
			return
		}
		delay()
		n.mutexLock.Lock()
		peer := n.Peers[destination]
		n.mutexLock.Unlock()
		if peer == nil {
			return
		}
		var reply types.RequestVoteReply
		peer.RequestVote(args, &reply)
		channel <- reply
	}()
	return channel
}

func shouldDrop(rate uint8) bool {
	num := rand.Intn(100)
	if num < int(rate) {
		return true
	}
	return false
}

func delay() {
	min := 10
	max := 50
	num := min + rand.Intn(max-min)
	time.Sleep(time.Millisecond * time.Duration(num))
}

func (n *Network) SetIsolated(id types.Id, isolate bool) {
	n.mutexLock.Lock()
	defer n.mutexLock.Unlock()
	n.Isolated[id] = isolate
}

func (n *Network) RegisterPeer(id types.Id, raft types.RPCHandler) {
	n.mutexLock.Lock()
	defer n.mutexLock.Unlock()
	n.Peers[id] = raft
}

func (n *Network) IsIsolated(id types.Id) bool {
	n.mutexLock.Lock()
	defer n.mutexLock.Unlock()
	return n.Isolated[id]
}
