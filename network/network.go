package network

import (
	"math/rand"
	"raftkv/types"
	"time"
)

type Network struct {
	Peers    map[types.Id]types.RPCHandler
	NumPeers uint16
	DropRate uint8
}

func (n *Network) SendAppendEntries(destination types.Id, args *types.AppendEntriesArgs) chan types.AppendEntriesReply {
	channel := make(chan types.AppendEntriesReply)
	go func() {
		if shouldDrop(n.DropRate) {
			return
		}
		delay()
		peer := n.Peers[destination]
		if peer == nil {
			return
		}
		var reply types.AppendEntriesReply
		peer.AppendEntries(args, &reply)
		channel <- reply
	}()
	return channel
}

func (n *Network) SendRequestVote(destination types.Id, args *types.RequestVoteArgs) chan types.RequestVoteReply {
	channel := make(chan types.RequestVoteReply)
	go func() {
		if shouldDrop(n.DropRate) {
			return
		}
		delay()
		peer := n.Peers[destination]
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
	max := 100
	num := min + rand.Intn(max-min)
	time.Sleep(time.Millisecond * time.Duration(num))
}
