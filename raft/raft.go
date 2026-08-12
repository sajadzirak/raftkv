package raft

import "raftkv/types"

type Raft struct {
}

func (r *Raft) AppendEntries(args *types.AppendEntriesArgs, reply *types.AppendEntriesReply) {
}

func (r *Raft) RequestVote(args *types.RequestVoteArgs, reply *types.RequestVoteReply) {
}
