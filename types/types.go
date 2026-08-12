package types

type Term uint
type Id uint16
type LogIdx uint64

type RPCHandler interface {
	AppendEntries(*AppendEntriesArgs, *AppendEntriesReply)
	RequestVote(*RequestVoteArgs, *RequestVoteReply)
}

type AppendEntriesArgs struct {
	term         Term
	leaderId     Id
	prevLogIdx   LogIdx
	prevLogTerm  Term
	entries      []any
	leaderCommit LogIdx
}

type AppendEntriesReply struct {
	Term    Term
	Success bool
}

type RequestVoteArgs struct {
	term        Term
	candidateId Id
	lastLogIdx  LogIdx
	lastLogTerm Term
}

type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}
