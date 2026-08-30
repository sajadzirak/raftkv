package types

type Term int
type Id uint16
type LogIdx int64
type State int
type Command any

const (
	Follower State = iota
	Candidate
	Leader
)

type RPCHandler interface {
	AppendEntries(*AppendEntriesArgs, *AppendEntriesReply)
	RequestVote(*RequestVoteArgs, *RequestVoteReply)
}

type AppendEntriesArgs struct {
	Term         Term
	LeaderId     Id
	PrevLogIdx   LogIdx
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIdx
}

type AppendEntriesReply struct {
	Term    Term
	Success bool
}

type RequestVoteArgs struct {
	Term        Term
	CandidateId Id
	LastLogIdx  LogIdx
	LastLogTerm Term
}

type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

type LogEntry struct {
	Term    Term
	Command Command
}

func StateToString(state State) string {
	switch state {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return "Invalid"
	}
}
