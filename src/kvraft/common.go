package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutdatedReq = "ErrOutdatedReq"
	ErrApply	   = "ErrApply"
)


const (
	GET = iota
	PUT
	APPEND
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Op 	int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestArgs struct {
	Key 		string
	Value 		string
	OpType 		int
	ClientID	int64
	SeriesID 	int
}

type RequestReply struct {
	Err 	Err
	Value 	string
}

type FindLeaderArgs struct {

}

type FindLeaderReply struct {
	IsLeader	bool
	ServerID	int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key 		string
	Value 		string
	OpType		int
	ClientID	int64
	SeriesID	int
}