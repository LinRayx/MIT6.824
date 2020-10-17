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

// snapshot structure

func DeepCopy(value interface{}) interface{} {
	if valueMap, ok := value.(map[string]interface{}); ok {
		newMap := make(map[string]interface{})
		for k, v := range valueMap {
			newMap[k] = DeepCopy(v)
		}

		return newMap
	} else if valueSlice, ok := value.([]interface{}); ok {
		newSlice := make([]interface{}, len(valueSlice))
		for k, v := range valueSlice {
			newSlice[k] = DeepCopy(v)
		}

		return newSlice
	}

	return value
}