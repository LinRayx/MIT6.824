package kvraft

import (
	"src/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID 	int
	mu			sync.Mutex
	seriesID 	int
	opCh 		chan Op
	selfID 		int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seriesID = 0
	ck.leaderID = 0
	ck.opCh = make(chan Op)
	ck.selfID = nrand()
	//go ck.FindLeader()
	DPrintf("clientID: [%d] start!", ck.selfID)
	return ck
}

// findLeader
func (ck *Clerk) FindLeader() {
	for {
		for i, _ := range ck.servers {
			ck.mu.Lock()
			if ck.leaderID != -1 {
				ck.mu.Unlock()
				break
			}
			go func(index int) {
				for {
					args := FindLeaderArgs{}
					reply := FindLeaderReply{}
					ok := ck.sendIsLeaderRPC(index, &args, &reply)
					if ok {
						if reply.IsLeader == true {
							ck.mu.Lock()
							ck.leaderID = index
							DPrintf("clientID: [%d] find Leader! : %d index: %d", ck.selfID, reply.ServerID, index)
							ck.mu.Unlock()
						}
						break
					}
				}
			}(i)
			ck.mu.Unlock()
		}
		ck.mu.Lock()
		if ck.leaderID != -1 {
			ck.mu.Unlock()
			break
		}
		ck.mu.Unlock()
		// server还在选主
		time.Sleep(time.Second)
	}
}

func (ck *Clerk) sendIsLeaderRPC(server int, args *FindLeaderArgs, reply *FindLeaderReply) bool {
	DPrintf("clientID: [%d] sendIsLeaderRPC to server %d", ck.selfID, server)
	ok := ck.servers[server].Call("KVServer.IsLeader", args, reply)
	return ok
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ck.seriesID++
	ck.mu.Unlock()
	_, value := ck.SendRequestRPC(key, "", GET)
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op int) {
	// You will have to modify this function.
	ck.SendRequestRPC(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.mu.Lock()
	ck.seriesID++
	ck.mu.Unlock()
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.mu.Lock()
	ck.seriesID++
	ck.mu.Unlock()
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) SendRequestRPC(key string, value string, op int) (bool, string){
	for {
		ck.mu.Lock()
		args := RequestArgs{
			Key:      key,
			Value:    value,
			OpType:   op,
			SeriesID: ck.seriesID,
			ClientID: ck.selfID,
		}
		reply := RequestReply{}
		ck.mu.Unlock()
		ok := ck._sendRequestRPC(ck.leaderID, &args, &reply)
		DPrintf("clientID: [%d]  _sendRequestRPC serverID: %d result: ok: %v SeriesID: %d reply.Err: %s", ck.selfID, ck.leaderID, ok, args.SeriesID, reply.Err)
		if ok {
			if reply.Err == OK {
				return true, reply.Value
			} else if reply.Err == ErrNoKey {
				return true, reply.Value
			}  else if reply.Err == ErrWrongLeader {
				ck.mu.Lock()
				ck.leaderID = (ck.leaderID+1) % len(ck.servers)
				ck.mu.Unlock()
			}
		} else {
			ck.mu.Lock()
			ck.leaderID = (ck.leaderID+1) % len(ck.servers)
			ck.mu.Unlock()
		}
		//time.Sleep(100 * time.Millisecond)
	}
}

// send get put append RPC
func (ck *Clerk) _sendRequestRPC(server int, args *RequestArgs, reply *RequestReply) bool {
	//DPrintf("clientID: [%d] _sendRequestRPC seriesID: %d type: %v key: %v value: %v server: %d", ck.selfID, args.SeriesID, args.OpType, args.Key, args.Value, server)
	ok := ck.servers[server].Call("KVServer.ClientRequest",args, reply)
	return ok
}