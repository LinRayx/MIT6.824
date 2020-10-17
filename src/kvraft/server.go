package kvraft

import (
	"bytes"
	"src/labgob"
	"src/labrpc"
	"log"
	"src/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0



func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}




type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	maxSeriesID map[int64]int
	kvDB	map[string]string
	opChs	map[int]Op
	ackCh	map[int] chan Op
	indexTotal	int

	// snapShot
	snapshot raft.SnapShot
	lastApplied int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// 不加就会卡在index 153
	//kv.mu.Lock()
	//close(kv.applyCh)
	//kv.mu.Unlock()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	DPrintf("server: [%d] start!", me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 1)



	// You may need initialization code here.
	kv.indexTotal = 0
	kv.kvDB = make(map[string]string)
	kv.maxSeriesID = make(map[int64]int)
	kv.ackCh = make(map[int]chan Op)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	go kv.kvDBUpdate() // only way to change DB
	//kv.rf.InitInstallSnapshotToApp()
	//kv.rf.InitThread()
	go kv.snapshotThread()
	return kv
}

// receive Client Request
// concurrence
func (kv *KVServer) ClientRequest(args *RequestArgs, reply *RequestReply) {

	// Request Outdated
	DPrintf("server: [%d] receive  ClientRequest from clientID: %d seriesID: %d", kv.me, args.ClientID, args.SeriesID)
	kv.mu.Lock()
	if kv.maxSeriesID[args.ClientID] >= args.SeriesID {
		if args.OpType == GET {
			if value, ok := kv.kvDB[args.Key]; ok {
				reply.Value = value
			} else {
				reply.Value = ""
			}
		}
		reply.Err = OK

		kv.mu.Unlock()
		return
	} else {

		kv.mu.Unlock()
	}

	opStruct := Op{
		Key:    args.Key,
		Value:  args.Value,
		OpType: args.OpType,
		ClientID:	args.ClientID,
		SeriesID:	args.SeriesID,
	}

	index, _, isLeader := kv.rf.Start(opStruct)

	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op)
	kv.ackCh[index] = ch

	kv.mu.Unlock()
	//DPrintf("server: [%d] receive ClientRequest from clientID: %d seriesID: %d index: %d", kv.me, args.ClientID, args.SeriesID, index)


	// 直接用kv.ackCh[index]会出现data race
	DPrintf("server: [%d] waitFor reply ClientRequest from clientID: %d seriesID: %d index: %d", kv.me, args.ClientID, args.SeriesID, index)
	select {
	case applyOp := <-ch:
		if applyOp.ClientID != opStruct.ClientID ||
		applyOp.SeriesID != opStruct.SeriesID {
			// 消息丢失，需要重发
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.Lock()
		kv.closeCh(index)
		kv.mu.Unlock()
		break
	case <-time.After(time.Second * 2):
		// 无法提交, figure 8

		reply.Err = ErrWrongLeader
		if kv.killed() == true {
			return
		}
		DPrintf("server: [%d] receive ClientRequest from clientID: %d seriesID: %d index: %d timeout!!!", kv.me, args.ClientID, args.SeriesID, index)
		kv.mu.Lock()
		kv.closeCh(index) // time out后必须删掉这个channel，否则kvUpdate还会往这个管道发消息，但是却收不到
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	if args.OpType == GET {
		if value, ok := kv.kvDB[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	} else {
		reply.Err = OK
	}
	kv.mu.Unlock()
}

// find Leader
func (kv *KVServer) IsLeader(args *FindLeaderArgs, reply *FindLeaderReply) {
	_, isLeader := kv.rf.GetState()
	DPrintf("server: [%d] isLeader: %v", kv.me, isLeader)
	reply.IsLeader = isLeader
	reply.ServerID = kv.me
}

func (kv *KVServer) kvDBUpdate() {
	for kv.killed() == false {
		select {
			case applyMsg := <-kv.applyCh:
				DPrintf("serverID: [%d] kvDBUpdate get applyMsg: %v", kv.me, applyMsg)
				if applyMsg.CommandValid == true {
					kv.mu.Lock()
					kv.lastApplied = applyMsg.CommandIndex
					DPrintf("serverID: [%d] kvDBUpdate get applyMsg lock: %v", kv.me, applyMsg)
					op := applyMsg.Command.(Op)
					if seq, ok := kv.maxSeriesID[op.ClientID]; ok != true || seq < op.SeriesID {
						kv.maxSeriesID[op.ClientID] = op.SeriesID
						switch op.OpType {
						case PUT:
							kv.kvDB[op.Key] = op.Value
							//log.Printf("serverId: [%d] Put key: %s map: %s index: %d ", kv.me, op.Key, kv.kvDB[op.Key],kv.lastApplied)
						case APPEND:
							if _, ok := kv.kvDB[op.Key]; ok {
								oldV := kv.kvDB[op.Key]
								var buffer bytes.Buffer
								buffer.WriteString(oldV)
								buffer.WriteString(op.Value)
								kv.kvDB[op.Key] = buffer.String()
							} else {
								kv.kvDB[op.Key] = op.Value
							}
							//log.Printf("serverId: [%d] Append index: %d key: %s value: %s map: %s", kv.me, kv.lastApplied, op.Key, op.Value, kv.kvDB[op.Key])
						case GET:
							//log.Printf("serverId: [%d] Get  key: %s index: %d", kv.me,  op.Key, kv.lastApplied)
						}
						//DPrintf("serverID: [%d] updateDB index: %d Type: %d map[%v]: %v kv.maxSeriesID[%d]: %d total: %d", kv.me, applyMsg.CommandIndex, op.OpType, op.Key, op.Value, op.ClientID, kv.maxSeriesID[op.ClientID], kv.indexTotal)
					}
					// 即使是重复消息，也要发回去channel，代表已经受理
					if _, ok := kv.ackCh[applyMsg.CommandIndex]; ok {
						DPrintf("serverID: [%d] kvDBUpdate send to channel: %v", kv.me, applyMsg)
						kv.ackCh[applyMsg.CommandIndex] <- op
					} else {
						DPrintf("serverID: [%d] updateDB channel not create index: %d Type: %d map[%v]: %v kv.maxSeriesID[%d]: %d total: %d", kv.me, applyMsg.CommandIndex, op.OpType, op.Key, op.Value, op.ClientID, kv.maxSeriesID[op.ClientID], kv.indexTotal)
					}
					kv.indexTotal++
					DPrintf("serverID: [%d] updateDB finish Op index: %d Type: %d map[%v]: %v kv.maxSeriesID[%d]: %d total: %d", kv.me, applyMsg.CommandIndex, op.OpType, op.Key, kv.kvDB[op.Key], op.ClientID, kv.maxSeriesID[op.ClientID], kv.indexTotal)
					kv.mu.Unlock()
				} else {
					kv.mu.Lock()
					if applyMsg.Snapshot == nil || len(applyMsg.Snapshot) < 1 {
						DPrintf("kv.me: [%d] data == nil || data == nil || len(data) < 1 applyMsg: %v", kv.me, applyMsg)
						kv.kvDB = make(map[string]string)
						kv.maxSeriesID = make(map[int64]int)
						kv.lastApplied = applyMsg.CommandIndex
					} else {
						kv.lastApplied = applyMsg.CommandIndex
						r := bytes.NewBuffer(applyMsg.Snapshot)
						d := labgob.NewDecoder(r)
						if d.Decode(&kv.kvDB) != nil ||
							d.Decode(&kv.maxSeriesID) != nil {
							log.Fatalf("kv.me: [%d] install snapshot decode error", kv.me)
						}
						for k,v := range kv.kvDB {
							log.Printf("serverId: [%d] key: %s value: %s", kv.me, k, v)
						}
						log.Printf("serverId: [%d] installSnapshot finish len(applyMsg.Snapshot): %d kv.lastApplied: %d", kv.me, len(applyMsg.Snapshot), kv.lastApplied)
					}
					kv.mu.Unlock()
				}
		}
	}
}

func (kv *KVServer) closeCh(index int){
	if kv.ackCh[index] == nil {
		return
	}
	close(kv.ackCh[index])
	delete(kv.ackCh, index)
}

// snapshot

func (kv *KVServer) generateSnapshot() {
	kv.snapshot.Kv = DeepCopy(kv.kvDB).(map[string]string)
	kv.snapshot.MaxSeriesID = DeepCopy(kv.maxSeriesID).(map[int64]int)
}

func (kv *KVServer) snapshotThread() {
	for kv.killed() == false {

		var data []byte
		var lasIncludeIndex int
		if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) == true {
			kv.mu.Lock() // 10.16: 放外面会死锁,调了一天
			//DPrintf("serverId: [%d] begin snapshot", kv.me)

			lasIncludeIndex = kv.lastApplied
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvDB)
			e.Encode(kv.maxSeriesID)
			data = w.Bytes()
			//kv.rf.SnapshotCh <- kv.snapshot
			//DPrintf("serverId: [%d] finish snapshot", kv.me)
			//log.Printf("serverId: [%d] snapshotThread lastApplied: %d", kv.me, kv.lastApplied)
			kv.mu.Unlock()
		}

		if data != nil {

			kv.rf.LogTruncate(data, lasIncludeIndex)
		}
		time.Sleep(time.Millisecond * 30)
	}
}