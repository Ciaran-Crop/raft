package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command struct {
	*PutAppendArgs
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId int64
	LastReply *PutAppendReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	lastApplied  int
	StateMachine KVStateMachine
	LastOp       map[int64]Op
	notifyChan   map[int]chan *PutAppendReply
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memKV *MemoryKV) Put(key, value string) Err {
	memKV.KV[key] = value
	return OK
}

func (memKV *MemoryKV) Append(key, value string) Err {
	memKV.KV[key] += value
	return OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// send a command to raft
	DPrintf("server %v get a args:{%v}", kv.me, args)
	index, _, isLeader := kv.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case <-ch:
		kv.mu.Lock()
		reply.Value, reply.Err = kv.StateMachine.Get(args.Key)
		DPrintf("server %v 's Key: %v Value: %v", kv.me, args.Key, reply.Value)
		kv.mu.Unlock()
	case <-time.After(ExecTimeOut):
		reply.Err = ErrTimeOut
	}
	go kv.deleteNotifyChan(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Determine whether to repeat(do not)

	// kv.mu.Lock()
	// if kv.isDuplicateRequest(args.ClientId, args.CommandId) {
	// 	e := kv.LastOp[args.ClientId].LastReply.Err
	// 	reply.Err = e
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()

	// send a command to raft

	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server %v get a args:{%v}", kv.me, args)
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	// time.Sleep(10 * time.Second)
	select {
	case re := <-ch:
		reply.Err = re.Err
		DPrintf("server %v send reply:{%v}", kv.me, reply)
	case <-time.After(ExecTimeOut):
		DPrintf("server %v send time out", kv.me)
		reply.Err = ErrTimeOut
	}

	go kv.deleteNotifyChan(index)
}

func (kv *KVServer) deleteNotifyChan(index int) {
	kv.mu.Lock()
	delete(kv.notifyChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case apply := <-kv.applyCh:
			if apply.CommandValid {
				kv.mu.Lock()
				if apply.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = apply.CommandIndex

				reply := new(PutAppendReply)
				command, ok := apply.Command.(Command)
				if ok {
					if kv.isDuplicateRequest(command.ClientId, command.CommandId) {
						reply = kv.LastOp[command.ClientId].LastReply
					} else {
						DPrintf("server %v applyLog Command :{%v}", kv.me, command)
						reply = kv.applyLogToStateMachine(command.PutAppendArgs)
						kv.LastOp[command.ClientId] = Op{CommandId: command.CommandId, LastReply: reply}
					}
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == apply.CommandTerm {
					c := kv.getNotifyChan(apply.CommandIndex)
					c <- reply
				}
				if kv.needSnapshot() {
					// DPrintf("server %v need snapshot", kv.me)
					kv.takeSnapshot(apply.CommandIndex)
				}
				kv.mu.Unlock()
			} else if apply.SnapshotValid {
				// DPrintf("server %v snapshot", kv.me)
				if kv.rf.CondInstallSnapshot(apply.SnapshotTerm, apply.SnapshotIndex, apply.Snapshot) {
					kv.restoreSnapshot(apply.Snapshot)
					kv.lastApplied = apply.SnapshotIndex
				}
			} else {
				DPrintf("unexpected apply %v", apply)
			}
		}
	}
}

func (kv *KVServer) getNotifyChan(index int) chan *PutAppendReply {
	if _, ok := kv.notifyChan[index]; !ok {
		kv.notifyChan[index] = make(chan *PutAppendReply, 1)
	}
	return kv.notifyChan[index]
}

func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int64) bool {
	op, ok := kv.LastOp[clientId]
	return ok && commandId <= op.CommandId
}

func (kv *KVServer) applyLogToStateMachine(putAppendArgs *PutAppendArgs) *PutAppendReply {
	opName := putAppendArgs.Op
	reply := new(PutAppendReply)
	switch opName {
	case "Put":
		err := kv.StateMachine.Put(putAppendArgs.Key, putAppendArgs.Value)
		reply.Err = err
	case "Append":
		err := kv.StateMachine.Append(putAppendArgs.Key, putAppendArgs.Value)
		reply.Err = err
	}
	return reply
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var KvStateMachine MemoryKV
	var LastOp map[int64]Op
	if d.Decode(&KvStateMachine) != nil ||
		d.Decode(&LastOp) != nil {
		log.Fatalf("cannot decode snapshot with {%v} and fail reason is :{%v} and {%v}", snapshot, d.Decode(&KvStateMachine), d.Decode(&LastOp))
	}
	kv.LastOp = LastOp
	kv.StateMachine = &KvStateMachine
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.StateMachine)
	e.Encode(kv.LastOp)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) needSnapshot() bool {
	// attention [ kv.maxraftstate <= kv.rf.GetRaftStateSize() ] may make failed
	// use [ 8*kv.maxraftstate <= kv.rf.GetRaftStateSize() ]
	return kv.maxraftstate != -1 && 8*kv.maxraftstate <= kv.rf.GetRaftStateSize()
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
	// Your code here, if desired.
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.dead = 0
	kv.StateMachine = NewMemoryKV()
	kv.LastOp = make(map[int64]Op)
	kv.notifyChan = make(map[int]chan *PutAppendReply)
	kv.restoreSnapshot(persister.ReadSnapshot())
	go kv.applier()
	labgob.Register(Command{})
	labgob.Register(GetArgs{})
	return kv
}
