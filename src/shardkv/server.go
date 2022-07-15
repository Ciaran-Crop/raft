package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	GetOp uint8 = iota
	PutAppendOp
	Configuration
	InsertShards
	DeleteShards
	Empty
)

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type Command struct {
	Type          uint8
	GetArgs       *GetArgs
	PutAppendArgs *PutAppendArgs
	Config        *shardctrler.Config
	ShardResponse *ShardResponse
	ShardRequest  *ShardRequest
}

type ShardResponse struct {
}

type ShardRequest struct {
}

type CommandReply struct {
	Err   Err
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId int64
	LastReply *CommandReply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
	mck           *shardctrler.Clerk
	dead          int32
	lastApplied   int
	StateMachine  map[int]*Shard
	LastOp        map[int64]Op
	notifyChan    map[int]chan *CommandReply
}

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func NewShard() *Shard {
	return &Shard{
		KV:     make(map[string]string),
		Status: Serving,
	}
}

func (sh *Shard) Get(key string) (string, Err) {
	if value, ok := sh.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (sh *Shard) Put(key, value string) Err {
	sh.KV[key] = value
	return OK
}

func (sh *Shard) Append(key, value string) Err {
	sh.KV[key] += value
	return OK
}

func (sh *Shard) DeepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range sh.KV {
		newShard[k] = v
	}
	return newShard
}

func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid && (kv.StateMachine[shardId].Status == Serving ||
		kv.StateMachine[shardId].Status == GCing)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		reply.Err = kv.LastOp[args.ClientId].LastReply.Err
		reply.Value = kv.LastOp[args.ClientId].LastReply.Value
		kv.mu.Unlock()
		return
	}

	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(Command{
		Type:    GetOp,
		GetArgs: args,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case re := <-ch:
		reply.Err = re.Err
		reply.Value = re.Value
	case <-time.After(ExecTimeOut):
		reply.Err = ErrTimeOut
	}

	go kv.deleteNotifyChan(index)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		reply.Err = kv.LastOp[args.ClientId].LastReply.Err
		kv.mu.Unlock()
		return
	}
	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Command{
		Type:          PutAppendOp,
		PutAppendArgs: args,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case re := <-ch:
		reply.Err = re.Err
	case <-time.After(ExecTimeOut):
		reply.Err = ErrTimeOut
	}

	go kv.deleteNotifyChan(index)
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, commandId int64) bool {
	op, ok := kv.LastOp[clientId]
	return ok && commandId <= op.CommandId
}

func (kv *ShardKV) deleteNotifyChan(index int) {
	kv.mu.Lock()
	delete(kv.notifyChan, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChan[index]; !ok {
		kv.notifyChan[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChan[index]
}

func (kv *ShardKV) applyLogToStateMachine(command Command) *CommandReply {
	commandReply := &CommandReply{}
	switch command.Type {
	case GetOp:
		if kv.canServe(key2shard(command.GetArgs.Key)) {
			s, e := kv.StateMachine[key2shard(command.GetArgs.Key)].Get(command.GetArgs.Key)
			commandReply.Err = e
			commandReply.Value = s
			return commandReply
		}
	case PutAppendOp:
		if kv.canServe(key2shard(command.GetArgs.Key)) {
			switch command.PutAppendArgs.Op {
			case "Put":
				e := kv.StateMachine[key2shard(command.PutAppendArgs.Key)].Put(command.PutAppendArgs.Key, command.PutAppendArgs.Value)
				commandReply.Err = e
			case "Append":
				e := kv.StateMachine[key2shard(command.PutAppendArgs.Key)].Append(command.PutAppendArgs.Key, command.PutAppendArgs.Value)
				commandReply.Err = e
			}
			return commandReply
		}
	}
	return &CommandReply{
		Err:   ErrWrongGroup,
		Value: "",
	}
}

func (kv *ShardKV) applier() {
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
				reply := new(CommandReply)
				command, ok := apply.Command.(Command)
				if ok {
					switch command.Type {
					case GetOp:
						args := command.GetArgs
						if kv.isDuplicateRequest(args.ClientId, args.CommandId) {
							reply = &CommandReply{
								Err:   kv.LastOp[args.ClientId].LastReply.Err,
								Value: kv.LastOp[args.ClientId].LastReply.Value,
							}
						} else {
							reply = kv.applyLogToStateMachine(command)
							kv.LastOp[args.ClientId] = Op{
								CommandId: args.CommandId,
								LastReply: reply,
							}
						}
					case PutAppendOp:
						args := command.PutAppendArgs
						if kv.isDuplicateRequest(args.ClientId, args.CommandId) {
							reply = &CommandReply{
								Err: kv.LastOp[args.ClientId].LastReply.Err,
							}
						} else {
							reply = kv.applyLogToStateMachine(command)
							kv.LastOp[args.ClientId] = Op{
								CommandId: args.CommandId,
								LastReply: reply,
							}
						}
					case Configuration:
						reply = kv.applyConfiguration(command.Config)
					case InsertShards:
						reply = kv.applyInsertShards(command.ShardResponse)
					case DeleteShards:
						reply = kv.applyDeleteShards(command.ShardRequest)
					case Empty:
						reply = kv.applyEmptyEntry()
					}
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == apply.CommandTerm {
					c := kv.getNotifyChan(apply.CommandIndex)
					c <- reply
				}
				// snapshot
				if kv.needSnapshot() {
					kv.takeSnapshot(apply.CommandIndex)
				}
				kv.mu.Unlock()
			} else if apply.SnapshotValid {
				// snapshot
				if kv.rf.CondInstallSnapshot(apply.SnapshotTerm, apply.SnapshotIndex, apply.Snapshot) {
					kv.restoreSnapshot(apply.Snapshot)
					kv.lastApplied = apply.SnapshotIndex
				}
			} else {
				raft.DPrintf("unexpected apply %v", apply)
			}
		}
	}
}

func (kv *ShardKV) updateShardStatus(config *shardctrler.Config) {

}

func (kv *ShardKV) Execute(command Command) {
	index, _, isLeader := kv.rf.Start(command)
	reply := CommandReply{}
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case re := <-ch:
		reply.Err = re.Err
	case <-time.After(ExecTimeOut):
		reply.Err = ErrTimeOut
	}

	go kv.deleteNotifyChan(index)
}

func (kv *ShardKV) configure() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.StateMachine {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}

	currentConfigNum := kv.currentConfig.Num
	kv.mu.Unlock()

	if canPerformNextConfig {
		nextConfig := kv.mck.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			kv.Execute(Command{Type: Configuration, Config: &nextConfig})
		}
	}
}

func (kv *ShardKV) applyConfiguration(config *shardctrler.Config) *CommandReply {
	if config.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(config)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *config
		return &CommandReply{OK, ""}
	}
	return &CommandReply{ErrTimeOut, ""}
}

func (kv *ShardKV) applyInsertShards(shardResponse *ShardResponse) *CommandReply {

}

func (kv *ShardKV) applyDeleteShards(shardRequest *ShardRequest) *CommandReply {

}

func (kv *ShardKV) applyEmptyEntry() *CommandReply {

}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var KvStateMachine map[int]*Shard
	var LastOp map[int64]Op
	if d.Decode(&KvStateMachine) != nil ||
		d.Decode(&LastOp) != nil {
		log.Fatalf("cannot decode snapshot with {%v} and fail reason is :{%v} and {%v}", snapshot, d.Decode(&KvStateMachine), d.Decode(&LastOp))
	}
	kv.LastOp = LastOp
	kv.StateMachine = KvStateMachine
}

func (kv *ShardKV) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.StateMachine)
	e.Encode(kv.LastOp)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) needSnapshot() bool {
	// attention [ kv.maxraftstate <= kv.rf.GetRaftStateSize() ] may make failed
	// use [ 8*kv.maxraftstate <= kv.rf.GetRaftStateSize() ]
	return kv.maxraftstate != -1 && 8*kv.maxraftstate <= kv.rf.GetRaftStateSize()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(CommandReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.dead = 0

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	kv.StateMachine = make(map[int]*Shard)
	kv.LastOp = make(map[int64]Op)
	kv.notifyChan = make(map[int]chan *CommandReply)

	go kv.applier()

	return kv
}
