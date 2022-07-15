package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead       int32
	lastApply  int
	configs    []Config // indexed by config num
	lastOp     map[int64]Op
	notifyChan map[int]chan *CommandReply
}

type Command struct {
	Type      string
	QueryArgs *QueryArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
	JoinArgs  *JoinArgs
}

type CommandReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Op struct {
	// Your data here.
	CommandId int64
	LastReply *CommandReply
}

func Copy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	index, _, isLeader := sc.rf.Start(
		Command{
			Type:     "JoinArgs",
			JoinArgs: args,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("start Join from {%v}", args)
	sc.mu.Lock()
	ch := sc.makeNotifyChan(index)
	sc.mu.Unlock()

	select {
	case r := <-ch:
		reply.Err = r.Err
	case <-time.After(ExecTimeOut):
		reply.Err = ErrTimeOut
	}

	go sc.deleteNotifyChan(index)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	index, _, isLeader := sc.rf.Start(
		Command{
			Type:      "LeaveArgs",
			LeaveArgs: args,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("start Leave from {%v}", args)
	sc.mu.Lock()
	ch := sc.makeNotifyChan(index)
	raft.DPrintf("create notifychan")
	sc.mu.Unlock()
	raft.DPrintf("wait chan")
	select {
	case r := <-ch:
		reply.Err = r.Err
	case <-time.After(ExecTimeOut):
		raft.DPrintf("timeout")
		reply.Err = ErrTimeOut
	}

	go sc.deleteNotifyChan(index)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	index, _, isLeader := sc.rf.Start(
		Command{
			Type:     "MoveArgs",
			MoveArgs: args,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("start Move from {%v}", args)
	sc.mu.Lock()
	ch := sc.makeNotifyChan(index)
	sc.mu.Unlock()

	select {
	case r := <-ch:
		reply.Err = r.Err
	case <-time.After(ExecTimeOut):
		reply.Err = ErrTimeOut
	}

	go sc.deleteNotifyChan(index)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	index, _, isLeader := sc.rf.Start(
		Command{
			Type:      "QueryArgs",
			QueryArgs: args,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	raft.DPrintf("start Query from {%v}", args)
	sc.mu.Lock()
	ch := sc.makeNotifyChan(index)
	sc.mu.Unlock()

	select {
	case r := <-ch:
		reply.Err = r.Err
		reply.Config = r.Config
	case <-time.After(ExecTimeOut):
		reply.Err = ErrTimeOut
	}

	go sc.deleteNotifyChan(index)
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	o, ok := sc.lastOp[clientId]
	return ok && commandId <= o.CommandId
}

func (sc *ShardCtrler) applier() {
	for !sc.Killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				if message.CommandIndex <= sc.lastApply {
					sc.mu.Unlock()
					continue
				}
				sc.lastApply = message.CommandIndex
				cr := &CommandReply{}
				command := message.Command.(Command)
				types := command.Type
				raft.DPrintf("types: {%v}", types)
				switch types {
				case "JoinArgs":
					// args := command.args.(*JoinArgs)
					args := command.JoinArgs
					if sc.isDuplicateRequest(args.ClientId, args.CommandId) {
						cr = sc.lastOp[args.ClientId].LastReply
					} else {
						cr = sc.applyToConfigure(command)
						sc.lastOp[args.ClientId] = Op{CommandId: args.CommandId, LastReply: cr}
					}
				case "LeaveArgs":
					// args := command.args.(*LeaveArgs)
					args := command.LeaveArgs
					if sc.isDuplicateRequest(args.ClientId, args.CommandId) {
						cr = sc.lastOp[args.ClientId].LastReply
					} else {
						cr = sc.applyToConfigure(command)
						sc.lastOp[args.ClientId] = Op{CommandId: args.CommandId, LastReply: cr}
					}
				case "MoveArgs":
					// args := command.args.(*MoveArgs)
					args := command.MoveArgs
					if sc.isDuplicateRequest(args.ClientId, args.CommandId) {
						cr = sc.lastOp[args.ClientId].LastReply
					} else {
						cr = sc.applyToConfigure(command)
						sc.lastOp[args.ClientId] = Op{CommandId: args.CommandId, LastReply: cr}
					}
				case "QueryArgs":
					// args := command.args.(*QueryArgs)
					args := command.QueryArgs
					if sc.isDuplicateRequest(args.ClientId, args.CommandId) {
						cr = sc.lastOp[args.ClientId].LastReply
						raft.DPrintf("duplicate")
					} else {
						raft.DPrintf("start apply to configure")
						cr = sc.applyToConfigure(command)
						sc.lastOp[args.ClientId] = Op{CommandId: args.CommandId, LastReply: cr}
					}
				}
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && currentTerm >= message.CommandTerm {
					ch := sc.makeNotifyChan(message.CommandIndex)
					ch <- cr
				}
				sc.mu.Unlock()
			} else {
				raft.DPrintf("get invalid message %v", message)
			}
		}
	}
}

func (sc *ShardCtrler) makeNotifyChan(index int) chan *CommandReply {
	if _, ok := sc.notifyChan[index]; !ok {
		sc.notifyChan[index] = make(chan *CommandReply, 1)
	}
	return sc.notifyChan[index]
}

func (sc *ShardCtrler) deleteNotifyChan(index int) {
	sc.mu.Lock()
	delete(sc.notifyChan, index)
	sc.mu.Unlock()
}

func getGroupToShardsMap(config Config) map[int][]int {
	groupToShardsMap := make(map[int][]int, 0)
	for gid, _ := range config.Groups {
		groupToShardsMap[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		groupToShardsMap[gid] = append(groupToShardsMap[gid], shard)
	}
	return groupToShardsMap
}

func getGidWithMinAndMaxNumShards(groupToShardsMap map[int][]int) (int, int) {
	gids := make([]int, 0)
	for key := range groupToShardsMap {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	min := NShards + 1
	max := -1
	minIndex := -1
	maxIndex := -1
	for _, gid := range gids {
		if gid != 0 && len(groupToShardsMap[gid]) < min {
			min = len(groupToShardsMap[gid])
			minIndex = gid
		}
		if gid != 0 && len(groupToShardsMap[gid]) > max {
			max = len(groupToShardsMap[gid])
			maxIndex = gid
		}
	}
	if shards, ok := groupToShardsMap[0]; ok && len(shards) > 0 {
		maxIndex = 0
	}
	return minIndex, maxIndex
}

func (sc *ShardCtrler) applyToConfigure(command Command) *CommandReply {
	reply := &CommandReply{}
	types := command.Type
	raft.DPrintf(types, command)
	switch types {
	case "JoinArgs":
		lastConfig := sc.getLastConfig()
		newConfig := Config{
			Num:    len(sc.configs),
			Shards: lastConfig.Shards,
			Groups: Copy(lastConfig.Groups),
		}

		for gid, servers := range command.JoinArgs.Servers {
			if _, ok := newConfig.Groups[gid]; !ok {
				newServers := make([]string, len(servers))
				copy(newServers, servers)
				newConfig.Groups[gid] = newServers
			}
		}
		// balance
		groupToShardsMap := getGroupToShardsMap(newConfig)
		for {
			minGid, maxGid := getGidWithMinAndMaxNumShards(groupToShardsMap)
			if maxGid != 0 && len(groupToShardsMap[maxGid])-len(groupToShardsMap[minGid]) <= 1 {
				break
			}
			groupToShardsMap[minGid] = append(groupToShardsMap[minGid], groupToShardsMap[maxGid][0])
			groupToShardsMap[maxGid] = groupToShardsMap[maxGid][1:]
		}
		var newShards [NShards]int
		for gid, shards := range groupToShardsMap {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
		newConfig.Shards = newShards
		sc.configs = append(sc.configs, newConfig)
	case "LeaveArgs":
		lastConfig := sc.getLastConfig()
		newConfig := Config{
			Num:    len(sc.configs),
			Shards: lastConfig.Shards,
			Groups: Copy(lastConfig.Groups),
		}
		groupToShardsMap := getGroupToShardsMap(newConfig)
		noUsedShards := make([]int, 0)
		for _, gid := range command.LeaveArgs.GIDs {
			if _, ok := newConfig.Groups[gid]; ok {
				delete(newConfig.Groups, gid)
			}
			if shards, ok := groupToShardsMap[gid]; ok {
				noUsedShards = append(noUsedShards, shards...)
				delete(groupToShardsMap, gid)
			}
		}
		var newShards [NShards]int
		if len(newConfig.Groups) > 0 {
			for _, shard := range noUsedShards {
				minGid, _ := getGidWithMinAndMaxNumShards(groupToShardsMap)
				groupToShardsMap[minGid] = append(groupToShardsMap[minGid], shard)
			}
			for gid, shards := range groupToShardsMap {
				for _, shard := range shards {
					newShards[shard] = gid
				}
			}
		}
		newConfig.Shards = newShards
		sc.configs = append(sc.configs, newConfig)

	case "MoveArgs":
		shard := command.MoveArgs.Shard
		gid := command.MoveArgs.GID
		lastConfig := sc.getLastConfig()
		newConfig := Config{
			Num:    len(sc.configs),
			Shards: lastConfig.Shards,
			Groups: Copy(lastConfig.Groups),
		}
		newConfig.Shards[shard] = gid
		sc.configs = append(sc.configs, newConfig)
	case "QueryArgs":
		if num := command.QueryArgs.Num; num < 0 || num >= len(sc.configs) {
			reply.Config = sc.getLastConfig()
		} else {
			reply.Config = sc.configs[num]
		}
	}
	reply.Err = OK
	return reply
}

func (sc *ShardCtrler) getLastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) Killed() bool {
	i := atomic.LoadInt32(&sc.dead)
	return i == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.dead = 0
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(CommandReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApply = 0
	sc.notifyChan = make(map[int]chan *CommandReply)
	sc.lastOp = make(map[int64]Op)

	go sc.applier()

	return sc
}
