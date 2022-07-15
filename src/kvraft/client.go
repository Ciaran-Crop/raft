package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	leaderId  int64
	commandId int64
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
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
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
	getArgs := new(GetArgs)
	getArgs.ClientId = ck.clientId
	getArgs.CommandId = ck.commandId
	getArgs.Key = key
	for {
		getReply := new(GetReply)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", getArgs, getReply)
		if !ok || getReply.Err == ErrWrongLeader || getReply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			// time.Sleep(1 * time.Second)
			continue
		}
		if getReply.Err == ErrNoKey {
			return ""
		}
		ck.commandId++
		return getReply.Value
	}
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
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putAppendArgs := new(PutAppendArgs)
	putAppendArgs.ClientId = ck.clientId
	putAppendArgs.CommandId = ck.commandId
	putAppendArgs.Key = key
	putAppendArgs.Value = value
	putAppendArgs.Op = op
	for {
		putAppendReply := new(PutAppendReply)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", putAppendArgs, putAppendReply)
		if !ok || putAppendReply.Err == ErrWrongLeader || putAppendReply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			// time.Sleep(1 * time.Second)
			continue
		}
		ck.commandId++
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
