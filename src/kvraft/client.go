package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	leaderId  int64
	requestId int64
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
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	return ck
}

const breakTimeMs = time.Duration(20 * time.Millisecond)

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
func (ck *Clerk) Get(key string) string {
	DPrintf("clientId[%d]\t Get key=%s", ck.clientId, key)
	requestId := atomic.AddInt64(&ck.requestId, 1)
	// You will have to modify this function.
	args := &GetArgs{
		Key:       key,
		RequestId: int(requestId),
		ClientId:  int(ck.clientId),
	}
	reply := &GetReply{}

	leaderId := atomic.LoadInt64(&ck.leaderId)
	value := ""
	for {
		ok := ck._sendGet(leaderId, args, reply)
		DPrintf("clientId[%d]\t Get reply=%s", ck.clientId, reply.Err)
		if ok && reply.Err != ErrNotLeader {
			if reply.Err == NoError {
				value = reply.Value
			}
			break
		}
		DPrintf("clientId[%d]\t leaderid=%d change", ck.clientId, leaderId)
		leaderId = (leaderId + 1) % int64(len(ck.servers))
		time.Sleep(breakTimeMs)
	}
	ck.leaderId = leaderId
	DPrintf("clientId[%d]\t get from leader=%d return %s", ck.clientId, leaderId, value)
	return value
}

func (ck *Clerk) _sendGet(peerId int64, args *GetArgs, reply *GetReply) bool {
	return ck.servers[peerId].Call("KVServer.Get", args, reply)
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("clientId[%d]\t 【PutAppend】 key=%s,value=%s,op=%s", ck.clientId, key, value, op)
	// You will have to modify this function.
	requestId := atomic.AddInt64(&ck.requestId, 1)
	leaderId := atomic.LoadInt64(&ck.leaderId)
	args := &PutAppendArgs{
		Key:       key,
		Op:        op,
		Value:     value,
		RequestId: int(requestId),
		ClientId:  int(ck.clientId),
	}
	reply := &PutAppendReply{}
	for {
		ok := ck._sendPutAppend(leaderId, args, reply)
		DPrintf("clientId[%d]\t PutAppend reply=%s", ck.clientId, reply.Err)
		if ok && reply.Err == ErrNotLeader {
			DPrintf("clientId[%d]\t leaderid=%d change", ck.clientId, leaderId)
			leaderId = (leaderId + 1) % int64(len(ck.servers))
			time.Sleep(breakTimeMs)
			continue
		}
		break
	}
	ck.leaderId = leaderId
	DPrintf("clientId[%d]\t PutAppend to leader=%d", ck.clientId, leaderId)
	return
}

func (ck *Clerk) _sendPutAppend(peerId int64, args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[peerId].Call("KVServer.PutAppend", args, reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
