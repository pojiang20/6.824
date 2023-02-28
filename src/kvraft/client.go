package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	leaderId int

	debugMode bool
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
	ck.debugMode = true
	return ck
}

var ReqId = 0

func GenReqId() int {
	ReqId++
	return ReqId
}

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
	DPrintf("Get key=%s", key)

	// You will have to modify this function.
	args := &GetArgs{
		Key:   key,
		ReqId: GenReqId(),
	}
	reply := &GetReply{}
	leaderId := ck.leaderId
	for {
		ok := ck._sendGet(leaderId, args, reply)
		if !ok {
			DPrintf("Get rpc not ok")
			continue
		}
		DPrintf("Get return %s", reply.Err)
		switch reply.Err {
		case NoError:
			ck.leaderId = leaderId
			return reply.Value
		case ErrKeyNotExist:
			ck.leaderId = leaderId
			return ""
		case ErrTimeout:
			continue
		case ErrNotLeader:
			fallthrough
		default:
			DPrintf("leaderId[%d] increase", leaderId)
			leaderId = (leaderId + 1) % len(ck.servers)
		}
	}
	return ""
}

func (ck *Clerk) _sendGet(peerId int, args *GetArgs, reply *GetReply) bool {
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
	DPrintf("PutAppend key=%s,value=%s,op=%s", key, value, op)
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:   key,
		Op:    op,
		Value: value,
		ReqId: GenReqId(),
	}
	reply := &PutAppendReply{}
	leaderId := ck.leaderId
	for {
		ok := ck._sendPutAppend(leaderId, args, reply)
		if !ok {
			DPrintf("PutAppend rpc not ok")
			continue
		}
		DPrintf("PutAppend RPC return %s", reply.Err)
		switch reply.Err {
		case NoError:
			ck.leaderId = leaderId
			return
		case ErrKeyNotExist:
			ck.leaderId = leaderId
			DPrintf("set wrong key")
			return
		case ErrTimeout:
			continue
		case ErrNotLeader:
			fallthrough
		default:
			DPrintf("leaderId[%d] increase", leaderId)
			leaderId = (leaderId + 1) % len(ck.servers)
		}
	}
	return
}

func (ck *Clerk) _sendPutAppend(peerId int, args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[peerId].Call("KVServer.PutAppend", args, reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
