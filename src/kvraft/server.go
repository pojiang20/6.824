package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Val       string
	RequestId int //用于RetMsgMap中op和retCh的映射
	ClientId  int //表示客户端身份
	MsgId     int //与日志中该op的位置对应
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//内存存储数据
	data map[string]string
	//返回值
	RetMsgMap map[int]chan RetMsg
	//记录每一个客户端最后一个被应用的ID
	lastApplied map[int]int
}

type RetMsg struct {
	val string
	err Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("KVServer.Get, requestId=%d clientId=%d", args.RequestId, args.ClientId)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrNotLeader
		return
	}
	// Your code here.
	op := Op{
		OpType:    GET,
		Key:       args.Key,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	reply.Value, reply.Err = kv.OpRun(op, args.RequestId)
	DPrintf("KVServer.Get\t reply:[+%v]", reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Call.PutAppend, requestId=%d clientId=%d op=%s", args.RequestId, args.ClientId, args.Op)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrNotLeader
		return
	}
	// Your code here.
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Val:       args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	_, err := kv.OpRun(op, args.RequestId)
	reply.Err = err
	DPrintf("KVServer.PutAppend\t reply:%s", reply.Err)
}

func (kv *KVServer) OpRun(op Op, RequestId int) (ret string, err Err) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrNotLeader
	}
	retMsgCh := kv.getCh(RequestId)

	select {
	case retMgs := <-retMsgCh:
		kv.removeCh(RequestId)
		return retMgs.val, retMgs.err
	case <-time.After(600 * time.Millisecond):
		return "", ErrTimeout
	}
}

func (kv *KVServer) getCh(RequestId int) (ret chan RetMsg) {
	kv.mu.Lock()
	if _, ok := kv.RetMsgMap[RequestId]; !ok {
		retMsgCh := make(chan RetMsg, 1)
		kv.RetMsgMap[RequestId] = retMsgCh
	}
	ret = kv.RetMsgMap[RequestId]
	kv.mu.Unlock()
	return
}

func (kv *KVServer) removeCh(RequestId int) {
	kv.mu.Lock()
	delete(kv.RetMsgMap, RequestId)
	kv.mu.Unlock()
}

func (kv *KVServer) isRepeated(clientId int, msgId int) bool {
	if val, ok := kv.lastApplied[clientId]; ok {
		return msgId == val
	}
	return false
}

func (kv *KVServer) Applier() {
	DPrintf("kvId=%d, Start Applier", kv.me)
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				DPrintf("kvId=%d,Applier get op: [%+v]", kv.me, op)
				if retMsgCh, ok := kv.RetMsgMap[op.RequestId]; ok {
					ret := kv.applyToStateMachine(op)
					retMsgCh <- ret
				}
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) (ret RetMsg) {
	switch op.OpType {
	case GET:
		key := op.Key
		if val, ok := kv.data[key]; ok {
			ret = RetMsg{val: val, err: NoError}
		} else {
			ret = RetMsg{val: "", err: ErrKeyNotExist}
		}
	case PUT:
		k, v := op.Key, op.Val
		kv.data[k] = v
		ret = RetMsg{err: NoError}
	case APPEND:
		key, v := op.Key, op.Val
		if val, ok := kv.data[key]; ok {
			kv.data[key] = val + v
			ret = RetMsg{err: NoError}
		} else {
			ret = RetMsg{err: ErrKeyNotExist}
		}
	default:
		//不支持
		ret = RetMsg{err: ErrNotSupport}
	}
	DPrintf("【op】%+v【ret】%+v", op, ret)
	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
	kv.data = make(map[string]string)
	kv.RetMsgMap = make(map[int]chan RetMsg)
	kv.lastApplied = make(map[int]int)

	go kv.Applier()

	return kv
}
