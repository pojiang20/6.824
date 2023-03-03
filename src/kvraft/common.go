package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	NoError        = Err("Ok")
	ErrNotLeader   = Err("Not Leader")
	ErrTimeout     = Err("time out")
	ErrKeyNotExist = Err("key not exist")
	ErrNotSupport  = Err("Not support this method")
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int
	ClientId  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int
	ClientId  int
}

type GetReply struct {
	Err   Err
	Value string
}
