# 6.824

### 2A
#### 发起投票是异步的
发送是这样一种情况：一次性发送完所有请求，然后根据网络情况接收响应，当接收到返回值则`voteNum++`，符合Majority则变成`Leader`，那些回复地晚的忽略即可。
#### RPC操作后的状态判断
在`requestVote`后，需要进行角色判断和任期判断。
#### 如何设定心跳间隙?
最开始设置100ms，会发现有timeout。

这种写法有问题
```go
rf.mu.Lock()
if rf.State == Leader {
    continue
}
//如果最新的选举时间比睡眠前的时间都要新（即睡眠这个阶段没有任何时间刷新），则需要发起选举
if rf.latestElectionTime.Before(timeBeforeSleep) {
    go rf.StartElection()
}
rf.mu.Unlock()
```