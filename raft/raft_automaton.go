package raft

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/errors"
)

var (
	ErrUndefinedRaftMsg = errors.New("undefined raft message")
	ErrUnmatchedRaftMsg = errors.New("unmatched raft message")
)

type converter func(r *Raft, msg *pb.Message) error

type converterKey struct {
	state   StateType
	msgType pb.MessageType
}

func newConverterKey(state StateType, msgType pb.MessageType) converterKey {
	return converterKey{
		state:   state,
		msgType: msgType,
	}
}

func newRaftAutomaton() *raftAutomaton {
	ra := &raftAutomaton{
		converters: make(map[converterKey]converter),
	}
	ra.build()
	return ra
}

type raftAutomaton struct {
	converters map[converterKey]converter
}

func (ra *raftAutomaton) build() {
	register := func(stat StateType, msgType pb.MessageType, Converter converter) {
		if stat == StateAll {
			ra.converters[newConverterKey(StateFollower, msgType)] = Converter
			ra.converters[newConverterKey(StateCandidate, msgType)] = Converter
			ra.converters[newConverterKey(StateLeader, msgType)] = Converter
			return
		}

		ra.converters[newConverterKey(stat, msgType)] = Converter
	}

	register(StateFollower, pb.MessageType_MsgHup, handleHup)
	register(StateCandidate, pb.MessageType_MsgHup, handleHup)

	register(StateLeader, pb.MessageType_MsgBeat, handleBeat)

	register(StateAll, pb.MessageType_MsgPropose, handlePropose)

	// 日志追加消息状态转换
	register(StateAll, pb.MessageType_MsgAppend, handleAppendEntries)
	register(StateLeader, pb.MessageType_MsgAppendResponse, handleAppendEntriesResp)

	register(StateAll, pb.MessageType_MsgRequestVote, handleRequestVote)
	register(StateCandidate, pb.MessageType_MsgRequestVoteResponse, handleRequestVoteResp)

	register(StateAll, pb.MessageType_MsgHeartbeat, handleHeartBeat)
	register(StateLeader, pb.MessageType_MsgHeartbeatResponse, handleHeartbeatResp)
}

func (ra *raftAutomaton) convert(r *Raft, msg *pb.Message) error {
	converter, ok := ra.converters[newConverterKey(r.State, msg.MsgType)]
	if !ok {
		// 不需要操作
		log.Debug("handle func not found for state and message type: ", r.State, msg.MsgType)
		return nil
	}

	return converter(r, msg)
}

// handleHup 请求节点开始选举
// MsgHup = 0
func handleHup(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive hup from %x\n", r.id, m.From))
	r.startElection()
	return nil
}

// handleHup 告知 Leader 节点发送心跳
// MsgBeat = 1
func handleBeat(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive beat from %x\n", r.id, m.From))

	if r.State != StateLeader {
		return ErrUnmatchedRaftMsg
	}

	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}

	return nil
}

// handlePropose proposes to append data to the leader's log entries.
// MsgPropose = 2
func handlePropose(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive propose from %x\n", r.id, m.From))

	// 不是 leader 或者正在转移 leader 都要返回 error
	if r.State != StateLeader || r.leadTransferee != None {
		return ErrProposalDropped
	}

	r.appendEntry(m.Entries)
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}

		r.sendAppend(pr)
	}

	if len(r.Prs) == 1 {
		r.RaftLog.commitTo(r.Prs[r.id].Match)
	}

	return nil
}

/*
handleAppendEntries Common Msg，用于 Leader 给其他节点同步日志条目
 1. 判断 Msg 的 Term 是否大于等于自己的 Term，是则更新，否则拒绝；
 2. 拒绝，如果 prevLogIndex > r.RaftLog.LastIndex()。否则往下；
 3. 拒绝，如果接收者日志中没有包含这样一个条目：即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上。否则往下；
 4. 追加新条目，同时删除冲突条目，冲突条目的判别方式和论文中的一致；
 5. 记录下当前追加的最后一个条目的 Index。因为在节点更新 committedIndex 时，要比较 Leader 已知已经提交的最高的日志条目的索引
    m.Commit 或者是上一个新条目的索引，然后取两者的最小值。为了记录这个值，我在 RaftLog 中新增了一个字段 lastAppend；
 6. 接受；

MsgAppend = 3;
*/
func handleAppendEntries(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive append from %x\n", r.id, m.From))
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	if r.State == StateLeader {
		return nil
	}
	// 如果领导人的任期小于接收者的当前任期, 拒绝追加 Entry
	if m.Term < r.Term {
		r.sendAppendResp(m.From, r.RaftLog.LastIndex(), true)
		return nil
	}
	if m.From != r.Lead {
		r.Lead = m.From
	}

	preLogIndex := m.Index
	preLogTerm := m.LogTerm

	if preLogIndex > r.RaftLog.LastIndex() {
		r.sendAppendResp(m.From, r.RaftLog.LastIndex(), true)
		return nil
	}

	// 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上 则拒绝
	if tempTerm, _ := r.RaftLog.Term(preLogIndex); tempTerm != preLogTerm {
		r.sendAppendResp(m.From, r.RaftLog.LastIndex(), true)
		return nil
	}

	for _, entry := range m.Entries {
		index := entry.Index
		if index-r.RaftLog.FirstIndex() > uint64(len(r.RaftLog.entries)) || index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			continue
		}

		oldTerm, err := r.RaftLog.Term(index)
		if oldTerm != entry.Term || err != nil {
			if index < r.RaftLog.FirstIndex() {
				r.RaftLog.entries = make([]pb.Entry, 0)
			} else {
				r.RaftLog.entries = r.RaftLog.entries[:index-r.RaftLog.FirstIndex()]
			}
			r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}
	// 追加日志完成
	r.RaftLog.lastAppend = m.Index + uint64(len(m.Entries))
	r.sendAppendResp(m.From, r.RaftLog.LastIndex(), false)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.lastAppend)
	}

	return nil
}

// handleAppendEntriesResp response to log replication request('MessageType_MsgAppend') even reject appending
// MsgAppendResponse = 4
func handleAppendEntriesResp(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive append response from %x\n", r.id, m.From))

	// 同步失败，跳转 next 重新同步
	if m.Reject {
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
		return nil
	}

	// 同步成功, 更新 match 和 next
	r.Prs[m.From].Match, r.Prs[m.From].Next = m.Index, m.Index+1

	commited := r.RaftLog.committed
	r.updateCommitIndex()
	if r.RaftLog.committed != commited {
		for to := range r.Prs {
			if to == r.id {
				continue
			}
			r.sendAppend(to)
		}
	}

	// 如果是正在 transfer 的目标，transfer
	if m.From == r.leadTransferee {
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: m.From})
		if err != nil {
			return err
		}
	}
	return nil
}

// handleRequestVote requests votes for election
// MsgRequestVote = 5
func handleRequestVote(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive RequestVote from %x\n", r.id, m.From))

	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return nil
	}

	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return nil
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if m.LogTerm < lastTerm || (m.LogTerm == lastTerm && m.Index < lastIndex) {
		r.sendRequestVoteResponse(m.From, true)
		return nil
	}

	log.Debug(fmt.Sprintf("%x vote to %x at term %d\n", r.id, m.From, r.Term))
	r.Vote = m.From
	r.sendRequestVoteResponse(m.From, false)
	return nil
}

// handleRequestVoteResp responses from voting request.
// MsgRequestVoteResponse = 6
func handleRequestVoteResp(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive RequestVoteResp from %x\n", r.id, m.From))

	total := len(r.Prs)
	approved, rejected := 0, 0
	r.votes[m.From] = !m.Reject

	for _, isVote := range r.votes {
		if isVote {
			approved++
			continue
		}

		rejected++
	}

	if 2*approved > total {
		r.becomeLeader()
		return nil
	}

	if 2*rejected >= total {
		r.becomeFollower(r.Term, None)
	}

	return nil
}

// handleHeartBeat sends heartbeat from leader to its followers
// MsgHeartbeat = 8
func handleHeartBeat(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive heartbeat from %x\n", r.id, m.From))

	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if m.From != r.Lead {
		r.Lead = m.From
	}

	r.electionElapsed = 0
	r.sendHeartbeatResponse(m.From)

	return nil
}

func handleHeartbeatResp(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive heartbeat resp from %x\n", r.id, m.From))
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	r.heartbeatResp[m.From] = true
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
	return nil
}

func handleSnapshot(r *Raft, m *pb.Message) error {
	log.Debug(fmt.Sprintf("%x receive snapshot from %x\n", r.id, m.From))
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	if r.Term > m.Term {
		return nil
	}
	metaData := m.Snapshot.Metadata
	shotIndex := metaData.Index
	shotTerm := metaData.Term
	//shotConf := metaData.ConfState

	if shotIndex < r.RaftLog.committed || shotTerm < r.RaftLog.FirstIndex() {

	}
	return nil
}
