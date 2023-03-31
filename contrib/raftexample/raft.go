// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"go.uber.org/zap"
)

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

// A key-value stream backed by raft
//
// raftNode 中主要实现的功能如下：
//	- 将客户端发来的请求传递给底层 etcd-raft 组件中进行处理
//	- 从 node.readyc 通道中读取 Ready 实例，并处理其中封装的数据
//	- 管理 WAL 日志文件
//	- 管理快照数据
//	- 管理逻辑时钟
//	- 将 etcd-raft 模块返回的待发送消息通过网络组建发送到指定节点
//
type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *commit           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	// 节点 id
	id          int      // client ID for raft session
	// 节点 url
	peers       []string // raft peer URLs
	// 如果节点是以加入已有集群的方式启动，那么该值为 true ，否则 false 。
	join        bool     // node is joining an existing cluster
	// 预写日志（WAL）的路径
	waldir      string   // path to WAL directory
	// 保存快照的目录路径
	snapdir     string   // path to snapshot directory
	// 获取快照的方法签名
	getSnapshot func() ([]byte, error)

	confState     raftpb.ConfState 	// 集群配置状态
	snapshotIndex uint64 	// 快照的最后一条日志的索引
	appliedIndex  uint64 	// 已应用的最后一条日志的索引

	// raft backing for the commit/error channel
	node        raft.Node				// etcd-raft 的核心接口，对于一个最简单的实现来说，开发者只需要与该接口打交道即可实现基于 raft 的服务。
	raftStorage *raft.MemoryStorage		// etcd-raft 的状态缓存，存储所有未 snapshot 的 entries ，用于同步给集群中其它 follower 。
	wal         *wal.WAL				// 预写日志，确保持久化

	// 快照管理器
	snapshotter      *snap.Snapshotter
	// 一个用来发送 snapshotter 加载完毕的信号的 “一次性” 信道。
	// 因为 snapshotter 的创建对于新建 raftNode 来说是一个异步的过程，因此需要通过该信道来通知创建者 snapshotter 已经加载完成。
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	// 当 wal 中的日志超过该值时，触发快照操作并压缩日志
	snapCount uint64
	// 节点间通信接口
	transport *rafthttp.Transport


	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(
	id int,
	peers []string,
	join bool,
	getSnapshot func() ([]byte, error),
	proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange,
) (
	<-chan *commit,
	<-chan error,
	<-chan *snap.Snapshotter,
) {

	// commitC 用于 raft 向用户代码传输已经提交的 entries ，用户执行具体业务逻辑，将数据写入 State Machine 。
	commitC := make(chan *commit)
	// errorC  用于 raft 向用户代码暴露 raft 库的处理异常
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,		// 用户代码向 raft 提交写请求 Propose
		confChangeC: confChangeC,	//
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,

		// 初始化存放 WAL 日志和 SnapShot 文件的目录
		waldir:      fmt.Sprintf("raftexample-%d", id),
		snapdir:     fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,

		// 创建 stopc、httpstopc、httpdonec 和 snapshotterReady 四个通道
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		logger: zap.NewExample(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),

		// rest of structure populated after WAL replay
		// 其余字段在 WAL 日志回放完成之后才会初始化
	}

	// 单独启动一个 goroutine 执行 startRaft() 方法，在该方法中完成 raftNode 初始化操作
	go rc.startRaft()

	// 将 commitC、errorC 和 snapshotterReady 三个通道返回给上层应用
	return commitC, errorC, rc.snapshotterReady
}

// saveSnap() 方法会将新生成的快照数据、快照索引保存到磁盘上，还会根据快照的元数据释放部分旧 WAL 日志文件的句柄。
func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// [重要]
	// 根据快照 snap 生成一条 `SnapshotType` 类型的 WAL 日志条目，该条目用于索引快照信息。
	// 这个日志对 WAL 至关重要，因为其决定了日志压缩的时候哪些可以被回收，也决定了日志重放的时候哪些可以被略过。
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,		// 快照的最后一条日志 index
		Term:      snap.Metadata.Term,		// 快照的最后一条日志 term
		ConfState: &snap.Metadata.ConfState,
	}

	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	//
	// 将快照存入 snapDir
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}

	// 将快照索引条目存入 walDir
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}

	// 根据快照的 Last Index 清理 wal 旧的日志，因为 Index 之前的 wal 日志已经不需要了
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

// entriesToApply 获取要应用到状态机的日志条目，即 index 位于 lastApply 到 commit index 之间的日志。
func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}

	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}

	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}

	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
//
// publishEntries 将 ready 对象里的 CommittedEntries 发送到 commitC ，由应用层 (kvstore) 处理。
// 然后更新 raftNode.appliedIndex 为当前日志的 Index 。
//
// publishEntries 返回一个 chan ，以便 kvstore 通知 raftNode 数据存储成功。
//
// publishEntries 会遍历传入的日志列表，对于普通的日志条目，先将其反序列化，通过 commitC 信道传给 kvstore 处理；
// 对于用于变更集群配置的日志，则根据变更的内容（如增加或删除集群中的某个节点），修改通信模块中的相关记录。
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	// 遍历 entries
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			// 忽略空消息
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			// 存入数组
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			// 解析消息
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			// 应用配置
			rc.confState = *rc.node.ApplyConfChange(cc)
			// 更新状态
			switch cc.Type {
			case raftpb.ConfChangeAddNode: // 新增节点
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode: // 删除节点
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	// 数据为空，可能是因为不存在 `EntryNormal` 类型日志，无需应用，此时返回的 applyDoneC 为 nil
	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		// 将组装好的 data 通过 commitC 传给 kvstore 处理，当 kvstore 处理完成后，再通过 applyDoneC 通道通知过来
		case rc.commitC <- &commit{data, applyDoneC}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	// 只要投递到 commitC 中，就更新 appliedIndex 。
	//
	// 因为这些 committed entries 可能还未被应用到 state machine ，所以可能造成 state machine 和 appliedIndex 的不一致。
	//
	// 快照 snapshot 中会包含 <state machine, applied index> ，如果此时执行快照，这种不一致可能导致宕机重启后，系统处于错误状态。
	// 所以，在执行 snapshot 前会等待所有 committed entries 执行完毕，系统处于一致状态后再操作。
	//
	// 如果不执行 snapshot ，那么完全不用 care 应用层是否已经 applied ，因为即便宕机，最近的 snapshot 也是正确的。
	//
	// 因此，出于性能的考虑，这里直接更新，在有需要的地方（快照）再等待 applyDoneC 通知。
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

// 快照加载：从 wal 目录中取出所有 `snapshotType` 类型日志条目，进而到 snap 目录中获取最新可用的 snapshot 对象。
func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	// WAL 目录是否存在
	if wal.Exist(rc.waldir) {
		// 首先从 WAL 文件中提取出所有 snapshotType 类型的日志条目，其描述了 snapshot 的元数据。
		// wal.ValidSnapshotEntries() 返回 rc.waldir 目录中 wal 日志中的所有有效快照条目（ snapshotType 的 wal 日志条目）。
		walSnaps, err := wal.ValidSnapshotEntries(rc.logger, rc.waldir)
		if err != nil {
			log.Fatalf("raftexample: error listing snapshots (%v)", err)
		}
		// 通过这些 snapshot 元数据，找到最新的可用的 snapshot 数据，构建 sanpshot 对象并返回 (这里返回的已经是实际 snapshot ，而非 walSnapshot )
		// rc.snapshotter.LoadNewestAvailable() 函数在 snapdir 中查找匹配的快照对象，由于 WAL 是线性写入的，
		// 后写入的 Entry 最新，因此从 snap entry 中找到最后匹配元数据的 snapshot ，即为 NewestAvailable 对象。
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatalf("raftexample: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	// 检测 WAL 日志目录是否存在，如果不存在进行创建
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}
		// 新建 WAL 实例，其中会创建相应目录和一个空的 WAL 日志文件
		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		// 关闭 WAL, 其中包括各种关闭目录、文件和相关的 goroutine
		w.Close()
	}

	// 提取出 snapshot 的 term/index ，二者确定了读取 wal 的起始偏移。
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)

	// 打开 wal 实例，根据 snapshot 的 term/index 定位到起始偏移，后续的 entries 需要 redo
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}



//a. 将 snapshot 文件中的内容读出来。
//b. 并根据该内容中 last include Index 和 last include Term 将 wal 文件中对应的之后的日志条目内容读出来。
//c. 调用 ApplySnapshot 将读到的 snapshot 放到 raftStorage 的 snapshot 中。
//   调用 SetHardState 将 hardState 内容放到 raftStorage 的 hardState 中。
//   调用 Append(ents) 将 wal 读出的日志条目，放到 raftStorage 的 ents 中。
//d. 标 lastIndex ,发送 nil 给 commitC（ nil 的作用是告知接收到的地方，需要处理 snapshot ）。

// replayWAL replays WAL entries into the raft instance.
//
// 节点重新启动时，会调用 replayWAL 对 snapshot 以及 wal 中的日志条目进行回放。
//
// 步骤:
//  - 首先会读取快照数据，在快照数据中记录了该快照包含的最后一条 Entry 记录的 Term 值 和 索引值 。
//  - 然后根据 Term 值 和 索引值 确定读取 WAL 日志文件的位置，并进行日志记录的读取。
//  - 最后将读取到的快照数据、WAL日志记录和状态信息保存到 raftNode.raftStorage（即 etcd-raft 模块中 raftLog.storage ）中。
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)

	// 1. 加载快照：从 wal 中取出所有 `snapshotType` 类型日志条目，进而到 snap 目录中获取最新可用的 snapshot 对象。
	snapshot := rc.loadSnapshot()

	// 2. 打开 wal 日志，根据 snapshot 的 term/index 定位到起始偏移，后续的 entries 需要 redo 。
	w := rc.openWAL(snapshot)

	// 3. 从 wal 日志中读取所有待 redo 的日志条目
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}

	// 4. 构建 storage
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		// 5. 将快照加载到 storage
		rc.raftStorage.ApplySnapshot(*snapshot)
	}

	// 5. 将 HardState 更新到 storage
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	// 6. 将 wal redo 日志项保存到 storage
	rc.raftStorage.Append(ents)
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

// startRaft() 函数核心步骤：
//	- 创建 Snapshotter，并将该 Snapshotter 实例返回给上层模块
//	- 创建 WAL 实例，然后加载快照并回放 WAL 日志
//	- 创建 raft.Config 实例，其中包含了启动 etcd-raft 模块的所有配置
//	- 初始化底层 etcd-raft 模块，得到 node 实例
//	- 创建 Transport 实例，该实例负责集群中各个节点之间的网络通信，其具体实现在 raft-http 包中
//	- 建立与集群中其他节点的网络连接
//	- 启动网络组件，其中会监听当前节点与集群中其他节点之间的网络连接，并进行节点之间的消息读写
//	- 启动两个后台的 goroutine，它们主要工作是处理上层模块与底层 etcd-raft 模块的交互，但处理的具体内容不同，后面会详细介绍这两个 goroutine 的处理流程。
func (rc *raftNode) startRaft() {

	// 首先，startRaft 方法检查快照目录是否存在，该目录用于存放定期生成的快照数据；
	// 如果不存在则进行创建，然后创建基于该目录的快照管理器，若创建失败，则输出异常日志并终止程序。
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}

	// 1. 基于 snapdir 目录创建快照管理器，创建完成后，向 snapshotterReady 信道写入该快照管理器，通知其快照管理器已经创建完成。
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)

	// 2. 检测 waldir 目录下是否存在旧的 WAL 日志文件
	oldwal := wal.Exist(rc.waldir)

	// 2. 初始化 WAL 实例
	rc.wal = rc.replayWAL()

	// signal replay has finished
	// 通知快照管理器已经创建完成。
	rc.snapshotterReady <- rc.snapshotter

	// 3. 遍历为每一个 peer 节点分配 id ，从 1 开始
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	// 初始化 raft 配置
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,	// 选举超时
		HeartbeatTick:             1,	// 心跳超时
		Storage:                   rc.raftStorage,	// 存储
		MaxSizePerMsg:             1024 * 1024,	// 每条消息的最大长度
		MaxInflightMsgs:           256,	// 已发送但是未收到响应的消息上限个数
		MaxUncommittedEntriesSize: 1 << 30,
	}

	// 4. 初始化底层的 etcd-raft 模块，这里会根据 WAL 日志的回放情况，判断当前节点是首次启动还是重新启动
	//
	// 如果存在 wal 日志，或者节点是加入已有集群（非第一次启动）
	if oldwal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		// 启动底层 raft 的协议实体 node
		rc.node = raft.StartNode(c, rpeers)
	}

	// 5. 创建 Transport 实例并启动，他负责 raft 节点之间的网络通信服务
	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	// 启动网络服务相关组件
	rc.transport.Start()

	// 6. 建立与集群中其他各个节点的连接
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	// 7. 启动一个 goroutine 处理本节点与其它节点通信的 http 服务
	go rc.serveRaft()

	// 8. 启动一个 goroutine 处理 raftNode 与 底层 raft 通过通道来进行通信
	go rc.serveChannels()
}


// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

// publishSnapshot() 方法会通知上层模块加载新生成的快照数据，并使用新快照的元数据更新 raftNode 中的相应字段。
func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	// 是否为空
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	// 快照过旧
	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}

	// 通过 commitC 通知上层应用加载新快照
	rc.commitC <- nil // trigger kvstore to load snapshot

	// 更新最新快照的元数据
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000



// [快照管理]
// 快照(snapshot)本质是对日志进行压缩，它是对状态机某一时刻（或者日志的某一索引）的状态的保存。
// 快照操作可以缓解日志文件无限制增长的问题，一旦达日志项达到某一临界值，可以将内存的状态数据进行压缩成为 snapshot 文件并存储在快照目录。
//
// 生成的快照包含两个方面的数据：
//	- 一个显然是实际的内存状态机中的数据，一般将它存储到当前的快照目录中。
//  - 另外一个为快照的索引数据，即当前快照的索引信息，换言之，即记录下当前已经被执行快照的日志的索引编号，
//    因为在此索引之前的日志不需要执行重放操作，因此也不需要被 WAL 日志管理。快照的索引数据一般存储在日志目录下。
func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	// 1. 只有当前已经提交应用的日志的数据达到 rc.snapCount 才会触发快照操作
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	//
	// [重要]
	// 如果 applyDoneC 非 nil ，意味着有 entries 等待应用层 apply ，需要等待其应用完，否则快照中的数据可能不一致。
	//
	//
	// 需要注意到，在 publishEntries 中，只要把 entries 投递到 commitC 管道，就直接更新 rc.appliedIndex ，没等待应用层回复 apply done 通知。
	// 这个过程中，可能存在不一致。而执行 snapshot 会将 rc.appliedIndex 和状态机快照一同落盘，就会导致一致性问题。
	//
	// 如果在 publishEntries 执行后、状态机应用前，未发生 snapshot ，若此时宕机，内存最新的 rc.appliedIndex 会丢失，
	// 磁盘中 appliedIndex 和状态机仍旧是一致的，重启后会从之前的快照中加载并 redo + reapply，没有任何问题。
	//
	// 如果在 publishEntries 执行后、状态机应用前，发生 snapshot ，此时的快照和 appliedIndex 是不一致的，若此时宕机，
	// 重启后加载的数据是错误的。
	//
	// 所以，必须等待 committed entries 提交后，state machine 和 appliedIndex 一致的情况下，执行快照操作。
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	// 2. 获取应用层的快照数据，此函数由应用层自定义
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}

	// 3. 创建 Snapshot 实例：将快照数据、applied index、集群配置等封装成 pb.Snapshot 对象，存储到 storage 中后返回
	//
	// 将快照存储到 storage 的目的，是为了将来传递给滞后的 follower 来同步数据。
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}

	// 4. 将快照落盘存储：存入 wal 和 sap 目录
	//	- Case1: 异常宕机、主动重启时会从磁盘中读取快照，来恢复状态机
	//	- Case2: 正常运行时，应用层会从磁盘中读取快照，来更新状态机（每当触发快照，需要通知应用层加载，应用层会从磁盘中拉取这个快照并应用）
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	// 5. 判断是否达到阶段性整理内存日志的条件，若达到，则将内存中的数据进行阶段性整理标记
	//
	// 快照的直接目的是减少 wal 日志，在 rc.saveSnap(snap) 过程中会完成。
	//
	// 而 storage 为了能够支持向 follower 同步数据，需要存储最近一个快照及后续的所有日志。
	// 随着运行时间增长，日志越来越多，浪费内存空间。所以，当创建新快照后，便可以清理 storage 中的日志。
	//
	// 将 MemoryStorage 中的日志(ents)截断至 compactIndex ，这里保留了 commitIndex 往前 10000 条日志。
	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	// 将日志截断至 compactIndex
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)

	// 6. 最后更新当前已快照的日志索引
	rc.snapshotIndex = rc.appliedIndex
}


// [重要]
//
// raftNode 会将从 kvstore 接收到的用户对状态机的更新请求（proposeC/confChangeC）传递给底层 raft 核心库来处理。
// 此后，raftNode 会阻塞直至收到由 raft 组件传回的 Ready 指令。
//
// serverChannel 完成核心逻辑：
//	读取用户提交数据，提交给raft node处理；
//	将 entries 持久化、同步给 peer 节点；
//	将已经commit成功的entries写入commitC；
func (rc *raftNode) serveChannels() {

	// 读取当前快照
	//
	// 服务启动后，会通过 rc.replayWAL() 从磁盘中加载快照和日志，过程中会将最新 snapshot (如果有) 存入 raftStorage 。
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}

	// 初始化 snapshot 相关属性
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	// 初始化定时器，驱动 raft 逻辑时钟。
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	// 开启 go routine 以接收应用层(kvstore)的请求，并转发给 raftNode 来处理
	go func() {
		// 集群配置变更次数
		confChangeCount := uint64(0)
		// 循环监听来自 kvstore 的请求消息（包括正常的日志请求及集群配置变更请求）
		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			// 1. 正常的日志请求
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					// 调用底层的 raft 核心库的 node 的 Propose 接口来处理请求
					rc.node.Propose(context.TODO(), []byte(prop))
				}
			// 2. 配置变更请求
			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					// 调用底层的 raft 核心库的 node 的 ProposeConfChange 接口来处理请求
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// raft 这个包只实现了 raft 协议，其余的例如数据持久化、处理数据等等，需要这个包的调用者来做：
	//	- 调用 Node.Ready() 接受目前产生的更新，然后：
	//		- 把 HardState，Entries 和 Snapshot 持久化到硬盘里
	//		- 把信息发送到 To 所指定的节点里去
	//		- 把快照和已经提交的 Entry 应用到状态机里去
	//		- 调用 Node.Advance() 通知 Node 之前调用 Node.Ready() 所接受的数据已经处理完毕
	//	- 所有持久化的操作都必须使用满足 Storage 这个接口的实现来进行持久化
	//	- 当接收到其他节点发来的消息时，调用 Node.Step 这个函数
	//	- 每隔一段时间需要主动调用一次 Node.Tick()

	// event loop on raft state machine updates
	// 开启 go routine 以循环处理底层 raft 核心库通过 Ready 通道发送给 raftNode 的指令
	for {
		select {
		case <-ticker.C:
			// ticker 定时器会推进 etcd-raft 组件的逻辑时钟
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		// 通过 Ready 获取 raft 核心库传递的内容
		case rd := <-rc.node.Ready():

			// Must save the snapshot file and WAL snapshot entry before saving any other entries
			// or hardstate to ensure that recovery after a snapshot restore is possible.
			//
			// 持久化：如果 rd.Snapshot 不为空，则将快照数据、快照索引落盘（存入 wal, sap 目录中，并按需清理旧 wal ）。
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
			}

			// 持久化：将集群状态、日志条目写入 WAL 日志。
			rc.wal.Save(rd.HardState, rd.Entries)

			// 加载快照：如果 rd.Snapshot 不为空，将新快照数据存入 raftStorage 中，并通知应用层加载新快照。
			if !raft.IsEmptySnap(rd.Snapshot) {
				//[重要]
				// 保存到 storage 中，目的是将来有过于滞后的 follower 时，可以同步给它。
				// 要记住，storage 存在的目的就是为了给 follower 同步完整的状态。
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				//[重要]
				// 通过 rc.commitC 通知应用层加载快照，应用层收到通知后，会从磁盘中加载快照，然后全量更新状态机。
				// 这里有一点疑惑是，为什么应用层不能感知到 node 层的 storage ???
				rc.publishSnapshot(rd.Snapshot)
			}

			// 将日志条目存入 storage
			rc.raftStorage.Append(rd.Entries)
			// 将待发送的消息发送到指定节点
			rc.transport.Send(rc.processMessages(rd.Messages))

			// [重要]
			// 将已提交、待应用的记录应用到状态机中
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}

			// [重要]
			// 如果有必要，会触发一次快照
			//
			// 随着节点的运行， WAL 日志量和 storage 中的 Entry 记录会不断增加 ，前者浪费磁盘后者浪费内存，
			// 所以节点每处理 10000 条(默认值) Entry 记录，就会触发一次创建快照的过程，
			// 同时 WAL 会释放一些日志文件的句柄，storage 也会压缩其保存的 Entry 记录。
			rc.maybeTriggerSnapshot(applyDoneC)

			// 通知底层 raft 核心库，当前的指令已经提交应用完成，这使得 raft 核心库可以发送下一个 Ready 指令
			rc.node.Advance()

		// 处理网络异常
		case err := <-rc.transport.ErrorC:
			// 关闭与集群中其他节点的网络连接
			rc.writeError(err)
			return
		// 处理关闭命令
		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (rc *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
	}
	return ms
}

func (rc *raftNode) serveRaft() {
	// 本机监听地址
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	// 创建 http listener
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	// 启动 http 服务
	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
