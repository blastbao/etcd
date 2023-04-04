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
	"flag"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)


// 步骤:
//
// 1：程序启动需要传递 cluster，id，port，join 参数，这里主要完成参数解析
//	cluster：集群节点列表，使用逗号分隔
//	id：当前节点编号
//	port：当前节点 http 监听端口
//	join：该节点是否加入到当前集群
//
// 2：初始化 proposeC 和 confChangeC 通道。
//	proposeC 和 confChangeC 是应用和底层 etcd-raft 通信的pipeline
//
// 3：完成 raftNode 初始化。
//	初始化包括创建 raftNode 和 启动服务，这个会在后面的 raftNode 章节中详细阐述
//
// 4：初始化 kvstore 系统
//
// 5：启动 http 服务，对外提供服务
func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	// [初始化]
	//
	// 由于 raft 库只实现核心逻辑，所以需要与应用进行交互。
	//
	// 其交互采用 4 个 channel ：
	//	proposeC：用户代码向 raft 提交写请求 Propose
	//	confChangeC：用户代码向 raft 提交配置变更请求 ProposeConfChange
	//	commitC：用于 raft 向用户代码传输已经提交的 entries ，用户 raft 库暴露具体业务逻辑，将数据写入 State Machine
	//	errorC：向用户代码暴露 raft 库的处理异常
	//
	// 其中：
	// 	proposeC 和 confChangeC 是在开始手动创建，作为参数传递给 raft 组件。
	//	commitC 和 errorC 是用户代码手动封装 raftNode 时创建的 channel ，并在 raft 组件中使用。
	//
	//
	// 用户代码向 raft 提交写请求 Propose
	proposeC := make(chan string)
	defer close(proposeC)
	// 用户代码向 raft 提交配置变更请求 ProposeConfChange
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) {
		return kvs.getSnapshot()
	}

	//
	commitC, errorC, snapshotterReady := newRaftNode(
		*id,
		strings.Split(*cluster, ","),
		*join,
		getSnapshot,
		proposeC,
		confChangeC,
	)

	// 创建 kv 存储，它扮演着 raft 状态机的角色，也即 raft-lib 的应用层。
	//
	// 参数除 snapshotterReady 外，proposeC、commitC、errorC 均为管道，其中 propseC 为输入管道，commitC 和 errorC 为输出管道。
	// 可以推断出，kvstore 会通过 proposeC 与 raft 模块交互，并通过 commitC 与 errorC 接收来自 raft 模块的消息。
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveHTTPKVAPI(kvs, *kvport, confChangeC, errorC)
}
