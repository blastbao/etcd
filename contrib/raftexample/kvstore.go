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
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
)

// a key-value store backed by raft
//
// kvstore 扮演了持久化存储和状态机的角色。
//
// etcd-raft 模块通过 Ready 实例返回的 `待应用` 的 Entries 记录最终都会保存到 kvstore 中。
type kvstore struct {
	// 收到 HTTP PUT 请求时，会调用 kvstore.Propose() 方法，将用户请求的数据写入 proposeC 通道中，之后 raftNode 会从该通道读取数据并进行处理。
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	// 储存键值对，键值都是string类型。
	kvStore     map[string]string // current committed key-value pairs
	// 负责读取快照文件。
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(
	snapshotter *snap.Snapshotter,
	proposeC chan<- string,
	commitC <-chan *commit,
	errorC <-chan error,
) *kvstore {
	// 创建
	s := &kvstore{
		proposeC: proposeC,
		kvStore: make(map[string]string),
		snapshotter: snapshotter,
	}

	// 读取快照
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}

	// read commits from raft into kvStore map until error
	// 从 commitC 中读取数据并应用到 kv store 上，知道遇到错误为止。
	go s.readCommits(commitC, errorC)
	return s
}

// Lookup 查找 key
func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

// Propose 序列化 k-v pair 并发送到 proposeC 通道，proposeC 会在 RaftNode 中进行处理。
func (s *kvstore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	// 当收到 PUT 请求时，httpKVAPI 会将请求中的键值对通过 proposeC 通道传递给 raftNode 实例进行处理
	s.proposeC <- buf.String()
}



// [数据写入]
//
// 整个的数据写入过程：
//	http api 请求写入 proposeC ，监听到 proposeC 中有数据，将数据提交给 raft ；
//	当 raft 处理完成后通过 Ready chan 将请求交给用户代码继续处理，完成 wal、entries 持久化和 entries 发送给 peer ， 查找是否有已经 commit 的 entries ，将 commit 的 entries 写入 commitC
//	用户代码通过从 commitC 中读取已经提交成功的数据，可以放心的将数据写入状态机。
func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {

	// 循环遍历 commitC 信道中 raft 模块传来的已提交消息，消息可能为 nil 或非 nil 。
	//
	// 因为 raftexample 功能简单，其通过 nil 表示 raft 模块完成重放日志的信号或用来通知 kvstore 从上一个快照恢复的信号。
	// 当 commit 为 nil 时，该方法会通过 kvstore 的快照管理模块 snapshotter 尝试加载上一个快照。
	// 如果快照存在，说明这是通知其恢复快照的信号，接下来会调用 recoverFromSnapshot 方法从该快照中恢复，随后进入下一次循环，等待日志重放完成的信号；
	// 如果没找到快照，那么说明这是 raft 模块通知其日志重放完成的信号，因此直接返回。
	//
	// 当 commit 非 nil 时，说明这是 raft 模块发布的已经通过共识提交了的键值对。
	// 此时，先从字节数组数据中反序列化出键值对，并加锁修改 map 中的键值对。
	//
	// 可以看到，kvstore 中基本上没有多少与 raft 相关的处理逻辑，大部分代码是对键值存储抽象本身的实现。
	for commit := range commitC {

		// 1. 接收到的 commit 为 nil，意味这接收到新快照，需要加载快照
		if commit == nil {
			// signaled to load snapshot
			// 读取最新快照
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			// 若快照存在则加载到状态机
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		// 2. 接收到的 commit 不为 nil，序列化 data 并存储到 kvStore
		for _, data := range commit.data {
			// 数据解码
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			// 数据保存
			s.mu.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}

		// 3. 告知已应用
		close(commit.applyDoneC)
	}

	// 3. 接收到错误
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	// 序列化
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	// 存储到 kvStore
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
