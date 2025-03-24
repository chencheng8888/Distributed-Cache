package Distributed_Cache

import (
	"Distributed-Cache/distribute"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const keyNum = 30

var nodes = map[string]string{
	"node1": "localhost:16390",
	"node2": "localhost:16391",
	"node3": "localhost:16392",
}

var peers = make(map[string]distribute.Peer, len(nodes))
var cache *Cache

func TestMain(m *testing.M) {
	for name, addr := range nodes {
		cli := redis.NewClient(&redis.Options{
			Addr: addr,
		})
		//清空数据
		if err := cli.FlushDB(context.Background()).Err(); err != nil {
			panic(err)
		}
		for i := 0; i < keyNum; i++ {
			// 插入测试数据（每个节点设置不同键值）
			err := cli.Set(context.Background(),
				fmt.Sprintf("key_%s_%d", name, i),   // 格式如 key_node1
				fmt.Sprintf("value_%s_%d", name, i), // 值如 value_node1
				0,                                   // 0 表示永不过期
			).Err()
			if err != nil {
				panic(err)
			}
		}
		peer, err := distribute.NewRedisPeer(name, cli)
		if err != nil {
			panic(err)
		}
		peers[name] = peer
	}
	var err error
	cache, err = New(peers, WithReplicas(50))
	if err != nil {
		panic(err)
	}
	m.Run()
}

func TestCache_RedistributionKeys(t *testing.T) {
	err := cache.RedistributionKeys()
	if err != nil {
		t.Error(err)
	}

	//等待迁移完成
	time.Sleep(5 * time.Second)

	var cnt int

	//检测下key的分布
	for name, peer := range peers {
		var keyCh = make(chan string)
		var keys []string
		go peer.GetBatchKey(context.Background(), distribute.Server, 50, keyCh)

		for key := range keyCh {
			keys = append(keys, key)
		}
		cnt += len(keys)
		t.Logf("结点%s的kv对数量:%d", name, len(keys))
		t.Logf("结点%s的key分布:%v", name, keys)
	}
	assert.Equal(t, len(nodes)*keyNum, cnt)
}
