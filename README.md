# Distributed-Cache

一个高性能的分布式缓存系统，支持动态节点管理和数据自动迁移。

## 主要特性

- 双层缓存架构：本地LRU缓存 + 分布式缓存
- 一致性哈希路由，支持虚拟节点
- 动态节点管理(添加/删除节点)
- 自动数据迁移和重新平衡
- Redis节点支持
- 读写模式控制
- 防止缓存击穿(singleflight)
- 高性能并发迁移(ants goroutine池)

## 架构设计

```
┌─────────────────────────────────────────────────┐
│                    Application                  │
└─────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────┐
│                    Cache                        │
│  ┌─────────────┐       ┌───────────────────┐   │
│  │ Local Cache │◄─────►│ Distributed Cache │   │
│  └─────────────┘       └───────────────────┘   │
└─────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────┐
│                Peer Manager                     │
│  ┌─────────────┐       ┌───────────────────┐   │
│  │Consistent   │       │ Redis/Other Peers │   │
│  │Hash Ring    │       └───────────────────┘   │
│  └─────────────┘                               │
└─────────────────────────────────────────────────┘
```

## 快速开始

### 安装

```bash
go get github.com/yourusername/Distributed-Cache
```

### 使用示例

```go
package main

import (
	"context"
	"Distributed-Cache"
	"Distributed-Cache/distribute"
	"time"
)

func main() {
	// 创建Redis节点
	redisPeer1, _ := distribute.NewRedisPeer("node1", redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}))
	
	// 初始化缓存
	cache, _ := Distributed_Cache.New(map[string]distribute.Peer{
		"node1": redisPeer1,
	})
	
	// 添加数据
	ctx := context.Background()
	cache.Add(ctx, "key1", []byte("value1"), time.Minute)
	
	// 获取数据
	val, _ := cache.Get(ctx, "key1")
	println(string(val)) // 输出: value1
	
	// 关闭缓存
	cache.Close()
}
```

## 配置选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| WithLocalCacheExpire | 本地缓存过期时间 | 15分钟 |
| WithLocalCacheMaxBytes | 本地缓存最大大小(0表示无限制) | 1GB |
| WithHashFunc | 一致性哈希函数 | crc32.ChecksumIEEE |
| WithReplicas | 虚拟节点倍数 | 10 |
| WithMigrationBatchSize | 迁移批处理大小 | 100 |
| WithMigrationGoroutineLimit | 迁移goroutine限制 | 30 |
| WithPipeBufferSize | 监听管道缓冲区大小 | 10 |

## API文档

### Cache接口

```go
type Cache interface {
    // 获取缓存值
    Get(ctx context.Context, key string) ([]byte, error)
    // 添加缓存值
    Add(ctx context.Context, key string, value []byte, expireTime time.Duration) error
    // 添加节点
    AddPeer(name string, peer distribute.Peer) error
    // 移除节点
    RemovePeer(name string) error
    // 重新分配键
    RedistributionKeys() error
    // 关闭缓存
    Close()
}
```

## 性能优化

- 使用singleflight防止缓存击穿
- 批量操作减少网络IO
- 并发迁移提高数据平衡速度
- 读写锁优化并发性能

## 监控

- 节点状态自动监控
- 内存使用统计
- 键变更监听
