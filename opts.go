package Distributed_Cache

import (
	"hash/crc32"
	"time"
)

var defaultOpts = CacheOpts{
	localCacheExpire:        15 * time.Minute,
	localCacheMaxBytes:      1024 * 1024 * 1024,
	hashFunc:                crc32.ChecksumIEEE,
	replicas:                10,
	migrationBatchSize:      100,
	migrationGoroutineLimit: 30,
	pipeBufferSize: 10,
}

type CacheOpt func(*CacheOpts)

type CacheOpts struct {
	localCacheExpire        time.Duration
	localCacheMaxBytes      int64 //如果为0，则代表无限制
	hashFunc                func(data []byte) uint32
	replicas                int //虚拟节点倍数
	migrationBatchSize      int //迁移结点数据时一次迁移的key数
	migrationGoroutineLimit int //迁移结点数据时最大的goroutine数量
	pipeBufferSize int //监听结点的管道大小
}

func (o CacheOpts) LocalCacheExpire() time.Duration {
	return o.localCacheExpire
}
func (o CacheOpts) LocalCacheMaxBytes() int64 {
	return o.localCacheMaxBytes
}
func (o CacheOpts) HashFunc() func(data []byte) uint32 {
	return o.hashFunc
}
func (o CacheOpts) Replicas() int {
	return o.replicas
}
func (o CacheOpts) MigrationBatchSize() int {
	return o.migrationBatchSize
}
func (o CacheOpts) MigrationGoroutineLimit() int {
	return o.migrationGoroutineLimit
}
func (o CacheOpts) PipeBufferSize() int{
	return o.pipeBufferSize
}


// WithLocalCacheExpire 本地缓存的过期时间
func WithLocalCacheExpire(expire time.Duration) CacheOpt {
	return func(opts *CacheOpts) {
		opts.localCacheExpire = expire
	}
}

// WithLocalCacheMaxBytes 如果为0，则代表无限制
func WithLocalCacheMaxBytes(maxBytes int64) CacheOpt {
	return func(opts *CacheOpts) {
		opts.localCacheMaxBytes = maxBytes
	}
}

// WithHashFunc 一致性哈希函数
func WithHashFunc(hashFunc func(data []byte) uint32) CacheOpt {
	return func(opts *CacheOpts) {
		opts.hashFunc = hashFunc
	}
}

// WithReplicas 虚拟节点倍数
func WithReplicas(replicas int) CacheOpt {
	return func(opts *CacheOpts) {
		opts.replicas = replicas
	}
}

// WithMigrationBatchSize 迁移结点数据时一次迁移的key数
func WithMigrationBatchSize(batchSize int) CacheOpt {
	return func(opts *CacheOpts) {
		opts.migrationBatchSize = batchSize
	}
}

// WithMigrationGoroutineLimit 迁移时的goroutine最大数量
func WithMigrationGoroutineLimit(limit int) CacheOpt {
	return func(opts *CacheOpts) {
		opts.migrationGoroutineLimit = limit
	}
}

// WithPipeBufferSize 监听结点的管道大小
func WithPipeBufferSize(size int) CacheOpt {
	return func(opts *CacheOpts) {
		opts.pipeBufferSize  = size
	}
}


func repairOpts(opts *CacheOpts) {
	if opts.localCacheExpire <= 0 {
		opts.localCacheExpire = defaultOpts.localCacheExpire
	}
	if opts.localCacheMaxBytes < 0 {
		opts.localCacheMaxBytes = defaultOpts.localCacheMaxBytes
	}
	if opts.hashFunc == nil {
		opts.hashFunc = defaultOpts.hashFunc
	}
	if opts.replicas <= 0 {
		opts.replicas = defaultOpts.replicas
	}
	if opts.migrationBatchSize <= 0 {
		opts.migrationBatchSize = defaultOpts.migrationBatchSize
	}
	if opts.migrationGoroutineLimit <= 0 {
		opts.migrationGoroutineLimit = defaultOpts.migrationGoroutineLimit
	}
	if opts.pipeBufferSize<0 {
		opts.pipeBufferSize  = defaultOpts.pipeBufferSize
	}
}
