package Distributed_Cache

import (
	"Distributed-Cache/distribute/consistentHash"
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
}

type CacheOpt func(*CacheOpts)

type CacheOpts struct {
	localCacheExpire        time.Duration
	localCacheMaxBytes      int64 //如果为0，则代表无限制
	hashFunc                consistentHash.Hash
	replicas                int //虚拟节点倍数
	migrationBatchSize      int //迁移结点数据时一次迁移的key数
	migrationGoroutineLimit int
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
func WithHashFunc(hashFunc consistentHash.Hash) CacheOpt {
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
}
