package Distributed_Cache

import (
	"Distributed-Cache/distribute"
	"Distributed-Cache/distribute/consistentHash"
	"Distributed-Cache/local"
	"Distributed-Cache/local/lru"
	"Distributed-Cache/pkg"
	"context"
	"errors"
	"golang.org/x/sync/singleflight"
	"sync"
	"time"
)

type Cache struct {
	mu          sync.RWMutex
	local       *local.Cache
	distribute  *distribute.Cache
	singleGroup singleflight.Group
	opts        CacheOpts
}

func New(peers map[string]distribute.Peer, opts ...CacheOpt) (*Cache, error) {
	if len(peers) <= 0 {
		return nil, errors.New("the length of cache node should be greater than 0")
	}

	cacheOpts := defaultOpts
	for _, opt := range opts {
		opt(&cacheOpts)
	}
	repairOpts(&cacheOpts)

	localCache := local.WireCache(lru.MaxBytes(cacheOpts.localCacheMaxBytes))
	distributeCache, err := distribute.WireCache(consistentHash.Replicas(cacheOpts.replicas),
		cacheOpts.hashFunc,
		distribute.Batch(cacheOpts.migrationBatchSize),
		distribute.PoolSize(cacheOpts.migrationGoroutineLimit))
	if err != nil {
		return nil, err
	}
	distributeCache.InitPeers(peers)
	return &Cache{
		local:      localCache,
		distribute: distributeCache,
		opts:       cacheOpts,
	}, nil
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	//首先,先从本地缓存中获取
	val, ok := c.local.Get(key)
	if ok {
		return val.ByteSlice(), nil
	}

	//本地缓存获取不到,从分布式缓存中获取
	//使用singleflight,减少对redis的网络IO
	res, err, _ := c.singleGroup.Do(key, func() (interface{}, error) {
		res, err := c.distribute.Get(ctx, key)
		if err != nil {
			return pkg.ByteView{}, err
		}
		c.local.Add(key, res, time.Now().Add(c.opts.localCacheExpire).Unix())
		return res, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(pkg.ByteView).ByteSlice(), nil
}

func (c *Cache) Add(ctx context.Context, key string, value []byte, expireTime time.Duration) error {
	return c.distribute.Add(ctx, key, pkg.NewByteView(value), expireTime)
}

func (c *Cache) AddNode(name string, peer distribute.Peer) error {
	return c.distribute.AddNode(name, peer)
}

func (c *Cache) RemoveNode(name string) error {
	return c.distribute.RemoveNode(name)
}

func (c *Cache) Opts() CacheOpts {
	return c.opts
}
