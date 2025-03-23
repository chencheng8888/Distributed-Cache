package Distributed_Cache

import (
	"Distributed-Cache/distributedCache"
	"Distributed-Cache/localCache"
	"Distributed-Cache/pkg"
	"context"
	"golang.org/x/sync/singleflight"
	"sync"
	"time"
)

type Cache struct {
	mu               sync.RWMutex
	local            *localCache.LocalCache
	distribute       *distributedCache.Cache
	singleGroup      singleflight.Group
	localCacheExpire time.Duration
}

func NewCacheHandler(local *localCache.LocalCache, distribute *distributedCache.Cache) *Cache {
	return &Cache{
		local:            local,
		distribute:       distribute,
		localCacheExpire: 5 * time.Minute,
	}
}

func (c *Cache) Get(ctx context.Context, key string) (pkg.ByteView, error) {
	//首先,先从本地缓存中获取
	val, ok := c.local.Get(key)
	if ok {
		return val, nil
	}

	//本地缓存获取不到,从分布式缓存中获取
	//使用singleflight,减少对redis的网络IO
	res, err, _ := c.singleGroup.Do(key, func() (interface{}, error) {
		res, err := c.distribute.Get(ctx, key)
		if err != nil {
			return pkg.ByteView{}, err
		}
		c.local.Add(key, res, time.Now().Add(c.localCacheExpire).Unix())
		return res, nil
	})
	if err != nil {
		return pkg.ByteView{}, err
	}
	return res.(pkg.ByteView), nil
}

func (c *Cache) Add(ctx context.Context, key string, value pkg.ByteView, expireTime time.Duration) error {
	return c.distribute.Add(ctx, key, value, expireTime)
}

func (c *Cache) AddNode(name, addr string) error {
	return c.distribute.AddNode(name, addr)
}

func (c *Cache) RemoveNode(name string) error {
	return c.distribute.RemoveNode(name)
}
