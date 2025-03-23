package localCache

import (
	"Distributed-Cache/localCache/lru"
	"Distributed-Cache/pkg"
	"sync"
)

type LocalCache struct {
	mu       sync.RWMutex
	cache    *lru.Cache
	maxBytes int64
}

func NewLocalCache(maxBytes int64) *LocalCache {
	return &LocalCache{
		maxBytes: maxBytes,
		cache:    lru.New(maxBytes),
	}
}

func (c *LocalCache) Add(key string, value pkg.ByteView, expireTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Add(key, value, expireTime)
}

func (c *LocalCache) Get(key string) (value pkg.ByteView, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.cache.Get(key)
	if !ok {
		return pkg.ByteView{}, false
	}
	return val.(pkg.ByteView), true
}
