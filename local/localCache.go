package local

import (
	"Distributed-Cache/local/lru"
	"Distributed-Cache/pkg"
	"sync"
)

type Cache struct {
	mu    sync.RWMutex
	cache *lru.Cache
}

func NewCache(cache *lru.Cache) *Cache {
	return &Cache{
		cache: cache,
	}
}
func (c *Cache) Add(key string, value pkg.ByteView, expireTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Add(key, value, expireTime)
}

func (c *Cache) Get(key string) (value pkg.ByteView, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.cache.Get(key)
	if !ok {
		return pkg.ByteView{}, false
	}
	return val.(pkg.ByteView), true
}

func (c *Cache) Len() int {
	return c.cache.Length()
}

func (c *Cache) Bytes() int64 {
	return c.cache.Bytes()
}
