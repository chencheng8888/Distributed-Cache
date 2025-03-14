package Distributed_Cache

import (
	"Distributed-Cache/lru"
	"sync"
	"time"
)

const (
	NoExpire = -1
)

type cache struct {
	mu     sync.RWMutex
	lru    *lru.Cache
	nbytes int64
}

func (c *cache) add(key string, value ByteView, expire time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New(0)
	}
	var ttl int64
	if expire < 0 {
		ttl = NoExpire
	} else {
		ttl = time.Now().Add(expire).Unix()
	}
	c.lru.Add(key, value, ttl)
	c.nbytes += int64(len(key)) + int64(value.Len())
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lru == nil {
		return
	}
	if v, hit := c.lru.Get(key); hit {
		return v.(ByteView), true
	}
	return
}
func (c *cache) del(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.Del(key)
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}
