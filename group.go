package Distributed_Cache

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Group struct {
	name       string
	cacheBytes int64
	mainCache  cache
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64) *Group {
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:       name,
		cacheBytes: cacheBytes,
	}
	groups[name] = g
	return g
}

func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	if v, ok := g.mainCache.get(key); ok {
		log.Println(fmt.Sprintf("cache hit: key[%s]", key))
		return v, nil
	}
	return ByteView{}, fmt.Errorf("local cache don't hit")
}

func (g *Group) Add(key string, value ByteView, expire time.Duration) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}
	g.mainCache.add(key, value, expire)
	return nil
}

func (g *Group) TTL(key string) (time.Duration, error) {
	if key == "" {
		return 0, fmt.Errorf("key is required")
	}
	if ttl, ok := g.mainCache.ttl(key); ok {
		return time.Duration(ttl) * time.Second, nil
	}
	return 0, fmt.Errorf("local cache don't hit")
}
