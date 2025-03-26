package Distributed_Cache

import (
	"Distributed-Cache/distribute"
	"Distributed-Cache/distribute/consistentHash"
	"Distributed-Cache/local"
	"Distributed-Cache/local/lru"
	"Distributed-Cache/pkg"
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type Cache struct {
	cancelCtx   context.Context
	totalCancel context.CancelFunc
	cancelMap   map[string]context.CancelFunc
	local       *local.Cache
	distribute  *distribute.Cache
	singleGroup singleflight.Group
	mu          sync.Mutex
	opts        CacheOpts
	offlinePeerCh chan string
	changedKeysCh chan string
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

	//初始化结点
	distributeCache.InitPeers(peers)

	cache := &Cache{
		local:      localCache,
		distribute: distributeCache,
		opts:       cacheOpts,
		cancelMap:  make(map[string]context.CancelFunc, len(peers)),
		offlinePeerCh: make(chan string,cacheOpts.pipeBufferSize),
		changedKeysCh: make(chan string,cacheOpts.pipeBufferSize),
	}

	//加锁
	cache.mu.Lock()

	tcctx, tcancel := context.WithCancel(context.Background())
	cache.cancelCtx = tcctx
	cache.totalCancel = tcancel

	

	//开启协程来监听每个结点的状态
	go distributeCache.WatchPeerStatus(cache.cancelCtx,cache.offlinePeerCh)
	log.Println("started monitoring the status of all nodes")
	
	//开启协程来监听每个结点的key的变化
	for name, peer := range peers {
		cache.startWatchPeerKeys(name, peer)
	}
	
	//解锁
	cache.mu.Unlock()

	//处理结点的变化
	go cache.handlePeerChange(cache.cancelCtx)


	return cache, nil
}

func (c *Cache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	//由于监听key的ctx都是基于c.cancelCtx的
	//所以cancel这个就可以直接取消所有正在监听的协程
	c.totalCancel()
	log.Println("the shutdown signal has been sent to all nodes' listening goroutines")
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

// Add 添加kv对
func (c *Cache) Add(ctx context.Context, key string, value []byte, expireTime time.Duration) error {
	err := c.distribute.Add(ctx, key, pkg.NewByteView(value), expireTime)
	if err != nil {
		return err
	}
	return nil
}

// AddPeer 添加节点
func (c *Cache) AddPeer(name string, peer distribute.Peer) error {
	err := c.distribute.AddPeer(name, peer)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	//开始对peer的key的监听
	c.startWatchPeerKeys(name, peer)
	return nil
}

// RemovePeer 移除节点
func (c *Cache) RemovePeer(name string) error {
	err := c.distribute.RemovePeer(name)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	//取消对peer的监听
	c.cancelWatchPeerKeys(name)
	return nil
}

// RedistributionKeys 重新分配key
func (c *Cache) RedistributionKeys() error {
	return c.distribute.RedistributionKeys()
}

func (c *Cache) Opts() CacheOpts {
	return c.opts
}

type DataInfo interface {
	Len() int
	Bytes() int64
}

type PeerInfo interface {
	DataInfo
	Name() string
	Addr() string
}

func (c *Cache) LocalDataInfo() DataInfo {
	return c.local
}
func (c *Cache) PeerInfo(name string) (PeerInfo, bool) {
	return c.distribute.GetPeer(name)
}

func (c *Cache) startWatchPeerKeys(name string, peer distribute.Peer) {
	cctx, cancel := context.WithCancel(c.cancelCtx)
	c.cancelMap[name] = cancel
	go c.distribute.WatchPeersKeys(cctx, peer,c.changedKeysCh)
	log.Printf("the monitoring of the node[%s]'s key has started",name)
}
func (c *Cache) cancelWatchPeerKeys(name string) {
	if cancel, ok := c.cancelMap[name]; ok {
		//取消监听key
		cancel()
		delete(c.cancelMap, name)
		log.Printf("the cancellation signal for the node[%s] key monitoring has been sent",name)
	}
}

func (c *Cache) handlePeerChange(ctx context.Context){
	for {
		select {
		case peerName := <- c.offlinePeerCh:
			err := c.RemovePeer(peerName)
			if err==nil {
				c.cancelWatchPeerKeys(peerName)
			}else {
				log.Printf("peer[%s] is offline,but remove failed: %v",peerName,err)
			}
		case key := <- c.changedKeysCh:
			//有key发生改变,应该删除本地的缓存对应的key
			c.local.Del(key)
		case <-ctx.Done():
			log.Println("handling node changes is closed")
			return	
		}
	}
}