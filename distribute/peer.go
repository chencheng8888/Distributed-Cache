package distribute

import (
	"Distributed-Cache/distribute/consistentHash"
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"log"
	"sync"
	"time"
)

const (
	//这些权限只是限制客户端
	//对于服务端，则没有限制

	NormalMode = iota //正常模式,可读可写
	ReadOnly          //只可以读
	None              //无法读无法写

	Client = "client"
	Server = "server"
)

type Element struct {
	Key        string
	Value      []byte
	ExpireTime time.Duration
}

func (e Element) String() string {
	return fmt.Sprintf("Key:%v,Value:%v,ExpireTime:%v", e.Key, string(e.Value), e.ExpireTime)
}

type Peer interface {
	//关闭连接
	Close() error

	//基本信息
	Name() string
	Addr() string

	//模式
	Mode() int
	ChangeMode(mode int)

	//读操作
	Get(ctx context.Context, identity, key string) ([]byte, error)
	TTL(ctx context.Context, identity, key string) (time.Duration, error)
	//额外提供一个同时获取val和ttl的方法，减少网络请求次数
	GetAndTTL(ctx context.Context, identity, key string) ([]byte, time.Duration, error)
	//批量获取全部key的方法
	GetBatchKey(ctx context.Context, identity string, batch int, keyCh chan<- string)

	//添加元素
	Add(ctx context.Context, identity string, elements ...Element) error

	//删除元素
	Del(ctx context.Context, identity string, key ...string) error

	//测试连接
	Ping() bool
}

type PeerManager interface {
	PickPeer(key string) (Peer, bool)
	AddPeer(name string, peer Peer) error
	RemovePeer(name string) error
	RedistributionKeys() error
	InitPeers(peers map[string]Peer)
}

type PeerManagerImpl struct {
	mu    sync.RWMutex
	peers map[string]Peer
	//即将下线的结点，但是在数据迁移期间，其仍可以接受读操作
	offlineSoon map[string]Peer
	hash        consistentHash.Map
	//是否正在迁移数据
	IsBeingMigrated bool
	//批量获取key的数量
	batch int
	pool  *ants.Pool
}

type Batch int
type PoolSize int

func NewPeerManager(mp consistentHash.Map, batch Batch, poolSize PoolSize) (PeerManager, error) {
	pool, err := ants.NewPool(int(poolSize))
	if err != nil {
		return nil, errors.New("create goroutine pool failed")
	}

	return &PeerManagerImpl{
		peers:       make(map[string]Peer),
		offlineSoon: make(map[string]Peer),
		hash:        mp,
		batch:       int(batch),
		pool:        pool,
	}, nil
}

func (p *PeerManagerImpl) InitPeers(peers map[string]Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	//加载peers
	for name, peer := range peers {
		if peer == nil {
			continue
		}
		if !peer.Ping() {
			continue
		}
		p.peers[name] = peer
		p.hash.Add(name)
	}
}

// RedistributionKeys 重新分配kv对
func (p *PeerManagerImpl) RedistributionKeys() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.IsBeingMigrated {
		return errors.New("data is being migrated")
	}

	p.IsBeingMigrated = true
	var tag = false
	defer func() {
		if !tag {
			p.IsBeingMigrated = false
		}
	}()

	//如果只有一个结点，则不需要重新分配
	if len(p.peers) <= 1 {
		return nil
	}

	tag = true
	go func() {
		nodes := p.hash.GetAllNodes()

		type Task struct {
			From, To string
		}

		var tasks = make(map[Task][]string)

		for _, from := range nodes {
			keys := p.getNodeKeys(from)
			for _, key := range keys {
				to := p.hash.Get(key)
				if from == to {
					continue
				}
				task := Task{from, to}
				tasks[task] = append(tasks[task], key)
			}
		}

		//执行迁移任务
		var wg sync.WaitGroup
		// 遍历迁移任务，提交到 ants 协程池
		for task, keys := range tasks {
			wg.Add(1)
			err := p.pool.Submit(func() {
				defer wg.Done()
				log.Printf("start migrating data[%v] from %v to %v", keys, task.From, task.To)
				p.migrateData(context.Background(), p.peers[task.From], p.peers[task.To], keys)
				log.Printf("migration data completed from %v to %v", task.From, task.To)
			})
			if err != nil {
				log.Printf("failed to submit migration task: %v", err)
				wg.Done()
			}
		}
		wg.Wait()
	}()

	return nil
}

func (p *PeerManagerImpl) migrateData(ctx context.Context, from, to Peer, toBeMigratedKeys []string) {
	if from == nil || to == nil {
		log.Printf("when migrating data, it is found that at least one of from and to is empty")
		return
	}

	//在迁移过程中，我们不允许对数据进行写操作
	//给from，to设置只读状态
	from.ChangeMode(ReadOnly)
	to.ChangeMode(ReadOnly)
	defer func() {
		from.ChangeMode(NormalMode)
		to.ChangeMode(NormalMode)
	}()

	var wg sync.WaitGroup

	var elements = make(chan Element, p.batch)

	wg.Add(1)
	//开启一个协程来获取from需要转移的kv对
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Migration goroutine panicked: %v", r)
			}
			close(elements)
			wg.Done()
		}()
		for _, key := range toBeMigratedKeys {
			val, ttl, err := from.GetAndTTL(ctx, Server, key)
			if err != nil {
				log.Printf("get element[Key:%v] from one peer[%v %v] failed: %v", key, from.Name(), from.Addr(), err)
				continue
			}
			select {
			case elements <- Element{key, val, ttl}:
			case <-ctx.Done():
				log.Println("Context canceled, stopping migration")
				return
			}
		}
	}()

	//持续监听channel，并进行批量处理
	var handleElements []Element

outerLoop: // 定义标签
	for {
		select {
		case element, ok := <-elements:
			handleElements = append(handleElements, element)
			if !ok || len(handleElements) >= p.batch {
				err := to.Add(ctx, Server, handleElements...)
				if err != nil {
					log.Printf("add elements to one peer[%v %v] failed: %v", to.Name(), to.Addr(), err)
				}
				handleElements = nil // 清空数组，准备下一个批次
				if !ok {
					break outerLoop
				}
			}
		case <-ctx.Done(): // 任务被取消
			log.Println("Context canceled, stopping migration")
			break outerLoop
		}
	}

	//等待处理完成
	wg.Wait()

	from.ChangeMode(None)

	//最后我们需要删除from的kv对
	// 并发批量删除
	for len(toBeMigratedKeys) > 0 {
		wg.Add(1)
		err := p.pool.Submit(func() {
			defer wg.Done()
			var err error
			if len(toBeMigratedKeys) >= p.batch {
				err = from.Del(ctx, Server, toBeMigratedKeys[:p.batch]...)
				toBeMigratedKeys = toBeMigratedKeys[p.batch:]
			} else {
				err = from.Del(ctx, Server, toBeMigratedKeys...)
				toBeMigratedKeys = nil
			}
			if err != nil {
				log.Printf("failed to delete elements from peer[%v]: %v", from.Name(), err)
			}
		})
		if err != nil {
			log.Printf("failed to submit delete task: %v", err)
			wg.Done()
		}
	}

	wg.Wait()
}

func (p *PeerManagerImpl) RemovePeer(name string) error {

	//变更结点信息时，不能进行读取操作
	p.mu.Lock()
	defer p.mu.Unlock()

	//确保迁移数据时，不能执行删除或添加结点操作
	if p.IsBeingMigrated {
		return errors.New("data is being migrated")
	}

	//以下视为迁移过程
	//确保在执行前，将状态设置为迁移,以防止在更换状态前，添加或删除结点的操作被执行
	p.IsBeingMigrated = true

	var tag = false
	defer func() {
		if !tag {
			p.IsBeingMigrated = false
		}
	}()

	//假删除
	peer, err := p.fakeRemove(name)
	if err != nil {
		return err
	}

	//确保在移除结点后仍有结点,才能执行迁移工作
	if len(p.peers) == 0 {
		//没有数据迁移操作,可以真删除
		p.trueRemove(peer)
		return nil
	}

	tag = true

	//另起一个协程，来迁移数据
	go func() {

		//由于状态已经被设置为迁移了，所以所有修改结点信息的操作都不会执行
		//可以不用加锁

		defer func() {
			p.IsBeingMigrated = false
		}()

		var toBeMigratedKeys = make(map[string][]string)

		//获取需要迁移的Key
		keys := p.getNodeKeys(name)

		for _, key := range keys {
			nodes := p.hash.GetAndSkipNode(key, name)
			if nodes == "" {
				continue
			}
			toBeMigratedKeys[nodes] = append(toBeMigratedKeys[nodes], key)
		}

		var wg sync.WaitGroup

		//开始实际迁移数据
		log.Println("start migrating data")
		for to, migrateKeys := range toBeMigratedKeys {
			wg.Add(1)
			err = p.pool.Submit(func() {
				defer wg.Done()
				log.Printf("start migrating data[%v] from %v to %v", migrateKeys, peer.Name(), to)
				p.migrateData(context.Background(), peer, p.peers[to], migrateKeys)
				log.Printf("migration data completed from %v to %v", peer.Name(), to)
			})
			if err != nil {
				log.Printf("failed to submit migration task: %v", err)
				wg.Done()
			}
		}
		log.Println("migration data completed")

		//这个操作，不能允许读操作进行
		//加锁
		//迁移完毕,此时可以彻底将该结点关闭
		p.mu.Lock()
		defer p.mu.Unlock()
		p.trueRemove(peer)
	}()

	return nil
}

func (p *PeerManagerImpl) AddPeer(name string, peer Peer) error {

	p.mu.Lock()
	defer p.mu.Unlock()

	//确保迁移数据时，不能执行删除或添加结点操作
	if p.IsBeingMigrated {
		return errors.New("data is being migrated")
	}

	//以下视为迁移过程
	//确保在执行前，将状态设置为迁移,以防止在更换状态前，添加或删除结点的操作被执行
	p.IsBeingMigrated = true
	var tag = false
	defer func() {
		if !tag {
			p.IsBeingMigrated = false
		}
	}()

	if name == "" {
		return errors.New("name is empty")
	}
	if _, ok := p.peers[name]; ok {
		return errors.New("the name already exists")
	}
	if peer == nil {
		return errors.New("peer is nil")
	}
	if !peer.Ping() {
		return errors.New("peer is not available")
	}
	p.peers[name] = peer
	p.hash.Add(name)

	//如果只有一个结点，则不需要迁移数据
	if len(p.peers) <= 1 {
		return nil
	}

	tag = true

	go func() {
		//由于状态已经被设置为迁移了，所以所有修改结点信息的操作都不会执行
		//可以不用加锁
		defer func() {
			p.IsBeingMigrated = false
		}()

		//各个结点迁移到新结点的key集合
		var fromOtherNodeKeys = make(map[string][]string)

		nodes := p.hash.GetAllNodes()
		for _, node := range nodes {
			if node == name {
				continue
			}
			keys := p.getNodeKeys(node)
			for _, key := range keys {
				if p.hash.Get(key) == name {
					fromOtherNodeKeys[node] = append(fromOtherNodeKeys[node], key)
				}
			}
		}

		var wg sync.WaitGroup

		//开始实际迁移数据
		log.Println("start migrating data")
		for node, keys := range fromOtherNodeKeys {
			wg.Add(1)
			err := p.pool.Submit(func() {
				defer wg.Done()
				log.Printf("start migrating data[%v] from %v to %v", keys, node, peer.Name())
				p.migrateData(context.Background(), p.peers[node], peer, keys)
				log.Printf("migration data completed from %v to %v", node, peer.Name())
			})
			if err != nil {
				log.Printf("failed to submit migration task: %v", err)
				wg.Done()
			}
		}
		log.Println("migration data completed")
	}()

	return nil
}

func (p *PeerManagerImpl) PickPeer(key string) (Peer, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peerName := p.hash.Get(key)
	if peerName == "" {
		return nil, false
	}

	if peer, ok := p.peers[peerName]; ok {
		return peer, true
	}
	if peer, ok := p.offlineSoon[peerName]; ok {
		return peer, true
	}
	return nil, false
}

// 假删除
// 实际上，我们并不真正删除结点，而是将其设置为只读状态，并加入到即将下线的map中
func (p *PeerManagerImpl) fakeRemove(name string) (Peer, error) {
	if name == "" {
		return nil, errors.New("name is empty")
	}
	if _, ok := p.peers[name]; !ok {
		return nil, errors.New("the name does not exist")
	}
	peer := p.peers[name]

	//移除结点
	delete(p.peers, name)

	//将结点加入到即将下线的map中，其仍可以接受读操作
	//并将结点设置为只读
	p.offlineSoon[name] = peer
	peer.ChangeMode(ReadOnly)

	return peer, nil
}

// 真删除
// 删除结点，并关闭连接
func (p *PeerManagerImpl) trueRemove(peer Peer) {
	name := peer.Name()

	if _, ok := p.peers[name]; ok {
		delete(p.peers, name)
	}
	if _, ok := p.offlineSoon[name]; ok {
		delete(p.offlineSoon, name)
	}

	if err := peer.Close(); err != nil {
		log.Printf("close the connection with peer[name:%s] failed:%s", name, err)
	}
	p.hash.Remove(name)
}

// 获取某个结点所有的Key
func (p *PeerManagerImpl) getNodeKeys(from string) []string {
	var keyCh = make(chan string, p.batch)
	//开启一个协程来获取key
	go func() {
		tctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		p.peers[from].GetBatchKey(tctx, Server, p.batch, keyCh)
	}()

	var keys []string

	for key := range keyCh {
		keys = append(keys, key)
	}

	return keys
}
