package distribute

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
	"time"
)

//使用go-redis["github.com/redis/go-redis/v9"]实现peer接口

type RedisPeer struct {
	name string
	cli  *redis.Client
	mode int
	mu   sync.RWMutex
}

func NewRedisPeer(name string, cli *redis.Client) (Peer, error) {
	if err := cli.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}
	return &RedisPeer{
		name: name,
		cli:  cli,
		mode: NormalMode,
	}, nil
}

func (r *RedisPeer) Name() string {
	return r.name
}

func (r *RedisPeer) Addr() string {
	return r.cli.Options().Addr
}

func (r *RedisPeer) Ping() bool {
	return r.cli.Ping(context.Background()).Err() == nil
}

func (r *RedisPeer) Mode() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mode
}

func (r *RedisPeer) ChangeMode(mode int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch mode {
	case NormalMode, ReadOnly:
		r.mode = mode
	default:
		r.mode = NormalMode
	}
}
func (r *RedisPeer) Del(ctx context.Context, identity string, key ...string) error {
	if r.identification(identity) == Client {
		if !r.isWritable() {
			return errors.New("peer is not writeable")
		}
	}
	err := r.cli.Del(ctx, key...).Err()
	if err != nil {
		log.Printf("Delete Key[%s] from peer[name:%s addr:%s] failed:%s", key, r.Name(), r.Addr(), err)
		return err
	}
	return nil
}

func (r *RedisPeer) TTL(ctx context.Context, identity, key string) (time.Duration, error) {
	if r.identification(identity) == Client {
		if !r.isReadable() {
			return 0, errors.New("peer is not readable")
		}
	}

	ttl, err := r.cli.TTL(ctx, key).Result()
	if err != nil {
		log.Printf("Get the expire of Key[%s] from peer[name:%s addr:%s] failed:%s", key, r.Name(), r.Addr(), err)
		return 0, err
	}
	return ttl, nil
}

func (r *RedisPeer) GetAndTTL(ctx context.Context, identity, key string) ([]byte, time.Duration, error) {
	if r.identification(identity) == Client {
		if !r.isReadable() {
			return nil, 0, errors.New("peer is not readable")
		}
	}

	pip := r.cli.Pipeline()

	getcmd := pip.Get(ctx, key)
	ttlcmd := pip.TTL(ctx, key)

	_, err := pip.Exec(ctx)
	if err != nil {
		return nil, 0, err
	}

	val, err := getcmd.Bytes()
	if err != nil {
		return nil, 0, err
	}
	ttl, err := ttlcmd.Result()
	if err != nil {
		return nil, 0, err
	}
	return val, ttl, nil
}

func (r *RedisPeer) GetBatchKey(ctx context.Context, identity string, batch int, keyCh chan<- string) {
	defer func() {
		close(keyCh)
	}()

	if r.identification(identity) == Client {
		if !r.isReadable() {
			return
		}
	}

	if batch <= 0 {
		return
	}

	cnt, err1 := r.cli.DBSize(context.Background()).Result()
	if err1 != nil || cnt == 0 {
		return
	}

	var (
		cursor uint64
	)

	for {
		keys, nextCursor, err := r.cli.Scan(ctx, cursor, "*", int64(batch)).Result()
		if err != nil {
			return
		}

		for _, key := range keys {
			keyCh <- key
		}
		// 继续扫描
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

func (r *RedisPeer) Close() error {
	return r.cli.Close()
}

func (r *RedisPeer) Get(ctx context.Context, identity, key string) ([]byte, error) {
	if r.identification(identity); identity == Client {
		if !r.isReadable() {
			return nil, errors.New("peer is not readable")
		}
	}

	res, err := r.cli.Get(ctx, key).Bytes()
	if err != nil {
		log.Printf("Get the Value of Key[%s] from peer[name:%s addr:%s] failed:%s", key, r.Name(), r.Addr(), err)
		return nil, err
	}
	return res, nil
}

func (r *RedisPeer) Add(ctx context.Context, identity string, elements ...Element) error {
	if r.identification(identity) == Client {
		if !r.isWritable() {
			return errors.New("peer is not writable")
		}
	}

	if len(elements) == 0 {
		return nil
	}

	_, err := r.cli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, element := range elements {
			if element.ExpireTime == 0 {
				continue
			}
			var expire = element.ExpireTime
			if element.ExpireTime < 0 {
				expire = redis.KeepTTL
			}
			pipe.Set(ctx, element.Key, string(element.Value), expire)
		}
		return nil
	})
	if err != nil {
		log.Printf("add elements[%v] to peer[name:%s addr:%s] failed:%s", elements, r.Name(), r.Addr(), err)
		return err
	}
	return nil
}

func (r *RedisPeer) isReadable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.mode == ReadOnly || r.mode == NormalMode {
		return true
	}
	return false
}

func (r *RedisPeer) isWritable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.mode == NormalMode {
		return true
	}
	return false
}

func (r *RedisPeer) identification(identity string) string {
	switch identity {
	case Server, Client:
		return identity
	default:
		return Client
	}
}
