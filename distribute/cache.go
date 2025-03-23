package distribute

import (
	"Distributed-Cache/pkg"
	"context"
	"errors"
	"time"
)

type Cache struct {
	PeerManager
}

func NewCache(peerManager PeerManager) *Cache {
	return &Cache{
		PeerManager: peerManager,
	}
}

func (c *Cache) Get(ctx context.Context, key string) (pkg.ByteView, error) {
	if c.PeerManager == nil {
		return pkg.ByteView{}, errors.New("peer manager is nil")
	}
	peer, ok := c.PickPeer(key)
	if !ok || peer == nil {
		return pkg.ByteView{}, errors.New("no peer found")
	}
	res, err := peer.Get(ctx, Client, key)
	if err != nil {
		return pkg.ByteView{}, err
	}
	return pkg.NewByteView(res), nil
}

func (c *Cache) Add(ctx context.Context, key string, value pkg.ByteView, expireTime time.Duration) error {
	if c.PeerManager == nil {
		return errors.New("peer manager is nil")
	}
	peer, ok := c.PickPeer(key)
	if !ok || peer == nil {
		return errors.New("no peer found")
	}
	return peer.Add(ctx, Client, Element{key, value.ByteSlice(), expireTime})
}
