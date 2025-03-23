package distribute

import (
	"Distributed-Cache/pkg"
	"context"
	"errors"
	"time"
)

type Cache struct {
	peerManager PeerManager
}

func NewCache(peerManager PeerManager) *Cache {
	return &Cache{
		peerManager: peerManager,
	}
}

func (c *Cache) InitPeers(peers map[string]Peer) {
	c.peerManager.InitPeers(peers)
}

func (c *Cache) RedistributionKeys() error {
	return c.peerManager.RedistributionKeys()
}

func (c *Cache) AddNode(name string, peer Peer) error {
	return c.peerManager.AddPeer(name, peer)
}

func (c *Cache) RemoveNode(name string) error {
	return c.peerManager.RemovePeer(name)
}

func (c *Cache) Get(ctx context.Context, key string) (pkg.ByteView, error) {
	if c.peerManager == nil {
		return pkg.ByteView{}, errors.New("peer manager is nil")
	}
	peer, ok := c.peerManager.PickPeer(key)
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
	if c.peerManager == nil {
		return errors.New("peer manager is nil")
	}
	peer, ok := c.peerManager.PickPeer(key)
	if !ok || peer == nil {
		return errors.New("no peer found")
	}
	return peer.Add(ctx, Client, Element{key, value.ByteSlice(), expireTime})
}
