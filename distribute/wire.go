//go:build wireinject
// +build wireinject

package distribute

import (
	"Distributed-Cache/distribute/consistentHash"
	"github.com/google/wire"
)

func WireCache(replicas consistentHash.Replicas, fn consistentHash.Hash, batch Batch, poolSize PoolSize) (*Cache, error) {
	wire.Build(consistentHash.New, NewPeerManager, NewCache)
	return &Cache{}, nil
}
