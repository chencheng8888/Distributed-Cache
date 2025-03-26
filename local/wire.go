//go:build wireinject
// +build wireinject

package local

import (
	"Distributed-Cache/local/lru"

	"github.com/google/wire"
)

func WireCache(maxBytes lru.MaxBytes) *Cache {
	wire.Build(lru.New, NewCache)
	return &Cache{}
}
