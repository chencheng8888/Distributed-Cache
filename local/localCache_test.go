package local

import (
	"Distributed-Cache/local/lru"
	"Distributed-Cache/pkg"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func initLocalCache() *Cache {
	cache := NewCache(lru.New(10240))
	return cache
}

func TestCache_Add(t *testing.T) {

	type args struct {
		key        string
		value      pkg.ByteView
		expireTime int64
	}
	tests := []struct {
		name        string
		c           *Cache
		args        args
		isAvailable bool
	}{
		{
			name: "normal",
			c:    initLocalCache(),
			args: args{
				key:        "key1",
				value:      pkg.NewByteView([]byte("value1")),
				expireTime: time.Now().Add(10 * time.Minute).Unix(),
			},
			isAvailable: true,
		},
		{
			name: "expire is 0",
			c:    initLocalCache(),
			args: args{
				key:        "key1",
				value:      pkg.NewByteView([]byte("value1")),
				expireTime: 0,
			},
			isAvailable: false,
		},
		{
			name: "expire is < 0",
			c:    initLocalCache(),
			args: args{
				key:        "key1",
				value:      pkg.NewByteView([]byte("value1")),
				expireTime: -1,
			},
			isAvailable: true,
		},
		{
			name: "key is empty",
			c:    initLocalCache(),
			args: args{
				key:        "",
				value:      pkg.NewByteView([]byte("value1")),
				expireTime: time.Now().Add(10 * time.Minute).Unix(),
			},
			isAvailable: false,
		},
		{
			name: "value is empty",
			c:    initLocalCache(),
			args: args{
				key:        "key1",
				value:      pkg.NewByteView([]byte("")),
				expireTime: time.Now().Add(10 * time.Minute).Unix(),
			},
			isAvailable: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.c.Add(tt.args.key, tt.args.value, tt.args.expireTime)

			gotValue, gotOk := tt.c.Get(tt.args.key)

			assert.Equal(t, tt.isAvailable, gotOk)
			if tt.isAvailable {
				assert.Equal(t, tt.args.value, gotValue)
			}
		})
	}
}

func TestCache_Del(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name        string
		c           *Cache
		args        args
		preAdd      bool
		wantDeleted bool
	}{
		{
			name: "delete existing key",
			c:    initLocalCache(),
			args: args{
				key: "key1",
			},
			preAdd:      true,
			wantDeleted: true,
		},
		{
			name: "delete non-existent key",
			c:    initLocalCache(),
			args: args{
				key: "key1",
			},
			preAdd:      false,
			wantDeleted: false,
		},
		{
			name: "delete empty key",
			c:    initLocalCache(),
			args: args{
				key: "",
			},
			preAdd:      false,
			wantDeleted: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preAdd {
				tt.c.Add(tt.args.key, pkg.NewByteView([]byte("value1")), time.Now().Add(10*time.Minute).Unix())
			}
			
			beforeLen := tt.c.Len()
			tt.c.Del(tt.args.key)
			afterLen := tt.c.Len()
			
			if tt.wantDeleted {
				assert.Equal(t, beforeLen-1, afterLen)
			} else {
				assert.Equal(t, beforeLen, afterLen)
			}
		})
	}
}

func TestCache_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name      string
		c         *Cache
		args      args
		preAdd    bool
		wantValue pkg.ByteView
		wantOk    bool
	}{
		{
			name: "get existing key",
			c:    initLocalCache(),
			args: args{
				key: "key1",
			},
			preAdd:      true,
			wantValue:   pkg.NewByteView([]byte("value1")),
			wantOk:      true,
		},
		{
			name: "get non-existent key",
			c:    initLocalCache(),
			args: args{
				key: "key1",
			},
			preAdd:      false,
			wantValue:   pkg.ByteView{},
			wantOk:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preAdd {
				tt.c.Add(tt.args.key, tt.wantValue, time.Now().Add(10*time.Minute).Unix())
			}
			
			gotValue, gotOk := tt.c.Get(tt.args.key)
			assert.Equal(t, tt.wantOk, gotOk)
			if tt.wantOk {
				assert.Equal(t, tt.wantValue, gotValue)
			}
		})
	}
}

func TestCache_Len(t *testing.T) {
	tests := []struct {
		name   string
		c      *Cache
		addNum int
		want   int
	}{
		{
			name:   "empty cache",
			c:      initLocalCache(),
			addNum: 0,
			want:   0,
		},
		{
			name:   "cache with 1 item",
			c:      initLocalCache(),
			addNum: 1,
			want:   1,
		},
		{
			name:   "cache with 3 items",
			c:      initLocalCache(),
			addNum: 3,
			want:   3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < tt.addNum; i++ {
				key := string(rune('a' + i))
				tt.c.Add(key, pkg.NewByteView([]byte("value")), time.Now().Add(10*time.Minute).Unix())
			}
			assert.Equal(t, tt.want, tt.c.Len())
		})
	}
}

func TestCache_Bytes(t *testing.T) {
	tests := []struct {
		name   string
		c      *Cache
		addNum int
		want   int64
	}{
		{
			name:   "empty cache",
			c:      initLocalCache(),
			addNum: 0,
			want:   0,
		},
		{
			name:   "cache with 1 small item",
			c:      initLocalCache(),
			addNum: 1,
			want:   5, //  value (5 bytes)
		},
		{
			name:   "cache with multiple items",
			c:      initLocalCache(),
			addNum: 3,
			want:   15, // 15 bytes
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < tt.addNum; i++ {
				key := string(rune('a' + i))
				tt.c.Add(key, pkg.NewByteView([]byte("value")), time.Now().Add(10*time.Minute).Unix())
			}
			assert.Equal(t, tt.want, tt.c.Bytes())
		})
	}
}
